// Package kafka implements a Kafka transport using franz-go.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/netsampler/goflow2/v3/transport"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kprom"
)

// KafkaDriver sends formatted messages to Kafka topics.
type KafkaDriver struct {
	kafkaTLS         bool
	kafkaClientCert  string
	kafkaClientKey   string
	kafkaServerCA    string
	kafkaTlsInsecure bool

	kafkaSASL           string
	kafkaTopic          string
	kafkaSrv            string
	kafkaBrk            string
	kafkaMaxMsgBytes    int
	kafkaFlushBytes     int
	kafkaFlushFrequency time.Duration

	kafkaHashing          bool
	kafkaVersion          string
	kafkaCompressionCodec string

	producer *kgo.Client

	errors chan error
}

// KafkaTransportError wraps errors returned by the Kafka client.
type KafkaTransportError struct {
	Err error
}

func (e *KafkaTransportError) Error() string {
	return fmt.Sprintf("kafka transport %s", e.Err.Error())
}
func (e *KafkaTransportError) Unwrap() []error {
	return []error{transport.ErrTransport, e.Err}
}

// KafkaSASLAlgorithm names supported SASL auth mechanisms.
type KafkaSASLAlgorithm string

const (
	KAFKA_SASL_NONE         KafkaSASLAlgorithm = "none"
	KAFKA_SASL_PLAIN        KafkaSASLAlgorithm = "plain"
	KAFKA_SASL_SCRAM_SHA256 KafkaSASLAlgorithm = "scram-sha256"
	KAFKA_SASL_SCRAM_SHA512 KafkaSASLAlgorithm = "scram-sha512"
)

var (
	compressionCodecs = map[string]kgo.CompressionCodec{
		"none":   kgo.NoCompression(),
		"gzip":   kgo.GzipCompression(),
		"snappy": kgo.SnappyCompression(),
		"lz4":    kgo.Lz4Compression(),
		"zstd":   kgo.ZstdCompression(),
	}

	saslAlgorithms = map[KafkaSASLAlgorithm]bool{
		KAFKA_SASL_PLAIN:        true,
		KAFKA_SASL_SCRAM_SHA256: true,
		KAFKA_SASL_SCRAM_SHA512: true,
	}
	saslAlgorithmsList = []string{
		string(KAFKA_SASL_NONE),
		string(KAFKA_SASL_PLAIN),
		string(KAFKA_SASL_SCRAM_SHA256),
		string(KAFKA_SASL_SCRAM_SHA512),
	}
)

func (d *KafkaDriver) Prepare() error {
	flag.BoolVar(&d.kafkaTLS, "transport.kafka.tls", false, "Use TLS to connect to Kafka")

	flag.StringVar(&d.kafkaClientCert, "transport.kafka.tls.client", "", "Kafka client certificate")
	flag.StringVar(&d.kafkaClientKey, "transport.kafka.tls.key", "", "Kafka client key")
	flag.StringVar(&d.kafkaServerCA, "transport.kafka.tls.ca", "", "Kafka certificate authority")
	flag.BoolVar(&d.kafkaTlsInsecure, "transport.kafka.tls.insecure", false, "Skips TLS verification")

	flag.StringVar(&d.kafkaSASL, "transport.kafka.sasl", "none",
		fmt.Sprintf(
			"Use SASL to connect to Kafka, available settings: %s (TLS is recommended and the environment variables KAFKA_SASL_USER and KAFKA_SASL_PASS need to be set)",
			strings.Join(saslAlgorithmsList, ", ")))

	flag.StringVar(&d.kafkaTopic, "transport.kafka.topic", "flow-messages", "Kafka topic to produce to")
	flag.StringVar(&d.kafkaSrv, "transport.kafka.srv", "", "SRV record containing a list of Kafka brokers (or use brokers)")
	flag.StringVar(&d.kafkaBrk, "transport.kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	flag.IntVar(&d.kafkaMaxMsgBytes, "transport.kafka.maxmsgbytes", 1000000, "Kafka max message bytes")
	flag.IntVar(&d.kafkaFlushBytes, "transport.kafka.flushbytes", 100*1024*1024, "Kafka maximum batch bytes")
	flag.DurationVar(&d.kafkaFlushFrequency, "transport.kafka.flushfreq", time.Second*5, "Kafka flush frequency")

	flag.BoolVar(&d.kafkaHashing, "transport.kafka.hashing", false, "Enable partition hashing")

	flag.StringVar(&d.kafkaVersion, "transport.kafka.version", "2.8.0", "Kafka version")
	flag.StringVar(&d.kafkaCompressionCodec, "transport.kafka.compression", "", "Kafka default compression")

	return nil
}

// Errors returns the Kafka producer error channel.
func (d *KafkaDriver) Errors() <-chan error {
	return d.errors
}

// Init configures the Kafka producer and establishes connections.
func (d *KafkaDriver) Init() error {
	kafkaConfigVersion := kversion.FromString(d.kafkaVersion)
	if kafkaConfigVersion == nil {
		return fmt.Errorf("invalid Kafka version %q", d.kafkaVersion)
	}

	if d.kafkaMaxMsgBytes <= 0 || d.kafkaFlushBytes <= 0 || d.kafkaMaxMsgBytes > int(^uint32(0)>>1) || d.kafkaFlushBytes > int(^uint32(0)>>1) {
		return errors.New("kafka message and batch byte limits must be positive and at most 2147483647")
	}
	batchBytes := min(d.kafkaMaxMsgBytes, d.kafkaFlushBytes)
	opts := []kgo.Opt{
		kgo.MaxVersions(kafkaConfigVersion),
		kgo.ProducerBatchMaxBytes(int32(batchBytes)),
		kgo.ProducerLinger(d.kafkaFlushFrequency),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.WithHooks(newKafkaMetrics()),
	}

	if d.kafkaCompressionCodec != "" {
		cc, ok := compressionCodecs[strings.ToLower(d.kafkaCompressionCodec)]
		if !ok {
			return fmt.Errorf("compression codec does not exist")
		}
		opts = append(opts, kgo.ProducerBatchCompression(cc))
	}

	if d.kafkaTLS {
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			return fmt.Errorf("error initializing TLS: %v", err)
		}
		tlsConfig := &tls.Config{
			RootCAs:    rootCAs,
			MinVersion: tls.VersionTLS12,
		}

		tlsConfig.InsecureSkipVerify = d.kafkaTlsInsecure

		if d.kafkaServerCA != "" {
			serverCaFile, err := os.Open(d.kafkaServerCA)
			if err != nil {
				return fmt.Errorf("error initializing server CA: %v", err)
			}

			serverCaBytes, err := io.ReadAll(serverCaFile)
			if err != nil {
				if closeErr := serverCaFile.Close(); closeErr != nil {
					return fmt.Errorf("error closing server CA: %v", closeErr)
				}
				return fmt.Errorf("error reading server CA: %v", err)
			}
			if err := serverCaFile.Close(); err != nil {
				return fmt.Errorf("error closing server CA: %v", err)
			}

			block, _ := pem.Decode(serverCaBytes)

			serverCa, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return fmt.Errorf("error parsing server CA: %v", err)
			}

			certPool := x509.NewCertPool()
			certPool.AddCert(serverCa)

			tlsConfig.RootCAs = certPool
		}

		if d.kafkaClientCert != "" && d.kafkaClientKey != "" {
			_, err := tls.LoadX509KeyPair(d.kafkaClientCert, d.kafkaClientKey)
			if err != nil {
				return fmt.Errorf("error initializing mTLS: %v", err)
			}

			tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert, err := tls.LoadX509KeyPair(d.kafkaClientCert, d.kafkaClientKey)
				if err != nil {
					return nil, err
				}
				return &cert, nil
			}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if d.kafkaHashing {
		opts = append(opts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	}

	kafkaSASL := KafkaSASLAlgorithm(d.kafkaSASL)
	if d.kafkaSASL != "" && kafkaSASL != KAFKA_SASL_NONE {
		_, ok := saslAlgorithms[KafkaSASLAlgorithm(strings.ToLower(d.kafkaSASL))]
		if !ok {
			return errors.New("sasl algorithm does not exist")
		}

		user := os.Getenv("KAFKA_SASL_USER")
		password := os.Getenv("KAFKA_SASL_PASS")
		if user == "" && password == "" {
			return fmt.Errorf("kafka SASL config from environment was unsuccessful: KAFKA_SASL_USER and KAFKA_SASL_PASS need to be set")
		}

		switch kafkaSASL {
		case KAFKA_SASL_PLAIN:
			opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: password}.AsMechanism()))
		case KAFKA_SASL_SCRAM_SHA256:
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha256Mechanism()))
		case KAFKA_SASL_SCRAM_SHA512:
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha512Mechanism()))
		}
	}

	var addrs []string
	if d.kafkaSrv != "" {
		var err error
		addrs, err = GetServiceAddresses(d.kafkaSrv)
		if err != nil {
			return err
		}
	} else {
		addrs = strings.Split(d.kafkaBrk, ",")
	}

	opts = append(opts, kgo.SeedBrokers(addrs...))
	kafkaProducer, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	if err := kafkaProducer.Ping(context.Background()); err != nil {
		kafkaProducer.Close()
		return err
	}
	d.producer = kafkaProducer

	return nil
}

func newKafkaMetrics() *kprom.Metrics {
	return kprom.NewMetrics("goflow2", kprom.Subsystem("kafka"), kprom.Registerer(prometheus.DefaultRegisterer))
}

// Send publishes a message to Kafka.
func (d *KafkaDriver) Send(key, data []byte) error {
	d.producer.Produce(context.Background(), &kgo.Record{
		Topic: d.kafkaTopic,
		Key:   key,
		Value: data,
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			select {
			case d.errors <- &KafkaTransportError{err}:
			default:
			}
		}
	})
	return nil
}

// Close stops the producer and releases resources.
func (d *KafkaDriver) Close() error {
	err := d.producer.Flush(context.Background())
	d.producer.Close()
	close(d.errors)
	return err
}

// GetServiceAddresses resolves SRV records into broker addresses.
func GetServiceAddresses(srv string) (addrs []string, err error) {
	_, srvs, err := net.LookupSRV("", "", srv)
	if err != nil {
		return nil, fmt.Errorf("service discovery: %v", err)
	}
	for _, srv := range srvs {
		addrs = append(addrs, net.JoinHostPort(srv.Target, strconv.Itoa(int(srv.Port))))
	}
	return addrs, nil
}

func init() {
	d := &KafkaDriver{
		errors: make(chan error),
	}
	transport.RegisterTransportDriver("kafka", d)
}
