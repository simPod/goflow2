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
	"sync"
	"time"

	"github.com/netsampler/goflow2/v2/transport"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
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

	kafkaSASL               string
	kafkaTopic              string
	kafkaSrv                string
	kafkaBrk                string
	kafkaMaxMsgBytes        int
	kafkaFlushBytes         int
	kafkaFlushFrequency     time.Duration
	kafkaMaxBufferedRecords int
	kafkaPingTimeout        time.Duration
	kafkaFlushTimeout       time.Duration
	kafkaFlushStallTimeout  time.Duration

	kafkaHashing          bool
	kafkaVersion          string
	kafkaCompressionCodec string

	producer     *kgo.Client
	callbacks    sync.WaitGroup
	cancelClient context.CancelFunc

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
	enqueueDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "goflow2",
		Subsystem: "kafka",
		Name:      "enqueue_duration_seconds",
		Help:      "Time spent enqueueing a record in franz-go, including buffer waits but not broker delivery.",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 5, 9),
	})
	producerErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "goflow2",
		Subsystem: "kafka",
		Name:      "producer_errors_total",
		Help:      "Kafka records that failed to produce, by bounded error code.",
	}, []string{"code"})
	errorForwardingDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "goflow2",
		Subsystem: "kafka",
		Name:      "error_forwarding_dropped_total",
		Help:      "Kafka error notifications dropped because the application error channel was not ready.",
	})
	producerBufferCapacity = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "goflow2",
		Subsystem: "kafka",
		Name:      "producer_buffer_capacity_records",
		Help:      "Configured record buffer limit for the active Kafka producer.",
	})

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
	flag.DurationVar(&d.kafkaFlushFrequency, "transport.kafka.flushfreq", -1, "Kafka linger time (-1ns uses the client default)")
	flag.IntVar(&d.kafkaMaxBufferedRecords, "transport.kafka.maxbufferedrecords", 100000, "Maximum buffered Kafka records")
	flag.DurationVar(&d.kafkaPingTimeout, "transport.kafka.pingtimeout", 30*time.Second, "Kafka startup timeout, including SRV lookup and Ping")
	flag.DurationVar(&d.kafkaFlushTimeout, "transport.kafka.flushtimeout", 0, "Maximum Kafka shutdown flush duration (0 waits indefinitely; expiry may lose records)")
	flag.DurationVar(&d.kafkaFlushStallTimeout, "transport.kafka.flushstalltimeout", 0, "Maximum Kafka shutdown time without buffer progress (0 waits indefinitely; expiry may lose records)")

	flag.BoolVar(&d.kafkaHashing, "transport.kafka.hashing", true, "Hash non-nil keys with the adaptive partitioner; false ignores keys")

	flag.StringVar(&d.kafkaVersion, "transport.kafka.version", "", "Optional maximum Kafka version (empty negotiates automatically)")
	flag.StringVar(&d.kafkaCompressionCodec, "transport.kafka.compression", "", "Kafka default compression")

	return nil
}

// Errors returns the Kafka producer error channel.
func (d *KafkaDriver) Errors() <-chan error {
	return d.errors
}

// Init configures the Kafka producer and establishes connections.
func (d *KafkaDriver) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), d.kafkaPingTimeout)
	defer cancel()
	return d.initProducer(ctx)
}

func (d *KafkaDriver) initProducer(ctx context.Context) error {
	opts, err := d.producerOptions()
	if err != nil {
		return err
	}
	var addrs []string
	if d.kafkaSrv != "" {
		addrs, err = getServiceAddresses(ctx, d.kafkaSrv)
		if err != nil {
			return err
		}
	} else {
		addrs = strings.Split(d.kafkaBrk, ",")
	}
	clientCtx, cancelClient := context.WithCancel(context.Background())
	opts = append(opts, kgo.SeedBrokers(addrs...), kgo.WithHooks(newKafkaMetrics()), kgo.WithContext(clientCtx))
	kafkaProducer, err := kgo.NewClient(opts...)
	if err != nil {
		cancelClient()
		return err
	}
	pingErrors := make(chan error, 1)
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		pingErrors <- kafkaProducer.Ping(ctx)
	}()
	select {
	case err = <-pingErrors:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		cancelClient()
		kafkaProducer.Close()
		<-pingDone
		return fmt.Errorf("kafka startup ping: %w", err)
	}
	<-pingDone
	d.producer = kafkaProducer
	d.cancelClient = cancelClient
	producerBufferCapacity.Set(float64(d.kafkaMaxBufferedRecords))
	return nil
}

func (d *KafkaDriver) producerOptions() ([]kgo.Opt, error) {
	if d.kafkaMaxMsgBytes <= 0 || d.kafkaFlushBytes <= 0 || d.kafkaMaxMsgBytes > int(^uint32(0)>>1) || d.kafkaFlushBytes > int(^uint32(0)>>1) {
		return nil, errors.New("kafka message and batch byte limits must be positive and at most 2147483647")
	}
	if d.kafkaMaxBufferedRecords <= 0 {
		return nil, errors.New("transport.kafka.maxbufferedrecords must be positive")
	}
	if d.kafkaPingTimeout <= 0 || d.kafkaFlushTimeout < 0 || d.kafkaFlushStallTimeout < 0 || d.kafkaFlushFrequency < -1 {
		return nil, errors.New("kafka ping timeout must be positive, flush timeouts nonnegative, and linger at least -1ns")
	}
	batchBytes := min(d.kafkaMaxMsgBytes, d.kafkaFlushBytes)
	opts := []kgo.Opt{
		kgo.ProducerBatchMaxBytes(int32(batchBytes)),
		kgo.MaxBufferedRecords(d.kafkaMaxBufferedRecords),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}
	if d.kafkaVersion != "" {
		version := kversion.FromString(d.kafkaVersion)
		if version == nil {
			return nil, fmt.Errorf("invalid Kafka version %q", d.kafkaVersion)
		}
		opts = append(opts, kgo.MaxVersions(version))
	}
	if d.kafkaFlushFrequency >= 0 {
		opts = append(opts, kgo.ProducerLinger(d.kafkaFlushFrequency))
	}

	if d.kafkaCompressionCodec != "" {
		cc, ok := compressionCodecs[strings.ToLower(d.kafkaCompressionCodec)]
		if !ok {
			return nil, fmt.Errorf("compression codec does not exist")
		}
		opts = append(opts, kgo.ProducerBatchCompression(cc))
	}

	if d.kafkaTLS {
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("error initializing TLS: %v", err)
		}
		tlsConfig := &tls.Config{
			RootCAs:    rootCAs,
			MinVersion: tls.VersionTLS12,
		}

		tlsConfig.InsecureSkipVerify = d.kafkaTlsInsecure

		if d.kafkaServerCA != "" {
			serverCaFile, err := os.Open(d.kafkaServerCA)
			if err != nil {
				return nil, fmt.Errorf("error initializing server CA: %v", err)
			}

			serverCaBytes, err := io.ReadAll(serverCaFile)
			if err != nil {
				if closeErr := serverCaFile.Close(); closeErr != nil {
					return nil, fmt.Errorf("error closing server CA: %v", closeErr)
				}
				return nil, fmt.Errorf("error reading server CA: %v", err)
			}
			if err := serverCaFile.Close(); err != nil {
				return nil, fmt.Errorf("error closing server CA: %v", err)
			}

			block, _ := pem.Decode(serverCaBytes)
			if block == nil {
				return nil, errors.New("error parsing server CA: no PEM certificate found")
			}

			serverCa, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("error parsing server CA: %v", err)
			}

			certPool := x509.NewCertPool()
			certPool.AddCert(serverCa)

			tlsConfig.RootCAs = certPool
		}

		if d.kafkaClientCert != "" && d.kafkaClientKey != "" {
			_, err := tls.LoadX509KeyPair(d.kafkaClientCert, d.kafkaClientKey)
			if err != nil {
				return nil, fmt.Errorf("error initializing mTLS: %v", err)
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

	if !d.kafkaHashing {
		opts = append(opts, kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, false, nil)))
	}

	kafkaSASL := KafkaSASLAlgorithm(strings.ToLower(d.kafkaSASL))
	if d.kafkaSASL != "" && kafkaSASL != KAFKA_SASL_NONE {
		_, ok := saslAlgorithms[KafkaSASLAlgorithm(strings.ToLower(d.kafkaSASL))]
		if !ok {
			return nil, errors.New("sasl algorithm does not exist")
		}

		user := os.Getenv("KAFKA_SASL_USER")
		password := os.Getenv("KAFKA_SASL_PASS")
		if user == "" && password == "" {
			return nil, fmt.Errorf("kafka SASL config from environment was unsuccessful: KAFKA_SASL_USER and KAFKA_SASL_PASS need to be set")
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

	return opts, nil
}

func newKafkaMetrics() *kprom.Metrics {
	return kprom.NewMetrics("goflow2", kprom.Subsystem("kafka"), kprom.Registerer(prometheus.DefaultRegisterer))
}

func producerErrorCode(err error) string {
	var brokerError *kerr.Error
	switch {
	case errors.As(err, &brokerError):
		return strconv.Itoa(int(brokerError.Code))
	case errors.Is(err, kgo.ErrMaxBuffered):
		return "max_buffered"
	case errors.Is(err, kgo.ErrRecordTimeout):
		return "record_timeout"
	case errors.Is(err, kgo.ErrRecordRetries):
		return "record_retries"
	case errors.Is(err, kgo.ErrClientClosed):
		return "client_closed"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "other"
	}
}

// Send publishes a message to Kafka.
func (d *KafkaDriver) Send(key, data []byte) error {
	start := time.Now()
	d.callbacks.Add(1)
	d.producer.Produce(context.Background(), &kgo.Record{
		Topic: d.kafkaTopic,
		Key:   key,
		Value: data,
	}, func(_ *kgo.Record, err error) {
		defer d.callbacks.Done()
		if err != nil {
			producerErrors.WithLabelValues(producerErrorCode(err)).Inc()
			select {
			case d.errors <- &KafkaTransportError{err}:
			default:
				errorForwardingDropped.Inc()
			}
		}
	})
	enqueueDuration.Observe(time.Since(start).Seconds())
	return nil
}

// Close stops the producer and releases resources. Callers must stop Send first.
func (d *KafkaDriver) Close() error {
	ctx := context.Background()
	if d.kafkaFlushTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.kafkaFlushTimeout)
		defer cancel()
	}
	err := flushWhileProgressing(ctx, d.producer, 5*time.Second, d.kafkaFlushStallTimeout)
	if err != nil {
		err = fmt.Errorf("kafka shutdown flush: %w (%d outstanding records may not be delivered; no disk fallback)", err, d.producer.BufferedProduceRecords())
	}
	if d.cancelClient != nil {
		d.cancelClient()
	}
	d.producer.Close()
	d.callbacks.Wait()
	close(d.errors)
	producerBufferCapacity.Set(0)
	return err
}

type flushingProducer interface {
	Flush(context.Context) error
	BufferedProduceRecords() int64
}

func flushWhileProgressing(ctx context.Context, producer flushingProducer, interval, stallTimeout time.Duration) error {
	lastProgress := time.Now()
	for {
		before := producer.BufferedProduceRecords()
		if before == 0 {
			return nil
		}
		window := interval
		if stallTimeout > 0 {
			remaining := stallTimeout - time.Since(lastProgress)
			if remaining <= 0 {
				return fmt.Errorf("no flush progress for %s: %w", stallTimeout, context.DeadlineExceeded)
			}
			window = min(window, remaining)
		}
		flushCtx, cancel := context.WithTimeout(ctx, window)
		err := producer.Flush(flushCtx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if producer.BufferedProduceRecords() < before {
			lastProgress = time.Now()
		}
	}
}

// GetServiceAddresses resolves SRV records into broker addresses.
func GetServiceAddresses(srv string) (addrs []string, err error) {
	return getServiceAddresses(context.Background(), srv)
}

func getServiceAddresses(ctx context.Context, srv string) (addrs []string, err error) {
	_, srvs, err := net.DefaultResolver.LookupSRV(ctx, "", "", srv)
	if err != nil {
		return nil, fmt.Errorf("service discovery: %w", err)
	}
	for _, srv := range srvs {
		addrs = append(addrs, net.JoinHostPort(srv.Target, strconv.Itoa(int(srv.Port))))
	}
	return addrs, nil
}

func init() {
	prometheus.MustRegister(enqueueDuration)
	prometheus.MustRegister(producerErrors)
	prometheus.MustRegister(errorForwardingDropped)
	prometheus.MustRegister(producerBufferCapacity)
	d := &KafkaDriver{
		errors: make(chan error),
	}
	transport.RegisterTransportDriver("kafka", d)
}
