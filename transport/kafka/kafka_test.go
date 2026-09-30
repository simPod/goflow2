package kafka

import (
	"context"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v2/pkg/goflow2/httpserver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func preparedDriver(t *testing.T) *KafkaDriver {
	t.Helper()
	previous := flag.CommandLine
	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	t.Cleanup(func() { flag.CommandLine = previous })
	driver := &KafkaDriver{errors: make(chan error)}
	require.NoError(t, driver.Prepare())
	return driver
}

func TestProducerDefaultsAndExplicitOverrides(t *testing.T) {
	driver := preparedDriver(t)
	opts, err := driver.producerOptions()
	require.NoError(t, err)
	client, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	defaults, err := kgo.NewClient()
	require.NoError(t, err)
	t.Cleanup(defaults.Close)
	assert.Equal(t, int64(100000), client.OptValue(kgo.MaxBufferedRecords))
	assert.Equal(t, defaults.OptValue(kgo.RequiredAcks), client.OptValue(kgo.RequiredAcks))
	assert.Equal(t, defaults.OptValue(kgo.DisableIdempotentWrite), client.OptValue(kgo.DisableIdempotentWrite))
	assert.Equal(t, defaults.OptValue(kgo.MaxVersions), client.OptValue(kgo.MaxVersions))
	assert.Equal(t, defaults.OptValue(kgo.ProducerLinger), client.OptValue(kgo.ProducerLinger))
	assert.IsType(t, defaults.OptValue(kgo.RecordPartitioner), client.OptValue(kgo.RecordPartitioner))
	assert.Equal(t, []kgo.CompressionCodec{kgo.NoCompression()}, client.OptValue(kgo.ProducerBatchCompression))

	require.NoError(t, flag.CommandLine.Set("transport.kafka.maxbufferedrecords", "200000"))
	require.NoError(t, flag.CommandLine.Set("transport.kafka.flushfreq", "0s"))
	require.NoError(t, flag.CommandLine.Set("transport.kafka.version", "3.5.0"))
	opts, err = driver.producerOptions()
	require.NoError(t, err)
	overridden, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(overridden.Close)
	assert.Equal(t, int64(200000), overridden.OptValue(kgo.MaxBufferedRecords))
	assert.Equal(t, time.Duration(0), overridden.OptValue(kgo.ProducerLinger))
	assert.NotEqual(t, defaults.OptValue(kgo.MaxVersions), overridden.OptValue(kgo.MaxVersions))

	driver.kafkaMaxBufferedRecords = 0
	_, err = driver.producerOptions()
	require.ErrorContains(t, err, "maxbufferedrecords must be positive")
}

func TestStartupPingTimeoutCleansUpClient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn) // Accept Kafka requests without replying.
			}()
		}
	}()
	driver := preparedDriver(t)
	driver.kafkaBrk = listener.Addr().String()
	driver.kafkaPingTimeout = 20 * time.Millisecond
	started := time.Now()
	err = driver.Init()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, driver.producer)
	assert.Less(t, time.Since(started), time.Second, "startup must not wait for the broker connection timeout")
	// Failed startup must release kprom collectors before another attempt.
	err = driver.Init()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, driver.producer)
}

type testFlushingProducer struct {
	records int64
	flush   func(context.Context) error
}

func (p *testFlushingProducer) BufferedProduceRecords() int64   { return p.records }
func (p *testFlushingProducer) Flush(ctx context.Context) error { return p.flush(ctx) }

func TestGradualFlush(t *testing.T) {
	t.Run("progress continues beyond one window", func(t *testing.T) {
		producer := &testFlushingProducer{records: 3}
		calls := 0
		producer.flush = func(ctx context.Context) error {
			calls++
			<-ctx.Done()
			producer.records--
			return ctx.Err()
		}
		require.NoError(t, flushWhileProgressing(context.Background(), producer, time.Millisecond, time.Second))
		assert.Equal(t, 3, calls)
	})
	t.Run("no loss deadline by default", func(t *testing.T) {
		producer := &testFlushingProducer{records: 1}
		calls := 0
		producer.flush = func(ctx context.Context) error {
			calls++
			if calls == 3 {
				producer.records = 0
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		}
		require.NoError(t, flushWhileProgressing(context.Background(), producer, time.Millisecond, 0))
		assert.Equal(t, 3, calls)
	})
	t.Run("configured stall deadline", func(t *testing.T) {
		producer := &testFlushingProducer{records: 1}
		producer.flush = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
		err := flushWhileProgressing(context.Background(), producer, time.Millisecond, 5*time.Millisecond)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, int64(1), producer.records)
	})
	t.Run("overall deadline despite progress", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		producer := &testFlushingProducer{records: 1000}
		producer.flush = func(ctx context.Context) error {
			<-ctx.Done()
			producer.records--
			return ctx.Err()
		}
		require.ErrorIs(t, flushWhileProgressing(ctx, producer, time.Millisecond, time.Second), context.DeadlineExceeded)
		assert.Positive(t, producer.records)
	})
}

func TestCloseReportsOutstandingRecordsOnTimeout(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	driver := &KafkaDriver{
		producer: client, kafkaTopic: "flows", errors: make(chan error),
		kafkaFlushTimeout: 10 * time.Millisecond,
	}
	require.NoError(t, driver.Send(nil, []byte("flow")))
	err = driver.Close()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "no disk fallback")
	assert.Zero(t, client.BufferedProduceRecords())
	_, open := <-driver.Errors()
	assert.False(t, open)
}

func TestKafkaMetricsOnExistingEndpoint(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.WithHooks(newKafkaMetrics()))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	httpserver.New(httpserver.Config{}, nil, nil).ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	assert.Contains(t, response.Body.String(), "goflow2_kafka_buffered_produce_records")
}

func TestSendRecordsKafkaEnqueueDuration(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	count := func() uint64 {
		families, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, family := range families {
			if family.GetName() == "goflow2_kafka_enqueue_duration_seconds" {
				return family.GetMetric()[0].GetHistogram().GetSampleCount()
			}
		}
		t.Fatal("Kafka enqueue duration metric is not registered")
		return 0
	}

	before := count()
	driver := &KafkaDriver{producer: client, kafkaTopic: "flows", errors: make(chan error)}
	t.Cleanup(func() { client.Close(); driver.callbacks.Wait() })
	require.NoError(t, driver.Send(nil, []byte("flow")))
	assert.Equal(t, before+1, count())
}

func TestKafkaErrorsRemainCountedWhenNotificationCannotBeForwarded(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	metricValue := func(name, code string) float64 {
		t.Helper()
		families, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, family := range families {
			if family.GetName() != name {
				continue
			}
			for _, metric := range family.GetMetric() {
				if code == "" {
					return metric.GetCounter().GetValue()
				}
				for _, label := range metric.GetLabel() {
					if label.GetName() == "code" && label.GetValue() == code {
						return metric.GetCounter().GetValue()
					}
				}
			}
		}
		return 0
	}

	beforeErrors := metricValue("goflow2_kafka_producer_errors_total", "other")
	beforeDropped := metricValue("goflow2_kafka_error_forwarding_dropped_total", "")

	// A missing topic fails before contacting a broker.
	driver := &KafkaDriver{producer: client, errors: make(chan error)}
	t.Cleanup(func() { client.Close(); driver.callbacks.Wait() })
	require.NoError(t, driver.Send(nil, []byte("flow")))
	require.Eventually(t, func() bool {
		return metricValue("goflow2_kafka_producer_errors_total", "other") == beforeErrors+1
	}, time.Second*5, time.Millisecond*10)
	driver.callbacks.Wait()
	assert.Equal(t, beforeDropped+1, metricValue("goflow2_kafka_error_forwarding_dropped_total", ""))

	driver.errors = make(chan error, 1)
	require.NoError(t, driver.Send(nil, []byte("flow")))
	select {
	case err := <-driver.Errors():
		require.Error(t, err)
	case <-time.After(time.Second * 5):
		t.Fatal("producer error was not forwarded in time")
	}
	assert.Equal(t, beforeErrors+2, metricValue("goflow2_kafka_producer_errors_total", "other"))
	assert.Equal(t, beforeDropped+1, metricValue("goflow2_kafka_error_forwarding_dropped_total", ""))
}
