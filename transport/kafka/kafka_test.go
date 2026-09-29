package kafka

import (
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
	require.NoError(t, driver.Send(nil, []byte("flow")))
	require.Eventually(t, func() bool {
		return metricValue("goflow2_kafka_producer_errors_total", "other") == beforeErrors+1
	}, time.Second*5, time.Millisecond*10)
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
