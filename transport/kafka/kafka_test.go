package kafka

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/netsampler/goflow2/v3/pkg/goflow2/httpserver"
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
