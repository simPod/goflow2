package kafka

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/netsampler/goflow2/v3/pkg/goflow2/httpserver"
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
