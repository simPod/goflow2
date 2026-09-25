package kafka

import (
	"errors"
	"flag"
	"fmt"
	"sync"
	"testing"
	"time"

	sarama "github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metrics "github.com/rcrowley/go-metrics"
)

func TestDiagnosticsFlags(t *testing.T) {
	old := flag.CommandLine
	defer func() { flag.CommandLine = old }()
	flag.CommandLine = flag.NewFlagSet("kafka-diagnostics-test", flag.ContinueOnError)
	d := &KafkaDriver{}
	if err := d.Prepare(); err != nil {
		t.Fatal(err)
	}
	if d.kafkaDiagnostics || d.kafkaDiagnosticSuccesses {
		t.Fatal("diagnostics must default to disabled")
	}
	if err := flag.CommandLine.Parse([]string{"-diagnostics.kafka.successes=true"}); err != nil {
		t.Fatal(err)
	}
	if d.kafkaDiagnostics || !d.kafkaDiagnosticSuccesses {
		t.Fatal("success flag must not enable diagnostics implicitly")
	}
	if err := flag.CommandLine.Set("diagnostics.kafka", "true"); err != nil {
		t.Fatal(err)
	}
	if !d.kafkaDiagnostics {
		t.Fatal("Kafka diagnostics flag not applied")
	}
}

func gatherDiagnostics(t *testing.T, r *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]*dto.MetricFamily)
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func metricValue(t *testing.T, families map[string]*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	for _, m := range families[name].GetMetric() {
		if len(m.Label) != len(labels) {
			continue
		}
		match := true
		for _, l := range m.Label {
			if labels[l.GetName()] != l.GetValue() {
				match = false
			}
		}
		if match {
			if m.Gauge != nil {
				return m.Gauge.GetValue()
			}
			return m.Counter.GetValue()
		}
	}
	t.Fatalf("missing %s %v", name, labels)
	return 0
}

func TestDiagnosticsSnapshots(t *testing.T) {
	r := metrics.NewRegistry()
	defer r.UnregisterAll()
	for name, value := range map[string]int64{
		"request-latency-in-ms-for-broker-12": 250,
		"batch-size":                          2048,
		"records-per-request":                 100,
		"request-size-for-broker-12":          8192,
		"response-size":                       64,
	} {
		h := metrics.NewHistogram(metrics.NewUniformSample(100))
		h.Update(value)
		if err := r.Register(name, h); err != nil {
			t.Fatal(err)
		}
	}
	metrics.GetOrRegisterCounter("requests-in-flight-for-broker-12", r).Inc(3)
	for _, name := range []string{"request-rate-for-broker-12", "response-rate", "incoming-byte-rate-for-broker-12", "outgoing-byte-rate", "record-send-rate"} {
		metrics.GetOrRegisterMeter(name, r).Mark(42)
	}
	// Unlisted names, topic names, invalid broker IDs, and wrong types must not leak.
	for _, name := range []string{"request-rate-for-topic-secret", "request-rate-for-broker-hostname", "request-rate-for-broker--1", "request-rate-for-broker-01", "retry-rate", "arbitrary"} {
		metrics.GetOrRegisterMeter(name, r).Mark(99)
	}
	metrics.GetOrRegisterCounter("batch-size-for-broker-12", r).Inc(1)
	input := make(chan *sarama.ProducerMessage, 8)
	input <- &sarama.ProducerMessage{}
	d := newKafkaDiagnostics(r, input, false)
	p := prometheus.NewPedanticRegistry()
	p.MustRegister(d)
	f := gatherDiagnostics(t, p)
	checks := []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"producer_input_queue_length", nil, 1},
		{"producer_input_queue_capacity", nil, 8},
		{"requests_in_flight", map[string]string{"broker": "12"}, 3},
		{"requests_total", map[string]string{"broker": "12"}, 42},
		{"incoming_bytes_total", map[string]string{"broker": "12"}, 42},
		{"outgoing_bytes_total", map[string]string{"broker": "all"}, 42},
		{"responses_total", map[string]string{"broker": "all"}, 42},
		{"records_sent_total", map[string]string{"broker": "all"}, 42},
	}
	for name, want := range map[string]float64{"request_latency_seconds": .25, "batch_size_bytes": 2048, "records_per_request": 100, "request_size_bytes": 8192, "response_size_bytes": 64} {
		broker := "all"
		if name == "request_latency_seconds" || name == "request_size_bytes" {
			broker = "12"
		}
		for _, stat := range []string{"mean", "p50", "p95", "p99", "max"} {
			checks = append(checks, struct {
				name   string
				labels map[string]string
				want   float64
			}{name, map[string]string{"broker": broker, "statistic": stat}, want})
		}
	}
	for _, check := range checks {
		if got := metricValue(t, f, "goflow2_kafka_"+check.name, check.labels); got != check.want {
			t.Errorf("%s: got %v want %v", check.name, got, check.want)
		}
	}
	if len(f) != 19 {
		t.Fatalf("unexpected metric families: %d", len(f))
	}
	if len(f["goflow2_kafka_requests_total"].Metric) != 1 {
		t.Fatal("unexpected broker labels")
	}
	if f["goflow2_kafka_requests_in_flight"].GetType() != dto.MetricType_GAUGE {
		t.Fatal("in-flight must be a gauge")
	}
	if f["goflow2_kafka_request_latency_seconds"].GetType() != dto.MetricType_GAUGE {
		t.Fatal("reservoir must not be a cumulative histogram")
	}
	// Compare a live meter's EWMA snapshot rather than mistaking its count for a rate.
	if got := metricValue(t, f, "goflow2_kafka_requests_per_second", map[string]string{"broker": "12"}); got != r.Get("request-rate-for-broker-12").(metrics.Meter).Snapshot().Rate1() {
		t.Fatal("wrong rate")
	}
}

type diagnosticProducer struct {
	sarama.AsyncProducer
	input     chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
	successes chan *sarama.ProducerMessage
}

func (p *diagnosticProducer) Input() chan<- *sarama.ProducerMessage     { return p.input }
func (p *diagnosticProducer) Errors() <-chan *sarama.ProducerError      { return p.errors }
func (p *diagnosticProducer) Successes() <-chan *sarama.ProducerMessage { return p.successes }
func (p *diagnosticProducer) AsyncClose()                               { close(p.errors); close(p.successes); close(p.input) }

func TestDiagnosticsDrainAndLifecycle(t *testing.T) {
	old := prometheus.DefaultRegisterer
	r := prometheus.NewPedanticRegistry()
	prometheus.DefaultRegisterer = r
	defer func() { prometheus.DefaultRegisterer = old }()
	for _, successes := range []bool{false, true, false} {
		p := &diagnosticProducer{input: make(chan *sarama.ProducerMessage), errors: make(chan *sarama.ProducerError, 4), successes: make(chan *sarama.ProducerMessage, 2)}
		d := newKafkaDiagnostics(metrics.NewRegistry(), p.input, successes)
		if err := d.register(); err != nil {
			t.Fatal(err)
		}
		duplicate := newKafkaDiagnostics(metrics.NewRegistry(), p.input, successes)
		if err := duplicate.register(); err == nil {
			t.Fatal("duplicate collector accepted")
		}
		forward := make(chan error, 1)
		p.errors <- &sarama.ProducerError{Err: fmt.Errorf("wrapped: %w", sarama.ErrRequestTimedOut)}
		p.errors <- &sarama.ProducerError{Err: errors.New("arbitrary text")}
		p.errors <- &sarama.ProducerError{Err: sarama.KError(32767)}
		p.errors <- nil
		if successes {
			p.successes <- &sarama.ProducerMessage{}
			p.successes <- nil
		}
		// Count shutdown errors too, and preserve Close's error result.
		d.closing.Store(true)
		go d.drain(p, forward)
		driver := &KafkaDriver{producers: []producerMember{{p, d}}}
		closed := make(chan error, 1)
		go func() { closed <- driver.Close() }()
		select {
		case err := <-closed:
			var producerErrors sarama.ProducerErrors
			if !errors.As(err, &producerErrors) || len(producerErrors) != 3 {
				t.Fatalf("shutdown errors: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown blocked")
		}
		if driver.producers != nil {
			t.Fatal("collector not released")
		}
		if len(gatherDiagnostics(t, r)) != 0 {
			t.Fatal("collector leaked after Close")
		}
		// Inspect the detached collector to check every error was counted.
		inspection := prometheus.NewPedanticRegistry()
		inspection.MustRegister(d)
		f := gatherDiagnostics(t, inspection)
		if got := metricValue(t, f, "goflow2_kafka_producer_errors_total", map[string]string{"code": "7"}); got != 1 {
			t.Fatal(got)
		}
		if got := metricValue(t, f, "goflow2_kafka_producer_errors_total", map[string]string{"code": "other"}); got != 2 {
			t.Fatal(got)
		}
		if got := metricValue(t, f, "goflow2_kafka_error_forwarding_dropped_total", nil); got != 2 {
			t.Fatal(got)
		}
		if successes {
			if got := metricValue(t, f, "goflow2_kafka_producer_successes_total", nil); got != 1 {
				t.Fatal(got)
			}
		} else if f["goflow2_kafka_producer_successes_total"] != nil {
			t.Fatal("success metric enabled by default")
		}
		if len(forward) != 1 {
			t.Fatal("forwarded a channel-close sentinel")
		}
		var transportError *KafkaTransportError
		if !errors.As(<-forward, &transportError) {
			t.Fatal("error wrapper changed")
		}
	}
}

func TestDiagnosticsConcurrentSnapshots(t *testing.T) {
	r := metrics.NewRegistry()
	defer r.UnregisterAll()
	h := metrics.NewHistogram(metrics.NewExpDecaySample(1028, .015))
	if err := r.Register("request-latency-in-ms", h); err != nil {
		t.Fatal(err)
	}
	m := metrics.GetOrRegisterMeter("request-rate", r)
	c := metrics.GetOrRegisterCounter("requests-in-flight", r)
	p := prometheus.NewPedanticRegistry()
	p.MustRegister(newKafkaDiagnostics(r, make(chan *sarama.ProducerMessage), false))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			h.Update(int64(i))
			m.Mark(1)
			c.Inc(1)
			c.Dec(1)
			metrics.GetOrRegisterCounter("requests-in-flight-for-broker-1", r).Inc(1)
			r.Unregister("requests-in-flight-for-broker-1")
		}
	}()
	for i := 0; i < 100; i++ {
		gatherDiagnostics(t, p)
	}
	wg.Wait()
}
