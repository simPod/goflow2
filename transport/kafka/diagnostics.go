package kafka

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"

	sarama "github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/rcrowley/go-metrics"
)

// Only these Sarama 1.38.1 registry entries are exported. Topic, partition,
// protocol, and arbitrary registry names never become labels. Broker IDs are
// nonnegative int32 values; "all" includes bootstrap traffic. Broker metrics
// can reset when Sarama closes/reopens a connection.
type saramaMetric struct {
	name   string
	desc   *prometheus.Desc
	rate   *prometheus.Desc
	scale  float64
	kind   byte
	broker bool
}

func metricDesc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc("goflow2_kafka_"+name, help, labels, nil)
}

func saramaMetrics() []saramaMetric {
	meter := func(source, name, help string, broker bool) saramaMetric {
		return saramaMetric{source, metricDesc(name+"_total", help+"; Sarama meter count, can reset on reconnect.", "broker"), metricDesc(name+"_per_second", help+" per second; one-minute EWMA (Sarama ticks every five seconds).", "broker"), 1, 'm', broker}
	}
	hist := func(source, name, help string, scale float64, broker bool) saramaMetric {
		return saramaMetric{source, metricDesc(name, help+"; sampled exponentially decaying reservoir, not cumulative histogram buckets or sums.", "broker", "statistic"), nil, scale, 'h', broker}
	}
	return []saramaMetric{
		meter("request-rate", "requests", "All protocol requests sent", true),
		meter("response-rate", "responses", "Protocol responses received", true),
		meter("incoming-byte-rate", "incoming_bytes", "Protocol bytes received", true),
		meter("outgoing-byte-rate", "outgoing_bytes", "Protocol bytes sent", true),
		meter("record-send-rate", "records_sent", "Records encoded in produce requests, including retries; not acknowledgements", false),
		hist("request-latency-in-ms", "request_latency_seconds", "Request latency in seconds, from Sarama integer milliseconds", .001, true),
		hist("request-size", "request_size_bytes", "Protocol request size in bytes", 1, true),
		hist("response-size", "response_size_bytes", "Protocol response size in bytes", 1, true),
		hist("batch-size", "batch_size_bytes", "Encoded bytes per partition per produce request, including partition framing", 1, false),
		hist("records-per-request", "records_per_request", "Records per encoded produce request across all topics", 1, false),
		{name: "requests-in-flight", desc: metricDesc("requests_in_flight", "Current protocol requests in flight (Sarama up/down counter).", "broker"), kind: 'g', broker: true},
	}
}

type kafkaDiagnostics struct {
	registry                   metrics.Registry
	input                      chan<- *sarama.ProducerMessage
	metrics                    []saramaMetric
	queueLength, queueCapacity *prometheus.Desc
	errors                     *prometheus.CounterVec
	drops                      prometheus.Counter
	successes                  prometheus.Counter
	registerer                 prometheus.Registerer
	closing                    atomic.Bool
	done                       chan struct{}
	closeErrors                sarama.ProducerErrors // Written by drain; read only after done closes.
}

func newKafkaDiagnostics(registry metrics.Registry, input chan<- *sarama.ProducerMessage, successes bool) *kafkaDiagnostics {
	d := &kafkaDiagnostics{
		registry: registry, input: input, metrics: saramaMetrics(), done: make(chan struct{}),
		queueLength:   metricDesc("producer_input_queue_length", "Messages currently buffered in the async producer Input channel; excludes internal queues and blocked senders."),
		queueCapacity: metricDesc("producer_input_queue_capacity", "Async producer Input channel capacity in messages; zero means unbuffered."),
		errors:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "goflow2_kafka_producer_errors_total", Help: "Terminal async producer message errors, counted before lossy forwarding; not retry attempts."}, []string{"code"}),
		drops:         prometheus.NewCounter(prometheus.CounterOpts{Name: "goflow2_kafka_error_forwarding_dropped_total", Help: "Producer errors dropped by the nonblocking transport error forwarding channel."}),
	}
	if successes {
		d.successes = prometheus.NewCounter(prometheus.CounterOpts{Name: "goflow2_kafka_producer_successes_total", Help: "Successful producer message completions; acknowledgement semantics follow configured RequiredAcks."})
	}
	return d
}

func (d *kafkaDiagnostics) register() error {
	d.registerer = prometheus.DefaultRegisterer
	return d.registerer.Register(d)
}

func (d *kafkaDiagnostics) Describe(ch chan<- *prometheus.Desc) {
	ch <- d.queueLength
	ch <- d.queueCapacity
	for _, m := range d.metrics {
		ch <- m.desc
		if m.rate != nil {
			ch <- m.rate
		}
	}
	d.errors.Describe(ch)
	d.drops.Describe(ch)
	if d.successes != nil {
		d.successes.Describe(ch)
	}
}

func (d *kafkaDiagnostics) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(d.queueLength, prometheus.GaugeValue, float64(len(d.input)))
	ch <- prometheus.MustNewConstMetric(d.queueCapacity, prometheus.GaugeValue, float64(cap(d.input)))
	// Each copies the registry map under its lock. Each metric is then read from
	// a concurrency-safe snapshot. No measurements are added at scrape time.
	d.registry.Each(func(name string, value interface{}) {
		for _, m := range d.metrics {
			broker := "all"
			if name != m.name {
				if !m.broker || !strings.HasPrefix(name, m.name+"-for-broker-") {
					continue
				}
				broker = strings.TrimPrefix(name, m.name+"-for-broker-")
				id, err := strconv.ParseInt(broker, 10, 32)
				if err != nil || id < 0 || strconv.FormatInt(id, 10) != broker {
					continue
				}
			}
			switch m.kind {
			case 'm':
				if v, ok := value.(metrics.Meter); ok {
					s := v.Snapshot()
					ch <- prometheus.MustNewConstMetric(m.desc, prometheus.CounterValue, float64(s.Count()), broker)
					ch <- prometheus.MustNewConstMetric(m.rate, prometheus.GaugeValue, s.Rate1(), broker)
				}
			case 'g':
				if v, ok := value.(metrics.Counter); ok {
					ch <- prometheus.MustNewConstMetric(m.desc, prometheus.GaugeValue, float64(v.Snapshot().Count()), broker)
				}
			case 'h':
				if v, ok := value.(metrics.Histogram); ok {
					s := v.Snapshot()
					if s.Count() == 0 {
						break
					}
					p := s.Percentiles([]float64{.5, .95, .99})
					values := [...]float64{s.Mean(), p[0], p[1], p[2], float64(s.Max())}
					for i, statistic := range []string{"mean", "p50", "p95", "p99", "max"} {
						ch <- prometheus.MustNewConstMetric(m.desc, prometheus.GaugeValue, values[i]*m.scale, broker, statistic)
					}
				}
			}
			break
		}
	})
	d.errors.Collect(ch)
	d.drops.Collect(ch)
	if d.successes != nil {
		d.successes.Collect(ch)
	}
}

func producerErrorCode(err error) string {
	var code sarama.KError
	if errors.As(err, &code) && code >= sarama.ErrUnknown && code <= sarama.ErrProducerFenced {
		// Sarama 1.38.1 defines contiguous codes -1 through 90. Unknown future
		// codes share "other", bounding this family to 93 possible labels.
		return strconv.FormatInt(int64(code), 10)
	}
	return "other"
}

// AsyncClose must be used with this sole channel consumer: Sarama.Close would
// steal errors and successes from the exact diagnostics counters.
func (d *kafkaDiagnostics) drain(producer sarama.AsyncProducer, forward chan<- error) {
	defer close(d.done)
	errorsCh := producer.Errors()
	var successes <-chan *sarama.ProducerMessage
	if d.successes != nil {
		successes = producer.Successes()
	}
	for errorsCh != nil || successes != nil {
		select {
		case msg, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if msg == nil {
				continue
			}
			d.errors.WithLabelValues(producerErrorCode(msg.Err)).Inc()
			if d.closing.Load() {
				d.closeErrors = append(d.closeErrors, msg)
			}
			select {
			case forward <- &KafkaTransportError{msg}:
			default:
				d.drops.Inc()
			}
		case msg, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			if msg != nil {
				d.successes.Inc()
			}
		}
	}
}
