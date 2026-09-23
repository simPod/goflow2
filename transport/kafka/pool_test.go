package kafka

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	sarama "github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metrics "github.com/rcrowley/go-metrics"
)

func poolDriver(t *testing.T, count int, diagnostics bool) *KafkaDriver {
	t.Helper()
	old := flag.CommandLine
	flag.CommandLine = flag.NewFlagSet("pool", flag.ContinueOnError)
	defer func() { flag.CommandLine = old }()
	d := &KafkaDriver{errors: make(chan error, 100)}
	if err := d.Prepare(); err != nil {
		t.Fatal(err)
	}
	if d.kafkaProducers != 1 {
		t.Fatal("default producer count must be one")
	}
	if err := flag.CommandLine.Set("transport.kafka.producers", fmt.Sprint(count)); err != nil {
		t.Fatal(err)
	}
	d.kafkaDiagnostics = diagnostics
	return d
}

func poolRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	old := prometheus.DefaultRegisterer
	r := prometheus.NewPedanticRegistry()
	prometheus.DefaultRegisterer = r
	t.Cleanup(func() { prometheus.DefaultRegisterer = old })
	return r
}

// Snapshot the actual exported collectors after their drainers finish, before
// Close unregisters them. No production hooks or access to Sarama internals.
type unregisterSnapshots struct {
	prometheus.Registerer
	families map[string]*dto.MetricFamily
	err      error
}

func (s *unregisterSnapshots) Unregister(c prometheus.Collector) bool {
	r := prometheus.NewPedanticRegistry()
	if err := r.Register(c); err != nil {
		s.err = errors.Join(s.err, err)
	} else {
		families, err := r.Gather()
		s.err = errors.Join(s.err, err)
		for _, family := range families {
			if previous := s.families[family.GetName()]; previous != nil {
				previous.Metric = append(previous.Metric, family.Metric...)
			} else {
				s.families[family.GetName()] = family
			}
		}
	}
	return s.Registerer.Unregister(c)
}

func snapshotPoolRegistry(t *testing.T) (*prometheus.Registry, *unregisterSnapshots) {
	t.Helper()
	r := poolRegistry(t)
	s := &unregisterSnapshots{Registerer: r, families: make(map[string]*dto.MetricFamily)}
	prometheus.DefaultRegisterer = s
	return r, s
}

func poolFake() *diagnosticProducer {
	return &diagnosticProducer{
		input:     make(chan *sarama.ProducerMessage, 10000),
		errors:    make(chan *sarama.ProducerError),
		successes: make(chan *sarama.ProducerMessage),
	}
}

func TestProducerCountValidation(t *testing.T) {
	for _, count := range []int{0, -1} {
		d := poolDriver(t, count, false)
		if err := d.initProducers(func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			t.Fatal("factory called for invalid count")
			return nil, nil
		}); err == nil {
			t.Fatal("invalid count accepted")
		}
	}
}

func TestPoolConcurrentRoundRobin(t *testing.T) {
	for _, count := range []int{1, 2, 4} {
		for _, diagnostics := range []bool{false, true} {
			t.Run(fmt.Sprintf("producers=%d/diagnostics=%t", count, diagnostics), func(t *testing.T) {
				r, snapshots := snapshotPoolRegistry(t)
				d := poolDriver(t, count, diagnostics)
				d.kafkaDiagnosticSuccesses = true
				var producers []*diagnosticProducer
				if err := d.initProducers(func(_ []string, config *sarama.Config) (sarama.AsyncProducer, error) {
					if config.Producer.Return.Successes != diagnostics {
						t.Fatal("unexpected success notification setting")
					}
					p := poolFake()
					producers = append(producers, p)
					return p, nil
				}); err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				var wg sync.WaitGroup
				for worker := 0; worker < 8; worker++ {
					wg.Add(1)
					go func(worker int) {
						defer wg.Done()
						for i := 0; i < 100; i++ {
							if err := d.Send([]byte("same-key"), []byte(fmt.Sprintf("%d/%d", worker, i))); err != nil {
								t.Error(err)
							}
						}
					}(worker)
				}
				wg.Wait()
				seen := make(map[string]bool)
				for _, p := range producers {
					if len(p.input) != 800/count {
						t.Fatal("unbalanced producer selection")
					}
					for len(p.input) > 0 {
						msg := <-p.input
						value, _ := msg.Value.Encode()
						if seen[string(value)] || msg.Topic != d.kafkaTopic {
							t.Fatal("duplicate message or wrong topic")
						}
						seen[string(value)] = true
						if diagnostics {
							select {
							case p.successes <- msg:
							case <-time.After(5 * time.Second):
								t.Fatal("success drainer blocked")
							}
						}
					}
				}
				if len(seen) != 800 {
					t.Fatal("lost messages")
				}
				if count == 1 && d.nextProducer.Load() != 0 {
					t.Fatal("single producer used atomic chooser")
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				if snapshots.err != nil {
					t.Fatal(snapshots.err)
				}
				if diagnostics {
					for i := range producers {
						got := metricValue(t, snapshots.families, "goflow2_kafka_producer_successes_total", map[string]string{"producer": fmt.Sprint(i)})
						if got != float64(800/count) {
							t.Fatalf("producer %d: got %v successes", i, got)
						}
					}
				} else if len(snapshots.families) != 0 {
					t.Fatal("diagnostics exported while disabled")
				}
				if len(gatherDiagnostics(t, r)) != 0 {
					t.Fatal("collectors leaked")
				}
			})
		}
	}
}

func TestPoolKeyAffinity(t *testing.T) {
	d := poolDriver(t, 4, false)
	d.kafkaHashing = true
	var producers []*diagnosticProducer
	if err := d.initProducers(func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
		p := poolFake()
		producers = append(producers, p)
		return p, nil
	}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	keys := []string{"alpha", "beta", "gamma"}
	expected := make(map[string]string)
	for worker := 0; worker < 8; worker++ {
		for i := 0; i < 100; i++ {
			for _, key := range keys {
				expected[fmt.Sprintf("%d/%d/%s", worker, i, key)] = key
			}
		}
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				for _, key := range keys {
					if err := d.Send([]byte(key), []byte(fmt.Sprintf("%d/%d/%s", worker, i, key))); err != nil {
						t.Error(err)
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	owners := make(map[string]int)
	counts := make(map[string]int)
	for i, p := range producers {
		for len(p.input) > 0 {
			msg := <-p.input
			key, _ := msg.Key.Encode()
			payload, _ := msg.Value.Encode()
			if want, ok := expected[string(payload)]; !ok || want != string(key) || msg.Topic != d.kafkaTopic {
				t.Fatal("duplicate, unexpected, or incorrectly keyed message")
			}
			delete(expected, string(payload))
			counts[string(key)]++
			if owner, ok := owners[string(key)]; ok && owner != i {
				t.Fatal("key moved between producers")
			}
			owners[string(key)] = i
		}
	}
	if len(expected) != 0 {
		t.Fatalf("lost %d messages", len(expected))
	}
	for _, key := range keys {
		if counts[key] != 800 {
			t.Fatalf("key %s: got %d messages", key, counts[key])
		}
	}
	if len(owners) != 3 || d.nextProducer.Load() != 0 {
		t.Fatal("key affinity used round robin")
	}
	for i := 0; i < 8; i++ {
		key := []byte{}
		if i%2 == 0 {
			key = nil
		}
		if err := d.Send(key, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range producers {
		if len(p.input) != 2 {
			t.Fatal("empty keys did not use round robin")
		}
	}
}

func TestPoolIndependentConfigAndReinit(t *testing.T) {
	r := poolRegistry(t)
	d := poolDriver(t, 2, true)
	d.kafkaCompressionCodec = "gzip"
	d.kafkaTLS = true
	d.kafkaTlsInsecure = true
	var configs []*sarama.Config
	factory := func(_ []string, c *sarama.Config) (sarama.AsyncProducer, error) {
		configs = append(configs, c)
		return poolFake(), nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := d.initProducers(factory); err != nil {
			t.Fatal(err)
		}
		if err := d.initProducers(factory); err == nil {
			t.Fatal("double init accepted")
		}
		f := gatherDiagnostics(t, r)
		if metricValue(t, f, "goflow2_kafka_producers", nil) != 2 {
			t.Fatal("pool size")
		}
		for i := 0; i < 2; i++ {
			metricValue(t, f, "goflow2_kafka_producer_input_queue_capacity", map[string]string{"producer": fmt.Sprint(i)})
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		if len(gatherDiagnostics(t, r)) != 0 {
			t.Fatal("metrics leaked")
		}
		if err := d.Send(nil, nil); err == nil {
			t.Fatal("send after close accepted")
		}
	}
	for i, c := range configs {
		if c.Producer.Return.Successes || !c.Producer.Return.Errors || c.Producer.RequiredAcks != sarama.WaitForLocal || c.Producer.Compression != sarama.CompressionGZIP || c.Version != sarama.V2_8_0_0 || c.Producer.Flush.Bytes != d.kafkaFlushBytes || c.Producer.Flush.Frequency != d.kafkaFlushFrequency || c.Producer.MaxMessageBytes != d.kafkaMaxMsgBytes || !c.Net.TLS.Enable || !c.Net.TLS.Config.InsecureSkipVerify {
			t.Fatal("producer settings changed")
		}
		if c.ClientID != fmt.Sprintf("sarama-%d", i%2) {
			t.Fatal(c.ClientID)
		}
		partitioner := c.Producer.Partitioner("topic")
		for want := int32(0); want < 3; want++ {
			got, err := partitioner.Partition(&sarama.ProducerMessage{}, 3)
			if err != nil || got != want {
				t.Fatal("round-robin partitioner changed")
			}
		}
		for _, other := range configs[:i] {
			if c == other || c.MetricRegistry == other.MetricRegistry || c.Net.TLS.Config == other.Net.TLS.Config {
				t.Fatal("shared configuration")
			}
		}
	}
}

// Closing waits for all members to receive AsyncClose, then emits unbuffered
// results. This catches serial flushing and missing error/success drainers.
type flushProducer struct {
	*diagnosticProducer
	started     chan<- struct{}
	release     <-chan struct{}
	withSuccess bool
}

func (p *flushProducer) AsyncClose() {
	p.started <- struct{}{}
	go func() {
		<-p.release
		p.errors <- &sarama.ProducerError{Err: sarama.ErrRequestTimedOut}
		if p.withSuccess {
			p.successes <- &sarama.ProducerMessage{}
		}
		p.diagnosticProducer.AsyncClose()
	}()
}

func TestPoolParallelCloseAccounting(t *testing.T) {
	for _, diagnostics := range []bool{false, true} {
		t.Run(fmt.Sprint(diagnostics), func(t *testing.T) {
			r := poolRegistry(t)
			d := poolDriver(t, 2, diagnostics)
			d.kafkaDiagnosticSuccesses = true
			started := make(chan struct{}, 2)
			release := make(chan struct{})
			if err := d.initProducers(func(_ []string, c *sarama.Config) (sarama.AsyncProducer, error) {
				return &flushProducer{poolFake(), started, release, c.Producer.Return.Successes}, nil
			}); err != nil {
				t.Fatal(err)
			}
			members := append([]producerMember(nil), d.producers...)
			closed := make(chan error, 1)
			go func() { closed <- d.Close() }()
			for range members {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("serial flush")
				}
			}
			close(release)
			select {
			case err := <-closed:
				var errs sarama.ProducerErrors
				if !errors.As(err, &errs) || len(errs) != 2 {
					t.Fatalf("close errors: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("result drainer deadlock")
			}
			if len(d.errors) != 2 {
				t.Fatal("errors lost or close sentinel forwarded")
			}
			if len(gatherDiagnostics(t, r)) != 0 {
				t.Fatal("collector leaked")
			}
			if diagnostics {
				for i, member := range members {
					prometheus.WrapRegistererWith(prometheus.Labels{"producer": fmt.Sprint(i)}, r).MustRegister(member.diagnostics)
				}
				f := gatherDiagnostics(t, r)
				for i := range members {
					if metricValue(t, f, "goflow2_kafka_producer_errors_total", map[string]string{"producer": fmt.Sprint(i), "code": "7"}) != 1 {
						t.Fatal("error count")
					}
					if metricValue(t, f, "goflow2_kafka_producer_successes_total", map[string]string{"producer": fmt.Sprint(i)}) != 1 {
						t.Fatal("success count")
					}
				}
			}
		})
	}
}

func TestPoolPartialInitializationCleanup(t *testing.T) {
	for _, diagnostics := range []bool{false, true} {
		t.Run(fmt.Sprint(diagnostics), func(t *testing.T) {
			r := poolRegistry(t)
			d := poolDriver(t, 3, diagnostics)
			d.kafkaDiagnosticSuccesses = true
			var configs []*sarama.Config
			var producers []*diagnosticProducer
			failure := errors.New("connection failed")
			err := d.initProducers(func(_ []string, c *sarama.Config) (sarama.AsyncProducer, error) {
				configs = append(configs, c)
				metrics.GetOrRegisterMeter("test", c.MetricRegistry).Mark(1)
				if len(configs) == 3 {
					return nil, failure
				}
				p := poolFake()
				producers = append(producers, p)
				return p, nil
			})
			if !errors.Is(err, failure) || !strings.Contains(err.Error(), "producer 2") {
				t.Fatal(err)
			}
			for _, p := range producers {
				if _, ok := <-p.errors; ok {
					t.Fatal("producer not closed")
				}
			}
			for _, c := range configs {
				c.MetricRegistry.Each(func(string, interface{}) { t.Error("registry leaked") })
			}
			if len(d.producers) != 0 || len(gatherDiagnostics(t, r)) != 0 {
				t.Fatal("pool leaked")
			}
			if err := d.initProducers(func([]string, *sarama.Config) (sarama.AsyncProducer, error) { return poolFake(), nil }); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPoolCollectorFailureCleanup(t *testing.T) {
	for _, failPoolSize := range []bool{false, true} {
		t.Run(fmt.Sprint(failPoolSize), func(t *testing.T) {
			r := poolRegistry(t)
			d := poolDriver(t, 2, true)
			d.kafkaDiagnosticSuccesses = true
			if failPoolSize {
				r.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{Name: "goflow2_kafka_producers", Help: "Number of independent Kafka producer instances."}))
			} else {
				duplicate := newKafkaDiagnostics(metrics.NewRegistry(), make(chan *sarama.ProducerMessage), true)
				if err := duplicate.registerProducer(1); err != nil {
					t.Fatal(err)
				}
			}
			before := gatherDiagnostics(t, r)
			var producers []*diagnosticProducer
			var configs []*sarama.Config
			if err := d.initProducers(func(_ []string, c *sarama.Config) (sarama.AsyncProducer, error) {
				p := poolFake()
				producers = append(producers, p)
				configs = append(configs, c)
				return p, nil
			}); err == nil {
				t.Fatal("duplicate collector accepted")
			}
			for _, p := range producers {
				if _, ok := <-p.errors; ok {
					t.Fatal("producer leaked")
				}
				if _, ok := <-p.successes; ok {
					t.Fatal("success drainer leaked")
				}
			}
			for _, c := range configs {
				c.MetricRegistry.Each(func(string, interface{}) { t.Error("registry leaked") })
			}
			after := gatherDiagnostics(t, r)
			if len(before) != len(after) {
				t.Fatal("existing collectors removed or pool collectors leaked")
			}
			for name, family := range before {
				if len(after[name].GetMetric()) != len(family.Metric) {
					t.Fatal("existing collector changed")
				}
			}
		})
	}
}

func TestSingleProducerDefaultsAndLabels(t *testing.T) {
	r := poolRegistry(t)
	d := poolDriver(t, 1, true)
	if err := d.initProducers(func(_ []string, c *sarama.Config) (sarama.AsyncProducer, error) {
		if c.ClientID != sarama.NewConfig().ClientID || c.Producer.Return.Successes {
			t.Fatal("single producer defaults changed")
		}
		return poolFake(), nil
	}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	f := gatherDiagnostics(t, r)
	if metricValue(t, f, "goflow2_kafka_producers", nil) != 1 {
		t.Fatal("pool size")
	}
	metricValue(t, f, "goflow2_kafka_producer_input_queue_length", map[string]string{"producer": "0"})
}

func TestPoolDrainersDoNotForwardCloseSentinels(t *testing.T) {
	d := poolDriver(t, 2, false)
	var producers []*diagnosticProducer
	if err := d.initProducers(func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
		p := poolFake()
		producers = append(producers, p)
		return p, nil
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate one member's result channels closing before the other member.
	producers[0].AsyncClose()
	<-d.producers[0].diagnostics.done
	producers[1].errors <- &sarama.ProducerError{Err: sarama.ErrRequestTimedOut}
	select {
	case err := <-d.Errors():
		if err == nil {
			t.Fatal("premature nil forwarding")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second drainer stopped")
	}
	producers[1].AsyncClose()
	<-d.producers[1].diagnostics.done
	if len(d.errors) != 0 {
		t.Fatal("forwarded close sentinel")
	}
}
