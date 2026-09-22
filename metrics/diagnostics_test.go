package metrics

import (
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v2/decoders/netflow"
	"github.com/netsampler/goflow2/v2/diagnostics"
	"github.com/netsampler/goflow2/v2/producer"
	"github.com/netsampler/goflow2/v2/utils"
	"github.com/netsampler/goflow2/v2/utils/debug"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type diagnosticProducer struct {
	call    func()
	err     error
	commits int
}

func (p *diagnosticProducer) Produce(interface{}, *producer.ProduceArgs) ([]producer.ProducerMessage, error) {
	if p.call != nil {
		p.call()
	}
	return []producer.ProducerMessage{1, 2}, p.err
}
func (p *diagnosticProducer) Commit([]producer.ProducerMessage) { p.commits++ }
func (p *diagnosticProducer) Close()                            {}

type diagnosticFormat struct {
	call func()
	err  error
}

func (f diagnosticFormat) Format(interface{}) ([]byte, []byte, error) {
	if f.call != nil {
		f.call()
	}
	return nil, []byte("flow"), f.err
}

type diagnosticTransport struct {
	call  func()
	err   error
	sends int
}

func (s *diagnosticTransport) Send([]byte, []byte) error {
	s.sends++
	if s.call != nil {
		s.call()
	}
	return s.err
}

func diagnosticMetric(t *testing.T, reg *prometheus.Registry, name, label, value string) *dto.Metric {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "goflow_diagnostics_"+name {
			continue
		}
		for _, m := range f.Metric {
			for _, l := range m.Label {
				if l.GetName() == label && l.GetValue() == value {
					return m
				}
			}
		}
	}
	return &dto.Metric{}
}

func diagnosticPacket() *utils.Message {
	payload := make([]byte, 24)
	payload[1] = 5
	return &utils.Message{Payload: payload, Src: netip.MustParseAddrPort("127.0.0.1:1234"), Dst: netip.MustParseAddrPort("127.0.0.1:9801"), Received: time.Now()}
}

func TestDiagnosticsPipelineBoundaries(t *testing.T) {
	for _, stage := range []string{"decode", "produce", "format", "kafka_enqueue"} {
		t.Run(stage, func(t *testing.T) {
			r := diagnostics.NewRecorder("netflow://:9801", 1, 1)
			reg := prometheus.NewPedanticRegistry()
			reg.MustRegister(r)
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			block := func() { once.Do(func() { close(entered); <-release }) }
			prod := &diagnosticProducer{}
			formatter := diagnosticFormat{}
			transport := &diagnosticTransport{}
			cfg := &utils.PipeConfig{Producer: WrapPromProducer(prod), Transport: transport}
			switch stage {
			case "decode":
				cfg.NetFlowTemplater = func(string) netflow.NetFlowTemplateSystem { block(); return netflow.CreateTemplateSystem() }
			case "produce":
				prod.call = block
			case "format":
				formatter.call = block
			case "kafka_enqueue":
				transport.call = block
			}
			cfg.Format = formatter
			pipe := utils.NewNetFlowPipe(cfg)
			decode := PromDecoderWrapper(pipe.DecodeFlow, "netflow")
			go func() {
				defer close(done)
				msg := diagnosticPacket()
				trace := r.Begin(0, msg.Received)
				msg.Diagnostics = trace
				err := decode(msg)
				trace.Finish(err, false)
				r.Idle(0)
				if err != nil {
					t.Error(err)
				}
			}()
			released := false
			defer func() {
				if !released {
					close(release)
				}
				<-done
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("stage not entered")
			}
			if v := diagnosticMetric(t, reg, "workers", "state", "pipeline").GetGauge().GetValue(); v != 1 {
				t.Fatalf("busy = %v", v)
			}
			if v := diagnosticMetric(t, reg, "workers", "state", "sampled_"+stage).GetGauge().GetValue(); v != 1 {
				t.Fatalf("active %s = %v", stage, v)
			}
			for _, other := range []string{"decode", "produce", "producer_metrics", "format", "kafka_enqueue", "wrapper_metrics"} {
				if other != stage && diagnosticMetric(t, reg, "workers", "state", "sampled_"+other).GetGauge().GetValue() != 0 {
					t.Fatalf("incorrect stage %s", other)
				}
			}
			close(release)
			released = true
			<-done
			if prod.commits != 1 || transport.sends != 2 {
				t.Fatalf("commit/send semantics changed: %d/%d", prod.commits, transport.sends)
			}
			var sum float64
			for _, phase := range []string{"pipeline", "decode", "produce", "producer_metrics", "format", "kafka_enqueue", "wrapper_metrics"} {
				h := diagnosticMetric(t, reg, "stage_seconds", "stage", phase).GetHistogram()
				if h.GetSampleCount() != 1 {
					t.Fatalf("%s should count one datagram, got %d", phase, h.GetSampleCount())
				}
				sum += h.GetSampleSum()
			}
			total := diagnosticMetric(t, reg, "stage_seconds", "stage", "handling").GetHistogram().GetSampleSum()
			if delta := total - sum; delta < -1e-9 || delta > 1e-9 {
				t.Fatalf("phases overlap or omit time: sum %g total %g", sum, total)
			}
		})
	}
}

func TestDiagnosticsPipelineErrorsAndPanics(t *testing.T) {
	for _, stage := range []string{"decode", "produce", "format", "kafka_enqueue"} {
		for _, panicked := range []bool{false, true} {
			name := stage + "_error"
			if panicked {
				name = stage + "_panic"
			}
			t.Run(name, func(t *testing.T) {
				r := diagnostics.NewRecorder("test", 1, 1)
				reg := prometheus.NewPedanticRegistry()
				reg.MustRegister(r)
				failure := errors.New("test failure")
				var call func()
				if panicked {
					call = func() { panic(failure) }
				}
				prod, format, transport := &diagnosticProducer{}, diagnosticFormat{}, &diagnosticTransport{}
				cfg := &utils.PipeConfig{Producer: WrapPromProducer(prod), Transport: transport}
				msg := diagnosticPacket()
				switch stage {
				case "decode":
					if panicked {
						cfg.NetFlowTemplater = func(string) netflow.NetFlowTemplateSystem { panic(failure) }
					} else {
						msg.Payload = nil
					}
				case "produce":
					prod.call, prod.err = call, failure
				case "format":
					format.call, format.err = call, failure
				case "kafka_enqueue":
					transport.call, transport.err = call, failure
				}
				cfg.Format = format
				decode := PromDecoderWrapper(utils.NewNetFlowPipe(cfg).DecodeFlow, "netflow")
				trace := r.Begin(0, msg.Received)
				msg.Diagnostics = trace
				var err error
				func() {
					defer func() {
						p := recover()
						if (p != nil) != panicked {
							t.Errorf("panic mismatch: %v", p)
						}
						trace.Finish(err, p != nil)
						r.Idle(0)
					}()
					err = decode(msg)
				}()
				if !panicked && err == nil {
					t.Fatal("error swallowed")
				}
				outcome := "error"
				if panicked {
					outcome = "panic"
				}
				if diagnosticMetric(t, reg, "sample_errors_total", "outcome", outcome).GetCounter().GetValue() != 1 {
					t.Fatal("missing outcome")
				}
				if diagnosticMetric(t, reg, "stage_seconds", "stage", stage).GetHistogram().GetSampleCount() != 1 {
					t.Fatal("failed stage not counted")
				}
			})
		}
	}
}

func TestDiagnosticsRecoveredPanics(t *testing.T) {
	for _, source := range []string{"decoder", "producer"} {
		t.Run(source, func(t *testing.T) {
			r := diagnostics.NewRecorder("test", 1, 1)
			reg := prometheus.NewPedanticRegistry()
			reg.MustRegister(r)
			failure := errors.New("ordinary error")
			prod := &diagnosticProducer{}
			cfg := &utils.PipeConfig{Producer: WrapPromProducer(debug.WrapPanicProducer(prod))}
			stage := "produce"
			if source == "decoder" {
				stage = "decode"
				cfg.NetFlowTemplater = func(string) netflow.NetFlowTemplateSystem { panic("decoder panic") }
			} else {
				prod.call = func() { panic("producer panic") }
			}
			decode := PromDecoderWrapper(debug.PanicDecoderWrapper(utils.NewNetFlowPipe(cfg).DecodeFlow), "netflow")
			msg := diagnosticPacket()
			trace := r.Begin(0, msg.Received)
			msg.Diagnostics = trace
			err := decode(msg) // The real debug wrappers recover; this call returns normally.
			if !errors.Is(err, debug.ErrPanic) {
				t.Fatalf("want recovered panic, got %v", err)
			}
			if source == "producer" {
				var pipeErr *utils.PipeMessageError
				if !errors.As(err, &pipeErr) {
					t.Fatal("producer panic must pass through the pipeline error wrapper")
				}
			}
			trace.Finish(err, false) // Matches the worker's completed=true path.
			r.Idle(0)
			if diagnosticMetric(t, reg, "sample_errors_total", "outcome", "panic").GetCounter().GetValue() != 1 {
				t.Fatal("recovered panic not counted")
			}
			if diagnosticMetric(t, reg, "sample_errors_total", "outcome", "error").GetCounter().GetValue() != 0 {
				t.Fatal("recovered panic also counted as an ordinary error")
			}
			if diagnosticMetric(t, reg, "stage_seconds", "stage", stage).GetHistogram().GetSampleCount() != 1 {
				t.Fatal("panic stage observation missing")
			}

			// Reuse the worker slot: the recovered-panic marker must not leak into
			// the next datagram, and an ordinary error must keep its classification.
			prod.call, prod.err = nil, failure
			cfg.NetFlowTemplater = nil
			decode = PromDecoderWrapper(debug.PanicDecoderWrapper(utils.NewNetFlowPipe(cfg).DecodeFlow), "netflow")
			msg = diagnosticPacket()
			trace = r.Begin(0, msg.Received)
			msg.Diagnostics = trace
			err = decode(msg)
			if !errors.Is(err, failure) {
				t.Fatalf("ordinary error changed: %v", err)
			}
			trace.Finish(err, false)
			r.Idle(0)
			if diagnosticMetric(t, reg, "sample_errors_total", "outcome", "panic").GetCounter().GetValue() != 1 ||
				diagnosticMetric(t, reg, "sample_errors_total", "outcome", "error").GetCounter().GetValue() != 1 {
				t.Fatal("panic marker leaked into the next datagram")
			}
			// Disabled/unsampled traces leave the original returned error intact.
			msg.Diagnostics = nil
			if err := decode(msg); !errors.Is(err, failure) {
				t.Fatalf("nil trace changed error: %v", err)
			}
		})
	}
}
