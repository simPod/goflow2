package diagnostics

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestDiagnosticsSamplingAndAggregation(t *testing.T) {
	r := NewRecorder("sflow://:9801", 4, 1)
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(r)
	for i := 0; i < 10; i++ {
		trace := r.Begin(0, time.Now().Add(-time.Second))
		if (trace != nil) != (i%4 == 0) {
			t.Fatalf("packet %d sampling mismatch", i)
		}
		trace.SetStage(Format)
		trace.SetStage(KafkaEnqueue)
		trace.SetStage(Format)
		trace.SetStage(KafkaEnqueue)
		trace.Finish(errors.New("send failed"), false)
		r.Idle(0)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "goflow_diagnostics_stage_seconds" {
			continue
		}
		for _, m := range f.Metric {
			if m.Histogram.GetSampleCount() != 3 {
				t.Fatalf("want 3 datagrams per stage, got %v", m)
			}
			for _, l := range m.Label {
				if l.GetName() == "stage" && l.GetValue() == "queue_wait" && m.Histogram.GetSampleSum() < 3 {
					t.Fatal("queue wait missing receive time")
				}
			}
		}
	}
	metric := &dto.Metric{}
	if err := r.errors.WithLabelValues("error").Write(metric); err != nil {
		t.Fatal(err)
	}
	if got := metric.GetCounter().GetValue(); got != 3 {
		t.Fatalf("errors = %v", got)
	}
	if got := NewRecorder("default", 0, 1).every; got != 1024 {
		t.Fatalf("default = %d", got)
	}
}

func TestDiagnosticsConcurrentScrape(t *testing.T) {
	r := NewRecorder("test", 16, 2)
	r.BindReceiver(2, 2, func() (int, int) { return 1, 4 })
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(r)
	var wg sync.WaitGroup
	for id := 0; id < 2; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				r.Socket(id).Received(64)
				r.Socket(id).Dropped(64)
				trace := r.Begin(id, time.Time{})
				trace.SetStage(Decode)
				trace.Finish(nil, false)
				r.Idle(id)
			}
			r.Socket(id).Error()
		}()
	}
	for i := 0; i < 30; i++ {
		if _, err := registry.Gather(); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	assertDropTotals(t, registry, 2000, 128000)
	for id := 0; id < 2; id++ {
		s := r.Socket(id)
		if s.datagrams.Load() != 1000 || s.bytes.Load() != 64000 || s.errors.Load() != 1 {
			t.Fatal("socket snapshot mismatch")
		}
	}
}

func assertDropTotals(t *testing.T, registry *prometheus.Registry, datagrams, bytes float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"goflow_diagnostics_dropped_datagrams_total": datagrams,
		"goflow_diagnostics_dropped_bytes_total":     bytes,
	}
	for _, f := range families {
		value, ok := want[f.GetName()]
		if !ok {
			continue
		}
		if f.GetType() != dto.MetricType_COUNTER || len(f.Metric) != 1 {
			t.Fatalf("expected one listener counter: %v", f)
		}
		m := f.Metric[0]
		if len(m.Label) != 1 || m.Label[0].GetName() != "listener" || m.Label[0].GetValue() != "test" {
			t.Fatalf("unexpected drop labels: %v", m.Label)
		}
		if got := m.Counter.GetValue(); got != value {
			t.Errorf("%s = %v, want %v", f.GetName(), got, value)
		}
		delete(want, f.GetName())
	}
	if len(want) != 0 {
		t.Fatalf("missing drop counters: %v", want)
	}
}

func TestDiagnosticsDropTotals(t *testing.T) {
	r := NewRecorder("test", 1024, 1)
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(r)
	assertDropTotals(t, registry, 0, 0)
	r = NewRecorder("test", 1024, 1)
	r.BindReceiver(1, 3, nil)
	registry = prometheus.NewPedanticRegistry()
	registry.MustRegister(r)
	assertDropTotals(t, registry, 0, 0)
	r.Socket(0).Received(100)
	r.Socket(1).Received(200)
	assertDropTotals(t, registry, 0, 0)
	r.Socket(0).Dropped(7)
	r.Socket(0).Dropped(11)
	r.Socket(1).Dropped(23)
	assertDropTotals(t, registry, 3, 41)
	var disabled *Socket
	disabled.Dropped(99)
}

func BenchmarkDiagnosticsDropped(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		var socket *Socket
		if enabled {
			name = "enabled"
			socket = &Socket{}
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				socket.Dropped(1400)
			}
		})
	}
}

func BenchmarkDiagnostics(b *testing.B) {
	for _, tc := range []struct {
		name  string
		every uint64
	}{{"disabled", 0}, {"sample1024", 1024}, {"sample1", 1}} {
		b.Run(tc.name, func(b *testing.B) {
			var r *Recorder
			if tc.every != 0 {
				r = NewRecorder("benchmark", tc.every, 1)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				trace := r.Begin(0, time.Time{})
				trace.SetStage(Decode)
				trace.SetStage(Produce)
				for j := 0; j < 20; j++ {
					if trace != nil {
						trace.SetStage(Format)
						trace.SetStage(KafkaEnqueue)
					}
				}
				trace.Finish(nil, false)
				r.Idle(0)
			}
		})
	}
}
