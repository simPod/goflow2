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
	for id := 0; id < 2; id++ {
		s := r.Socket(id)
		if s.datagrams.Load() != 1000 || s.bytes.Load() != 64000 || s.errors.Load() != 1 {
			t.Fatal("socket snapshot mismatch")
		}
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
