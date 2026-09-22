package utils

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v2/diagnostics"
	"github.com/prometheus/client_golang/prometheus"
)

func diagnosticValue(t *testing.T, registry *prometheus.Registry, name, label, value string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "goflow_diagnostics_"+name {
			continue
		}
		for _, m := range f.Metric {
			match := label == ""
			for _, l := range m.Label {
				if l.GetName() == label && l.GetValue() == value {
					match = true
				}
			}
			if match {
				if m.Gauge != nil {
					return m.Gauge.GetValue()
				}
				if m.Counter != nil {
					return m.Counter.GetValue()
				}
				if m.Histogram != nil {
					return float64(m.Histogram.GetSampleCount())
				}
			}
		}
	}
	return 0
}

func TestDiagnosticsWorkerCleanup(t *testing.T) {
	for _, outcome := range []string{"error", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			recorder := diagnostics.NewRecorder("test", 1, 1)
			r, err := NewUDPReceiver(&UDPReceiverConfig{Workers: 1, Sockets: 1, Diagnostics: recorder})
			if err != nil {
				t.Fatal(err)
			}
			registry := prometheus.NewPedanticRegistry()
			registry.MustRegister(recorder)
			msg := &Message{Received: time.Now()}
			func() {
				defer func() {
					if p := recover(); (p != nil) != (outcome == "panic") {
						t.Errorf("unexpected panic: %v", p)
					}
				}()
				err = r.decodeWithDiagnostics(0, msg, func(interface{}) error {
					if outcome == "panic" {
						panic("test")
					}
					return errors.New("test")
				})
			}()
			if outcome == "error" && err == nil {
				t.Fatal("error swallowed")
			}
			if msg.Diagnostics != nil {
				t.Fatal("trace escaped handler")
			}
			if diagnosticValue(t, registry, "workers", "state", "waiting_for_packet") != 1 {
				t.Fatal("worker not idle")
			}
			if diagnosticValue(t, registry, "sample_errors_total", "outcome", outcome) != 1 {
				t.Fatal("outcome missing")
			}
			if diagnosticValue(t, registry, "stage_seconds", "stage", "handling") != 1 {
				t.Fatal("sample missing")
			}
		})
	}
}

// No decoder runs: received counters must still advance and queue length must be
// measured from the channel. Uses only a local ephemeral UDP socket.
func TestDiagnosticsReceiveBeforeDecode(t *testing.T) {
	recorder := diagnostics.NewRecorder("sflow://:9801", 1024, 1)
	r, err := NewUDPReceiver(&UDPReceiverConfig{Workers: 1, Sockets: 1, QueueSize: 2, Blocking: true, Diagnostics: recorder})
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(recorder)
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	done := make(chan error, 1)
	go func() { done <- r.receiveRoutine(conn, recorder.Socket(0)) }()
	defer func() {
		conn.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("receiver did not stop")
		}
	}()
	sender, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for diagnosticValue(t, registry, "queue_length", "", "") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("packet did not reach queue")
		}
		time.Sleep(time.Millisecond)
	}
	if diagnosticValue(t, registry, "queue_capacity", "", "") != 2 {
		t.Fatal("capacity mismatch")
	}
	if diagnosticValue(t, registry, "socket_datagrams_total", "socket", "0") != 1 {
		t.Fatal("receive counter missing")
	}
	if diagnosticValue(t, registry, "socket_bytes_total", "socket", "0") != 3 {
		t.Fatal("byte counter missing")
	}
	if diagnosticValue(t, registry, "stage_seconds", "stage", "handling") != 0 {
		t.Fatal("decoded unexpectedly")
	}
	packetPool.Put(<-r.dispatch)
	if diagnosticValue(t, registry, "queue_length", "", "") != 0 {
		t.Fatal("queue gauge is stale")
	}
}

func TestDiagnosticsUnsampledStall(t *testing.T) {
	recorder := diagnostics.NewRecorder("test", 1024, 1)
	r, _ := NewUDPReceiver(&UDPReceiverConfig{Workers: 1, Sockets: 1, Diagnostics: recorder})
	registry := prometheus.NewRegistry()
	registry.MustRegister(recorder)
	_ = r.decodeWithDiagnostics(0, &Message{}, func(interface{}) error { return nil })
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_ = r.decodeWithDiagnostics(0, &Message{}, func(msg interface{}) error {
			if msg.(*Message).Diagnostics != nil {
				t.Error("second packet should be unsampled")
			}
			close(entered)
			<-release
			return nil
		})
	}()
	defer func() { close(release); <-done }()
	<-entered
	if diagnosticValue(t, registry, "workers", "state", "pipeline") != 1 {
		t.Fatal("unsampled stall not busy")
	}
	if diagnosticValue(t, registry, "workers", "state", "sampled_pipeline") != 0 {
		t.Fatal("unsampled stall reported sampled")
	}
}
