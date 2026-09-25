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

type diagnosticsDropCallback func(Message)

func (f diagnosticsDropCallback) Dropped(msg Message) { f(msg) }

func TestDiagnosticsDropsSurviveSessionReset(t *testing.T) {
	recorder := diagnostics.NewRecorder("test", 1024, 1)
	r, err := NewUDPReceiver(&UDPReceiverConfig{Workers: 1, Sockets: 1, QueueSize: 1, Diagnostics: recorder})
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(recorder)
	// Stop resets session channels, but the receiver keeps its lifetime counters.
	if err := r.Start("127.0.0.1", 0, nil); err != nil {
		t.Fatal(err)
	}
	recorder.Socket(0).Dropped(3)
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
	if got := diagnosticValue(t, registry, "dropped_datagrams_total", "", ""); got != 1 {
		t.Fatalf("drop count after session reset = %v, want 1", got)
	}
	if err := r.Start("127.0.0.1", 0, nil); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	recorder.Socket(0).Dropped(5)
	if got := diagnosticValue(t, registry, "dropped_datagrams_total", "", ""); got != 2 {
		t.Fatalf("drop count after reuse = %v, want 2", got)
	}
	if got := diagnosticValue(t, registry, "dropped_bytes_total", "", ""); got != 8 {
		t.Fatalf("drop bytes after reuse = %v, want 8", got)
	}
}

// A capacity-one queue and no decoder make overflow independent of worker timing.
func TestDiagnosticsDispatchDrops(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		name := "overflow"
		if blocking {
			name = "blocking"
		}
		t.Run(name, func(t *testing.T) {
			recorder := diagnostics.NewRecorder("test", 1024, 1)
			registry := prometheus.NewPedanticRegistry()
			callbacks := make(chan string, 2)
			var callbackDrops float64 // used only by the receiver goroutine
			r, err := NewUDPReceiver(&UDPReceiverConfig{
				Workers: 1, Sockets: 1, QueueSize: 1, Blocking: blocking, Diagnostics: recorder,
				ReceiverCallback: diagnosticsDropCallback(func(msg Message) {
					callbackDrops++
					if got := diagnosticValue(t, registry, "dropped_datagrams_total", "", ""); got != callbackDrops {
						t.Errorf("counter before callback = %v, want %v", got, callbackDrops)
					}
					callbacks <- string(msg.Payload)
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			registry.MustRegister(recorder)
			families, err := registry.Gather()
			if err != nil {
				t.Fatal(err)
			}
			present := 0
			for _, f := range families {
				if f.GetName() == "goflow_diagnostics_dropped_datagrams_total" || f.GetName() == "goflow_diagnostics_dropped_bytes_total" {
					if len(f.Metric) != 1 || f.Metric[0].Counter == nil {
						t.Fatalf("expected one drop counter: %v", f)
					}
					present++
				}
			}
			if present != 2 {
				t.Fatalf("initial drop counters: count=%d", present)
			}
			checkDrops := func(datagrams, bytes float64) {
				t.Helper()
				for name, want := range map[string]float64{"dropped_datagrams_total": datagrams, "dropped_bytes_total": bytes} {
					if got := diagnosticValue(t, registry, name, "", ""); got != want {
						t.Fatalf("%s = %v, want %v", name, got, want)
					}
				}
			}
			checkDrops(0, 0)
			conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- r.receiveRoutine(conn, recorder.Socket(0)) }()
			defer func() {
				close(r.q)
				conn.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("receiver did not stop")
				}
				for len(r.dispatch) > 0 {
					packetPool.Put(<-r.dispatch)
				}
			}()
			sender, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Close()
			send := func(payload string) {
				t.Helper()
				if _, err := sender.Write([]byte(payload)); err != nil {
					t.Fatal(err)
				}
			}
			waitValue := func(name string, want float64) {
				t.Helper()
				deadline := time.Now().Add(time.Second)
				for diagnosticValue(t, registry, name, "", "") != want {
					if time.Now().After(deadline) {
						t.Fatalf("timed out waiting for %s = %v", name, want)
					}
					time.Sleep(time.Millisecond)
				}
			}
			take := func(want string) {
				t.Helper()
				select {
				case pkt := <-r.dispatch:
					got := string(pkt.payload[:pkt.size])
					packetPool.Put(pkt)
					if got != want {
						t.Fatalf("queued payload = %q, want %q", got, want)
					}
				case <-time.After(time.Second):
					t.Fatal("packet did not reach queue")
				}
			}
			send("queued")
			waitValue("queue_length", 1)
			checkDrops(0, 0)
			if blocking {
				send("blocked")
				waitValue("socket_datagrams_total", 2)
				checkDrops(0, 0)
				take("queued")
				take("blocked")
				checkDrops(0, 0)
			} else {
				for _, payload := range []string{"abc", "12345"} {
					send(payload)
					select {
					case got := <-callbacks:
						if got != payload {
							t.Fatalf("callback payload = %q, want %q", got, payload)
						}
					case <-time.After(time.Second):
						t.Fatal("drop callback did not fire")
					}
				}
				checkDrops(2, 8)
				take("queued")
				send("queued again")
				take("queued again")
				checkDrops(2, 8)
			}
			select {
			case payload := <-callbacks:
				t.Fatalf("unexpected drop callback: %q", payload)
			default:
			}
		})
	}
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
