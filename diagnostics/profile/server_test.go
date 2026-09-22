package profile

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestProfilingLoopbackOnly(t *testing.T) {
	for _, address := range []string{":6060", "0.0.0.0:6060", "[::]:6060", "192.0.2.1:6060", "localhost:6060"} {
		if _, _, err := StartProfiling(address, 0, 0); err == nil {
			t.Fatalf("accepted nonliteral/nonloopback address %q", address)
		}
	}
}

func TestProfilingServer(t *testing.T) {
	server, done, err := StartProfiling("127.0.0.1:0", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = StopProfiling(context.Background(), server) })
	response, err := http.Get("http://" + server.Addr + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), "goroutine profile") {
		t.Fatalf("unexpected goroutine profile: status=%d err=%v", response.StatusCode, err)
	}
	if err := StopProfiling(context.Background(), server); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != http.ErrServerClosed {
		t.Fatalf("unexpected server exit: %v", err)
	}
}
