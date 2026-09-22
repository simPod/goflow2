// Package profile provides an explicitly enabled local profiling server.
package profile

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/netip"
	"runtime"
	"time"
)

// StartProfiling starts an explicit loopback-only profiling server. Importing
// net/http/pprof registers handlers on DefaultServeMux, so the public metrics
// server must always use its own ServeMux, including when profiling is disabled.
// Rates apply only when this server is enabled. Zero disables a profile type.
func StartProfiling(addr string, mutexFraction, blockRate int) (*http.Server, <-chan error, error) {
	if mutexFraction < 0 || blockRate < 0 {
		return nil, nil, fmt.Errorf("profiling rates must be nonnegative")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, nil, fmt.Errorf("profiling address: %w", err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !ip.IsLoopback() {
		return nil, nil, fmt.Errorf("profiling address must use a literal loopback IP, such as 127.0.0.1:6060")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	server := &http.Server{Addr: listener.Addr().String(), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	runtime.SetMutexProfileFraction(mutexFraction)
	runtime.SetBlockProfileRate(blockRate)
	errors := make(chan error, 1)
	go func() {
		errors <- server.Serve(listener)
		close(errors)
	}()
	return server, errors, nil
}

// StopProfiling allows active bounded profile requests to finish until ctx expires.
func StopProfiling(ctx context.Context, server *http.Server) error {
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}
