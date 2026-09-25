// Package diagnostics provides optional, bounded receiver and pipeline diagnostics.
// A recorder belongs to one listener. Worker and socket IDs are fixed at setup.
package diagnostics

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// DefaultSampleEvery is the per-worker datagram sampling interval.
const DefaultSampleEvery = 1024

// Stage identifies an exclusive phase of sampled datagram handling.
type Stage uint32

const (
	Pipeline Stage = iota
	Decode
	Produce
	ProducerMetrics
	Format
	KafkaEnqueue // Time in Transport.Send, not broker acknowledgement; also applies to other transports.
	WrapperMetrics
	stageCount
)

var stageNames = [...]string{"pipeline", "decode", "produce", "producer_metrics", "format", "kafka_enqueue", "wrapper_metrics"}

type worker struct {
	// 0 = waiting; 1 = unsampled pipeline; 2+ = sampled stage.
	state    atomic.Uint32
	sequence uint64 // owned by this worker's goroutine
	trace    Trace
}

// Socket counters use single-writer cumulative stores, not contended increments.
// Read errors include socket shutdown errors. Counts are published before dispatch.
type Socket struct {
	datagrams        atomic.Uint64
	bytes            atomic.Uint64
	errors           atomic.Uint64
	n, b, e          uint64
	droppedDatagrams atomic.Uint64
	droppedBytes     atomic.Uint64
	dn, db           uint64
}

// Dropped publishes a datagram rejected by the full, nonblocking dispatch queue.
// The socket goroutine is the sole writer, as for Received. It is nil-safe.
func (s *Socket) Dropped(bytes int) {
	if s == nil {
		return
	}
	s.dn++
	s.db += uint64(bytes)
	s.droppedDatagrams.Store(s.dn)
	s.droppedBytes.Store(s.db)
}

// Received publishes a successful socket read before it enters the dispatch queue.
func (s *Socket) Received(bytes int) {
	if s == nil {
		return
	}
	s.n++
	s.b += uint64(bytes)
	s.datagrams.Store(s.n)
	s.bytes.Store(s.b)
}

// Error publishes a socket setup/read error. The socket goroutine is the sole writer.
func (s *Socket) Error() {
	if s == nil {
		return
	}
	s.e++
	s.errors.Store(s.e)
}

// Recorder implements prometheus.Collector. Configure it before registration/start.
type Recorder struct {
	listener string
	every    uint64
	workers  []worker
	sockets  []Socket
	queue    func() (int, int)
	duration *prometheus.HistogramVec
	errors   *prometheus.CounterVec
	desc     [9]*prometheus.Desc
}

// NewRecorder allocates fixed worker slots. Zero sampleEvery selects the default.
// It does not register metrics or enable diagnostics on any receiver by itself.
func NewRecorder(listener string, sampleEvery uint64, workerCount int) *Recorder {
	if sampleEvery == 0 {
		sampleEvery = DefaultSampleEvery
	}
	r := &Recorder{listener: listener, every: sampleEvery, workers: make([]worker, workerCount)}
	labels := prometheus.Labels{"listener": listener}
	r.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "goflow_diagnostics_stage_seconds", Help: "Unscaled sampled datagram duration; repeated stage calls are summed per datagram.", ConstLabels: labels, Buckets: prometheus.ExponentialBuckets(0.000001, 4, 12)}, []string{"stage"})
	r.errors = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "goflow_diagnostics_sample_errors_total", Help: "Sampled datagrams ending in error or panic.", ConstLabels: labels}, []string{"outcome"})
	names := []string{"queue_length", "queue_capacity", "workers", "sample_every", "socket_datagrams_total", "socket_bytes_total", "socket_errors_total", "dropped_datagrams_total", "dropped_bytes_total"}
	help := []string{"Dispatch queue length at scrape time.", "Dispatch queue capacity.", "Worker count by exact busy state and sampled detail. Detailed stages describe sampled packets only.", "One in this many datagrams is sampled per worker (first is sampled).", "Received UDP datagrams before dispatch, including empty datagrams.", "Received UDP bytes before dispatch.", "Socket receive/setup errors, including shutdown read errors.", "UDP datagrams dropped because the nonblocking dispatch queue is full, summed across listener sockets.", "Received UDP payload bytes dropped because the nonblocking dispatch queue is full, summed across listener sockets."}
	for i, name := range names {
		var variable []string
		if i == 2 {
			variable = []string{"state"}
		}
		if i >= 4 && i <= 6 {
			variable = []string{"socket"}
		}
		r.desc[i] = prometheus.NewDesc("goflow_diagnostics_"+name, help[i], variable, labels)
	}
	return r
}

// BindReceiver must run once before registration/start. Counts must match NewRecorder.
func (r *Recorder) BindReceiver(workers, sockets int, queue func() (int, int)) {
	if r == nil {
		return
	}
	if workers != len(r.workers) {
		panic("diagnostics: worker count mismatch")
	}
	r.sockets = make([]Socket, sockets)
	r.queue = queue
}

// Socket returns a fixed socket slot after BindReceiver. A nil recorder returns nil.
func (r *Recorder) Socket(id int) *Socket {
	if r == nil {
		return nil
	}
	return &r.sockets[id]
}

// Begin marks every datagram busy, but only reads the clock for sampled datagrams.
// Each worker ID must have one caller. Finish must precede its next Begin.
func (r *Recorder) Begin(id int, received time.Time) *Trace {
	if r == nil {
		return nil
	}
	w := &r.workers[id]
	sample := w.sequence%r.every == 0
	w.sequence++
	if !sample {
		w.state.Store(1)
		return nil
	}
	now := time.Now()
	w.trace = Trace{recorder: r, worker: w, start: now, last: now, current: Pipeline}
	w.state.Store(uint32(Pipeline) + 2)
	if !received.IsZero() {
		w.trace.queueWait = now.Sub(received).Seconds()
		// UDP receive timestamps use wall time; tolerate a backwards clock step.
		if w.trace.queueWait < 0 {
			w.trace.queueWait = 0
		}
	}
	return &w.trace
}

// Idle must be deferred around every handler, including unsampled ones.
func (r *Recorder) Idle(id int) {
	if r == nil {
		return
	}
	r.workers[id].state.Store(0)
}

// Trace is goroutine-local and valid only during one datagram handler.
type Trace struct {
	recorder    *Recorder
	worker      *worker
	start, last time.Time
	current     Stage
	durations   [stageCount]time.Duration
	seen        [stageCount]bool
	queueWait   float64
	wrapper     bool
	panicked    bool
}

// SetStage closes the previous phase and starts the next. It is nil-safe.
func (t *Trace) SetStage(stage Stage) {
	if t == nil {
		return
	}
	now := time.Now()
	t.durations[t.current] += now.Sub(t.last)
	t.seen[t.current] = true
	t.last, t.current = now, stage
	t.worker.state.Store(uint32(stage) + 2)
}

// MarkWrapper attributes outer decoder wrapper time to wrapper_metrics. The
// worker timer encloses the wrapper; inner stage switches exclude their time.
func (t *Trace) MarkWrapper() {
	if t != nil {
		t.wrapper = true
		t.SetStage(WrapperMetrics)
	}
}

// EndPipeline returns to the enclosing decoder wrapper, if present.
func (t *Trace) EndPipeline() {
	if t == nil {
		return
	}
	if t.wrapper {
		t.SetStage(WrapperMetrics)
	} else {
		t.SetStage(Pipeline)
	}
}

// MarkPanic records a panic recovered by an inner wrapper. It is nil-safe.
func (t *Trace) MarkPanic() {
	if t != nil {
		t.panicked = true
	}
}

// Finish records one observation per visited stage even on error/panic. It does not
// recover panics. Total handling excludes queue wait and diagnostic publication.
func (t *Trace) Finish(err error, panicked bool) {
	if t == nil {
		return
	}
	now := time.Now()
	t.durations[t.current] += now.Sub(t.last)
	t.seen[t.current] = true
	for i, seen := range t.seen {
		if seen {
			t.recorder.duration.WithLabelValues(stageNames[i]).Observe(t.durations[i].Seconds())
		}
	}
	t.recorder.duration.WithLabelValues("queue_wait").Observe(t.queueWait)
	t.recorder.duration.WithLabelValues("handling").Observe(now.Sub(t.start).Seconds())
	if panicked || t.panicked {
		t.recorder.errors.WithLabelValues("panic").Inc()
	} else if err != nil {
		t.recorder.errors.WithLabelValues("error").Inc()
	}
}

func (r *Recorder) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range r.desc {
		ch <- d
	}
	r.duration.Describe(ch)
	r.errors.Describe(ch)
}

func (r *Recorder) Collect(ch chan<- prometheus.Metric) {
	length, capacity := 0, 0
	if r.queue != nil {
		length, capacity = r.queue()
	}
	ch <- prometheus.MustNewConstMetric(r.desc[0], prometheus.GaugeValue, float64(length))
	ch <- prometheus.MustNewConstMetric(r.desc[1], prometheus.GaugeValue, float64(capacity))
	ch <- prometheus.MustNewConstMetric(r.desc[3], prometheus.GaugeValue, float64(r.every))
	waiting, busy := 0, 0
	var stages [stageCount]int
	for i := range r.workers {
		w := &r.workers[i]
		if s := w.state.Load(); s > 0 {
			busy++
			if s > 1 {
				stages[s-2]++
			}
		} else {
			waiting++
		}
	}
	ch <- prometheus.MustNewConstMetric(r.desc[2], prometheus.GaugeValue, float64(waiting), "waiting_for_packet")
	ch <- prometheus.MustNewConstMetric(r.desc[2], prometheus.GaugeValue, float64(busy), "pipeline")
	for i, n := range stages {
		ch <- prometheus.MustNewConstMetric(r.desc[2], prometheus.GaugeValue, float64(n), "sampled_"+stageNames[i])
	}
	var droppedDatagrams, droppedBytes uint64
	for i := range r.sockets {
		s := &r.sockets[i]
		droppedDatagrams += s.droppedDatagrams.Load()
		droppedBytes += s.droppedBytes.Load()
		label := strconv.Itoa(i)
		for j, v := range []uint64{s.datagrams.Load(), s.bytes.Load(), s.errors.Load()} {
			ch <- prometheus.MustNewConstMetric(r.desc[4+j], prometheus.CounterValue, float64(v), label)
		}
	}
	ch <- prometheus.MustNewConstMetric(r.desc[7], prometheus.CounterValue, float64(droppedDatagrams))
	ch <- prometheus.MustNewConstMetric(r.desc[8], prometheus.CounterValue, float64(droppedBytes))
	r.duration.Collect(ch)
	r.errors.Collect(ch)
}
