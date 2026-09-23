# GoFlow2 peak diagnostics — issue 508

Investigation history, measured findings, profile evidence and next experiments:
[performance-investigation-508.md](https://github.com/simpod/goflow2/blob/perf/508-diagnostics/docs/performance-investigation-508.md).

This experimental build is based on **v2.2.6**, pinned to **Go 1.25.5**, Linux
amd64, `GOAMD64=v1`, and `CGO_ENABLED=0`. Dependencies are unchanged. It measures
the existing pipeline; it does not tune queues, worker counts, compression,
partitioning, retries, or acknowledgement requirements.

## Deploy before peak

The artifact contains the binary, this document, `capture-peak.py`, build
metadata, and `SHA256SUMS`. In the extracted artifact directory:

```sh
sha256sum -c SHA256SUMS
chmod +x goflow2-508-diagnostics-linux-amd64
./goflow2-508-diagnostics-linux-amd64 -v
```

Use this binary in place of the current collector. Preserve the existing
`GOFLOW2_ARGS`, SASL environment, Kafka configuration, and listener. In particular,
keep `-addr ':8081'` so the existing remote Prometheus scrape continues to work,
and keep `-listen 'sflow://:9801?count=24'` (24 sockets, 48 workers).

Append these arguments:

```text
-diagnostics=true
-diagnostics.sample-every=1024
-diagnostics.kafka=true
-diagnostics.pprof.addr=127.0.0.1:6060
-diagnostics.pprof.mutex-fraction=1000
-diagnostics.pprof.block-rate=10000000
```

After restarting the collector, check on the server:

```sh
curl -fsS http://127.0.0.1:8081/metrics
curl -fsS 'http://127.0.0.1:6060/debug/pprof/goroutine?debug=1'
```

Check the scrape contains `goflow_diagnostics_build_info`,
`goflow_diagnostics_queue_capacity` with listener `sflow://:9801`,
`goflow2_kafka_requests_total`, and `go_sync_mutex_wait_total_seconds_total`.
Stage histograms appear after sampled traffic. Confirm Prometheus still scrapes
the target successfully. The binary's `-v` identifies its branch and commit.

Profiling is disabled without an explicit address. Only literal loopback IPs
are accepted. Profiling routes are **not** served on the public metrics port.
Access remotely through an SSH tunnel if needed. Do not use SIGQUIT: it would
terminate the Go process instead of collecting a non-destructive profile.

## Automatic peak capture

Run the capture script **on the collector server**, preferably shortly before
the peak window. Python 3 is sufficient. `ss` and `pidstat` add useful socket and
thread data when already available; the script does not install anything.

For the next eight hours, including the full evening peak:

```sh
umask 077
# If the packaged systemd service is used:
PID=$(systemctl show -p MainPID --value goflow2.service)
test "$PID" -gt 0
nohup python3 -u capture-peak.py \
  --pid "$PID" \
  --thread-schedstats \
  --duration 28800 \
  --output "./peak-$(date -u +%Y%m%dT%H%M%SZ)" \
  >capture-peak.log 2>&1 </dev/null &
```

For a manually managed collector, supply its actual PID instead. Start the
capture again with the new PID if GoFlow restarts. Do not run two capture scripts
against the same process: only one CPU profile can be active at a time.

If starting much earlier, the default duration is 24 hours. Hourly baselines and
peak triggers share a **24-bundle cap**: frequent triggers can exhaust it before
the duration ends. For a 30-hour run spanning tomorrow's peak, use
`--duration 108000 --baseline-interval 14400 --cooldown 1800`; check the log and
restart with a new output directory before peak if the bundle cap is reached.

Capture behavior:

- Validate diagnostics and local pprof access at startup; fail visibly if absent.
- Capture an initial baseline and periodic baselines (hourly by default).
- Poll every 10 seconds; trigger at queue occupancy >=90% or increasing drop
  counters, with a default 15-minute cooldown between bundles.
- Capture 30-second CPU, mutex, block, and allocation delta profiles concurrently.
- Capture heap without forcing GC, binary goroutine profiles, before/after full
  goroutine stacks, metrics, socket state, process/thread scheduling data, and
  optional `pidstat` alongside the profiles.
- Record UTC start/end times and success/errors for each capture. Files remain
  local, private to the capture user; no environment or credentials are dumped.
- Retain at most **six bundles**: the initial baseline, the first queue/drop
  capture, and the four newest other captures. Before the first congestion
  capture, retain the baseline and five newest captures. Delete only older,
  completed, unpinned bundles created by this script in this run directory.
- Limit **all retained capture file contents to 2 GiB** and require at least
  **5 GiB free** on the output filesystem before each write. The shared budget
  includes profiles, metrics, system snapshots, metadata and `errors.log`, even
  when capture threads write concurrently. Keep evidence already collected and
  stop with a `STORAGE STOP` message and nonzero exit status if either guard trips.
- Limit each profile response to **16 MiB** by default. Reject and remove an
  incomplete response that exceeds this limit, while allowing other captures to
  continue. Removed files and rotated bundles release their recorded byte budget.
- Do not capture execution traces by default. `--trace-seconds 1` enables a short
  trace, which has higher overhead and should be used only if needed.

The storage controls are enabled by default; no extra flags are needed:

```text
--keep-bundles 6
--max-total-bytes 2147483648
--min-free-bytes 5368709120
--profile-bytes 16777216
```

The profile trigger is configurable with `--queue-threshold 0.90` (the default).
For the one-million-datagram queue, this means 900,000 queued datagrams. It is
independent of the disk-space guards. Increasing drops can still trigger a
capture below the threshold if the queue filled and partly drained between polls.
Initial and periodic baselines still run; all triggers respect the cooldown.

Use a directory under `/home` on the reported collector filesystem (44 GiB free),
not RAM-backed `/tmp` or the nearly capacity-limited root filesystem. The budget
counts file contents, not filesystem block/metadata overhead. The free-space
reserve adds headroom for that overhead, but other processes can consume disk
space between a check and a write. An externally redirected `capture-peak.log`
is outside the budget; messages are bounded and the script stops on a storage
limit. A storage-interrupted bundle can be incomplete or lack final metadata.
Retention protects already completed baseline and first-congestion evidence.

`--max-bundles 24` still limits the **total number of captures made**, while
`--keep-bundles 6` limits how many remain on disk. These are separate limits.

Read `capture-peak.log` and the output directory's `errors.log` after the first
baseline. Missing permissions or utilities are reported, not silently treated as
successful captures. Polling pauses during each bundle, so brief transients can
still be missed. Long-lived stalls remain visible in goroutine profiles even if
the relevant datagrams were not sampled.

Keep the exact binary with the capture bundles for symbol resolution:

```sh
go tool pprof -top ./goflow2-508-diagnostics-linux-amd64 ./peak-*/01-baseline/cpu.pb
go tool pprof -top ./goflow2-508-diagnostics-linux-amd64 ./peak-*/01-baseline/block.pb
go tool pprof -top ./goflow2-508-diagnostics-linux-amd64 ./peak-*/01-baseline/mutex.pb
```

Select the desired peak bundle rather than the baseline for diagnosis. If the
Go CPU profile identifies system-call wrappers but not the kernel function
responsible, a separate Linux `perf` capture may still be required. This build
cannot guarantee that one peak isolates every possible kernel or broker problem.

## Pipeline and receiver metrics

`-diagnostics` adds a collector per listener and enables all metrics available
from the pinned Go runtime. Existing Go memory metrics are preserved.

| Metric | Meaning |
|---|---|
| `goflow_diagnostics_build_info` | Version and build metadata, value 1 |
| `goflow_diagnostics_queue_length` / `_capacity` | Dispatch datagrams at scrape time; label `listener` |
| `goflow_diagnostics_workers{state="pipeline"}` | Exact number of worker slots marked busy, including unsampled handlers |
| `goflow_diagnostics_workers{state="waiting_for_packet"}` | Worker slots not handling a packet |
| `goflow_diagnostics_workers{state="sampled_..."}` | Sampled active phase only; overlaps `pipeline`, never add to it |
| `goflow_diagnostics_socket_datagrams_total` | Reads completed before enqueue/drop; labels `listener`, `socket` |
| `goflow_diagnostics_socket_bytes_total` | Bytes received before dispatch |
| `goflow_diagnostics_socket_errors_total` | Setup/read errors, including socket shutdown |
| `goflow_diagnostics_sample_every` | Sampling interval per worker (default 1024; first datagram sampled) |
| `goflow_diagnostics_stage_seconds` | Histogram with `_sum`, `_count`, `_bucket`; unscaled sampled datagram timings |
| `goflow_diagnostics_sample_errors_total{outcome}` | Sampled error/panic outcomes, including recovered panics |

Stages are exclusive, except for the separately reported totals:

| Stage | Boundary |
|---|---|
| `decode` | Protocol decoding |
| `produce` | Constructing and enriching flow messages |
| `producer_metrics` | Existing per-sample production metrics |
| `format` | Binary formatting, summed over messages in the sampled datagram |
| `kafka_enqueue` | Calls to `Transport.Send`, including waits; summed per datagram, not acknowledgements |
| `wrapper_metrics` | Existing receive/timing wrapper work outside the inner pipeline |
| `pipeline` | Remaining inner pipeline work, including normal pooled-message commit cleanup |
| `handling` | Entire sampled handler; overlaps all stages above |
| `queue_wait` | Time from receive timestamp to worker start; separate from handling |

Only sampled packets incur stage clock reads and histogram publication. Each
visited stage produces **one observation per sampled datagram**, not one per flow
message. Receiver counters and exact busy flags use per-socket/per-worker slots;
no exporter labels or per-message shared diagnostic counter updates are added.
Queue wait uses the existing wall-clock timestamp and can be affected by clock
adjustments. Very long stalls are not added to completed-duration totals until
they finish. Use active state and profiles alongside completed timings.

For stage time per sampled datagram (add the instance/listener selectors):

```promql
sum by (stage) (rate(goflow_diagnostics_stage_seconds_sum{stage!~"handling|queue_wait"}[5m]))
/
scalar(sum(rate(goflow_diagnostics_stage_seconds_count{stage="handling"}[5m])))
```

Multiplying sampled stage `_sum` rates by `sample_every` estimates worker-seconds
per second; it is an estimate, not an exact worker count. Quantiles describe
sampled datagrams. Do not add `handling` or `queue_wait` to exclusive stage totals.

## Kafka diagnostics

`-diagnostics.kafka` exports metrics already maintained by Sarama 1.38.1 at
scrape time. It is independent of pipeline diagnostics. Families include:

- `goflow2_kafka_requests_total`, `responses_total`, `requests_in_flight`.
- `goflow2_kafka_incoming_bytes_total`, `outgoing_bytes_total`.
- `goflow2_kafka_records_sent_total`: records encoded into produce requests,
  **including retries**, not acknowledged deliveries.
- `goflow2_kafka_request_latency_seconds`, `request_size_bytes`,
  `response_size_bytes`, `batch_size_bytes`, `records_per_request`.
- `goflow2_kafka_producer_errors_total{code}`: terminal message errors counted
  before the existing best-effort logging channel.
- `goflow2_kafka_error_forwarding_dropped_total`: error notifications lost by
  that logging channel, not an additional count of lost messages.
- `goflow2_kafka_producer_input_queue_length` / `_capacity`: the public input
  channel is **unbuffered in this Sarama version**. Both are normally zero; this
  says nothing about internal backlog or waiting senders.

Broker-specific metrics use `broker="<id>"`; `broker="all"` is the aggregate.
Do not sum the aggregate with individual brokers. Reservoir statistics use
`statistic="mean|p50|p95|p99|max"` and are **gauges**, not cumulative histogram
buckets. Meter counts can reset on reconnection. `_per_second` families are
Sarama one-minute EWMAs; normal Prometheus `rate(..._total[5m])` is also available.
This Sarama version provides no retry-attempt counter; none is fabricated.

`-diagnostics.kafka.successes=true` optionally enables and drains Sarama success
notifications, exposing `goflow2_kafka_producer_successes_total`. It adds a channel
handoff/counter operation for every completed message. Leave it **off for the
first low-overhead comparison**. It does not change RequiredAcks; successful
completion has the configured acknowledgement semantics. Without it, broker
acknowledged message count is not measured directly.

## Interpretation during peak

- Queue filling + dominant `kafka_enqueue`: inspect block/goroutine profiles and
  Sarama request latency/in-flight/batch metrics to distinguish client submission
  contention from downstream delay.
- Dominant `producer_metrics` or `wrapper_metrics`: inspect mutex/block profiles
  for shared Prometheus collectors.
- Dominant `produce`/`format` + allocation/GC work: inspect CPU/allocs profiles.
- High runnable latency or runtime mutex wait: inspect scheduler and mutex
  profiles; idle host cores alone do not prove workers can run concurrently.
- Socket receive imbalance: inspect per-socket rates before proposing reuseport
  sharding. Several heavy exporter tuples may concentrate on a few sockets.

Existing node-exporter kernel UDP drops, host CPU modes, network rates and
context-switch metrics should be graphed alongside the collector metrics.

Do not treat the absence of a metric as zero. Stage series appear only after a
sample reaches them, error series after their first event, and broker series
depend on established connections. The standard application-drop series may be
absent after restart until its first drop.
