# GoFlow2 peak-loss investigation — issue 508

Status: diagnostic data collected; dominant worker delay identified; configurable producer-pool experiment implemented on `perf/508-producer-pool`; production benefit not yet validated. Alert work is explicitly deferred.

Observation dates: 2026-09-20 through 2026-09-23. Times below are **Europe/Prague (CEST, UTC+02:00)** unless marked UTC. This document records the investigation through analysis of the September 23 peak captures. It is an investigation log, not an accepted architecture decision.

- Upstream discussion: <https://github.com/netsampler/goflow2/issues/508>
- Fork: <https://github.com/simpod/goflow2>
- Diagnostic branch: `perf/508-diagnostics`
- Deployment/capture reference: [diagnostics-508.md](diagnostics-508.md)
- Upstream performance guidance reviewed: [performance.md at 6dee964](https://github.com/netsampler/goflow2/blob/6dee964c38ee5f6b04a38681d069427c28ee5cb3/docs/performance.md)

## Executive summary

The collector receives more datagrams at peak than it can process. Its million-datagram application queue fills, retains substantial RAM, and drops new datagrams. This is confirmed by application counters; kernel UDP receive-buffer loss is not the dominant loss source in the measured intervals.

The diagnostic build attributes about **89% of sampled worker handling time to the Kafka enqueue stage**. Peak goroutine profiles show most collector workers blocked sending messages to Sarama. Sarama's producer and topic dispatchers spend substantial CPU on channel handoffs, while partition handlers mostly wait for input. This is strong evidence of a **shared client-side dispatch/handoff throughput limit**. It is not evidence that broker processing is slow.

The next controlled experiment is **two independent Sarama asynchronous producer instances inside the same GoFlow process**, retaining the current receiver configuration. It is now implemented on `perf/508-producer-pool`, but its production benefit is not yet measured. More collector workers alone may just add waiting senders. See [experiment instructions](producer-pool-508.md) and [ADR](adr/0001-independent-kafka-producers.md).

No root-cause commit explaining all differences between releases has been proved. No production throughput fix has yet been deployed or verified.

## 1. Environment and original settings

- Debian GNU/Linux 12 (bookworm), amd64.
- 64 online logical CPUs shown by the operator; CPUs 65–128 offline in htop.
- Diagnostic runtime reports `GOMAXPROCS=64`; process affinity allows CPUs 0–63.
- About 64 GiB RAM (62.5 GiB shown by htop).
- Official GitHub release binaries were used, not local custom builds before diagnostics.
- All flow traffic must continue to arrive on **UDP port 9801**.
- One Kafka topic, `flows`; six brokers are visible in diagnostic metrics.

Relevant arguments, with the service-discovery hostname omitted:

```text
-transport=kafka
-transport.kafka.tls=true
-transport.kafka.tls.insecure=true
-transport.kafka.sasl scram-sha256
-transport.kafka.srv=<existing SRV name>
-transport.kafka.topic=flows
-transport.kafka.version=3.5.0
-format=bin
-addr :8081
-listen sflow://:9801?count=24
```

SASL credentials are not recorded here. The existing TLS settings were retained for the experiment, not reviewed as part of the performance diagnosis.

Effective settings in v2.2.6:

| Setting | Value |
|---|---|
| Receive sockets | 24 |
| Processing workers | 48 (default is twice the socket count) |
| Dispatch queues | One shared queue per listener |
| Dispatch capacity | 1,000,000 datagrams |
| Blocking receive mode | False: full dispatch queue drops new datagrams |
| Kafka producer instances | One |
| Kafka compression | None |
| Kafka partitioner | Round-robin |
| Kafka flush frequency | 5 seconds |
| Kafka flush-byte target | 100 MiB; other producer limits can cause earlier sends |
| Kafka successes notifications | Disabled unless explicitly enabled by diagnostic option |

Each retained receive packet has a 9,000-byte backing buffer. A full million-packet queue accounts for about 8.4 GiB of payload allocations before allocator overhead and other objects. This is not an explanation of all process/host memory by itself.

## 2. Important corrections to earlier assumptions

1. The original interpretation of the upgrade was “lower CPU means better performance.” Delivery/processing counts do not support that conclusion on their own.
2. The operator initially believed v2.2.2 had no comparable drops, then clarified that this had not been checked. Drops were subsequently confirmed on v2.2.3. **There is no established loss-free v2.2.2 baseline.** The old-version control build is a measurement baseline, not proven good.
3. The original graph query was:

   ```promql
   sum(rate(goflow2_flow_process_sf_samples_records_total[5m])) by (version,type)
   ```

   It measures **records inside processed sFlow samples**, not incoming UDP packets or successful Kafka deliveries. One datagram can contain multiple samples, and one sample can contain multiple records.
4. The Prometheus v1.21.1 CAS-backoff revert was initially highlighted as a possible explanation. Inspection showed it does not change the relevant integer-counter or objective-enabled-summary paths used here. It was also already present in v2.2.3. This specific explanation was withdrawn.
5. Go 1.24's new maps, `sync.Map`, and internal mutex implementation were initially discussed across v2.2.2 → v2.2.6. The actual v2.2.3 binary already uses Go 1.24.1. Their introduction does not explain v2.2.3 → v2.2.6 by itself.
6. A host CPU snapshot at 40–50% was off-peak. Later time-series measurements, not that snapshot, established the peak comparison.
7. The CPU step at about 14:38 on September 22 followed a downgrade from v2.2.6 to v2.2.3. Evening drops did not start until about 20:15–20:20. Extra CPU cost therefore preceded drops; it is not solely the cost of discarding packets.
8. The upstream guide recommends worker count near available CPU count. Trying 64 workers was discussed, then deferred: first identify what the occupied workers wait for. The diagnostic baseline remains **48 workers**.

## 3. Release and commit analysis

Embedded metadata was inspected in downloaded official Linux amd64 binaries:

| Release | Actual Go runtime | Prometheus client | Protobuf runtime | Sarama |
|---|---|---|---|---|
| v2.2.2 | 1.23.6 | 1.21.0 | 1.36.5 | 1.38.1 |
| v2.2.3 | 1.24.1 | 1.22.0 | 1.36.6 | 1.38.1 |
| v2.2.6 | 1.25.5 | 1.23.2 | 1.36.11 | 1.38.1 |

All inspected binaries use `CGO_ENABLED=0`, `GOARCH=amd64`, and `GOAMD64=v1`. Workflow/toolchain directives alone did not identify the actual compiler version, because some selectors floated.

Relevant source findings:

- Sarama, the Kafka send path, receiver queue/drop policy, and worker defaults did not change materially across these releases.
- `3306119` / #465 regenerated protobuf code from generator v1.26.0 to v1.36.11. It changes message layout, reset/reflection methods and descriptor storage. It is a candidate for performance differences, but **cannot explain drops already observed on v2.2.3**, where it is absent.
- `67761ed` / #467 changes only generated compiler-version comments. It is not a runtime-performance candidate.
- Protobuf runtime upgrade commits: `2d7b840`, `0e7433f`, `39f34e9`, `b981ce8`, `a1f5198`. No particular one was proved responsible.
- `454930f` / #463 corrects an EtherType shift used to construct mapping keys. Actual parser selection was already correct; no custom mapping was supplied. Low-priority candidate.
- `6f741dc` / #469 changes switches and formatting code; no obvious new blocking path was found.
- `klauspost/compress` changes are not a direct Kafka compression explanation because compression is disabled.
- SCRAM changes principally affect authentication, not steady-state message submission absent reconnect activity.
- IPFIX timestamp/template changes do not apply to this sFlow listener.
- v2.2.6 embeds `containermaxprocs=0,updatemaxprocs=0`; automatic Go 1.25 container-aware GOMAXPROCS was not enabled by default.
- v2.2.3 embeds `asynctimerchan=1`; v2.2.6 uses newer timer behavior by default. An old-timer comparison was proposed but **not reported as executed**. No timer cause has been established.

## 4. Isolated binaries prepared

Each variant has its own branch in the fork. They are not cumulative experiments.

| Branch under `perf/508-` | Purpose | GitHub Actions run |
|---|---|---|
| `control-v222` | v2.2.2 + Go 1.23.6 | [35732351490](https://github.com/simpod/goflow2/actions/runs/35732351490) |
| `v222-go1255` | Same old source/dependencies + Go 1.25.5 | [35732352032](https://github.com/simpod/goflow2/actions/runs/35732352032) |
| `control-v226` | v2.2.6 + Go 1.25.5 | [35732355941](https://github.com/simpod/goflow2/actions/runs/35732355941) |
| `v226-old-protobuf-code` | v2.2.6 with old generated files, new protobuf runtime | [35732406940](https://github.com/simpod/goflow2/actions/runs/35732406940) |
| `v226-old-protobuf-runtime` | v2.2.6 generated files, protobuf runtime replaced with 1.36.5 | [35732570762](https://github.com/simpod/goflow2/actions/runs/35732570762) |

The protobuf-runtime-only experiment needs a module `replace`, because Prometheus otherwise selects a newer protobuf dependency. Other dependencies were retained. These controls have diagnostic version strings and are not byte-identical copies of official releases.

All five built and passed their Go tests on GitHub Actions; artifact checksums and embedded toolchains/dependencies were checked. No results from deploying these isolation variants were reported during this investigation. Artifact retention was configured to 90 days.

## 5. Existing metrics: useful boundaries and measured comparison

The existing `goflow2_flow_decoding_time_seconds` summary times the inner pipeline, including production, its metrics, formatting and Kafka enqueue. It is **not pure decode time** and is **not broker delivery latency**. It excludes receive-queue wait, initial wrapper metrics and updating the timing summary itself.

At steady state, `rate(..._sum[5m])` approximates concurrent workers inside that timed path. About 47 of 48 workers were occupied even though host CPU had headroom.

Five-minute rates at 21:00 CEST, mapped to releases using the operator's deployment timeline:

| Metric | Sept 21, v2.2.6 | Sept 22, v2.2.3 |
|---|---:|---:|
| Host UDP datagrams received/s (not port-specific) | 156,248 | 156,557 |
| Completed GoFlow datagrams/s | 136,692 | 150,623 |
| Application queue drops/s | 18,954 | 5,565 |
| Timed worker occupancy | 47.33 | 47.25 |
| Process CPU cores consumed | 15.66 | 45.22 |
| Host system CPU cores consumed | 1.43 | 30.76 |
| Kernel UDP receive-buffer drops/s | 0 | 0 |

At those points, v2.2.3 processed about 10% more datagrams while using nearly three times the process CPU. This is a workload comparison, not a controlled replay proving a particular runtime regression.

Historical loss intervals from five-minute queries:

- Sept 21: drops from about 18:20 to 23:45, peak around 19.5k datagrams/s.
- Sept 22: restart at 14:38:44 CEST; drops from about 20:20 to 22:30, peak around 6.1k/s in the operator's graph.
- Small kernel UDP drop rates appeared near the restart, not comparable to the sustained application losses.

The normal drop counter can be absent until an exporter first drops. Absence is not an independently measured zero.

## 6. Diagnostic build and capture tooling

Branch: `perf/508-diagnostics`, based on v2.2.6 with pinned Go 1.25.5 and unchanged dependencies.

| Commit | Change | Artifact run |
|---|---|---|
| `d041895` | Pipeline/receiver/Kafka/runtime diagnostics; loopback profiling; automated captures | [35793691021](https://github.com/simpod/goflow2/actions/runs/35793691021) |
| `74942da` | Total storage budget, free-space guard, retained evidence rotation | [35829886153](https://github.com/simpod/goflow2/actions/runs/35829886153) |
| `d55116d` | Configurable queue trigger, default 90% | [35830110911](https://github.com/simpod/goflow2/actions/runs/35830110911) |

The deployed build identifies itself as `perf-508-diagnostics-d55116d80616`.

Enabled flags:

```text
-diagnostics=true
-diagnostics.sample-every=1024
-diagnostics.kafka=true
-diagnostics.pprof.addr=127.0.0.1:6060
-diagnostics.pprof.mutex-fraction=1000
-diagnostics.pprof.block-rate=10000000
```

Measurements include:

- Sampled per-datagram stage durations: decode, produce, producer metrics, format, Kafka enqueue, wrapper metrics, remaining pipeline work, total handling and queue wait.
- Exact per-worker busy slots; sampled active-stage detail is separate and overlaps the total busy count.
- Actual dispatch queue depth/capacity and per-socket successful receive counts/bytes before enqueue or drop.
- Existing Sarama registry statistics at scrape time; terminal producer errors counted before lossy log forwarding.
- Full Go runtime metrics, including scheduler latency, mutex waiting, GC CPU and allocation assists.
- Separate explicit loopback-only pprof server; public metrics mux returns 404 for profiling paths.

Stage sums/counts are **unscaled samples**, one in 1024 datagrams per worker. Repeated format/send work is summed per sampled datagram. Multiplication by 1024 estimates occupancy; it is not an exact instantaneous worker count.

Sarama's public Input channel is unbuffered, so its length and capacity are zero. Those gauges cannot reveal blocked senders or internal queue lengths. Sarama records-sent counts include retries and are not acknowledgements. Optional success notifications were left off to avoid adding a per-message channel/counter cost. There is no retry-attempt counter in this pinned Sarama registry.

Capture script defaults after d55116d:

- Initial baseline, then hourly baseline; congestion trigger at queue >=90% or increasing drop counters.
- Default 15-minute cooldown; the documented 30-hour command uses four-hour baselines and a 30-minute cooldown.
- 24 captures maximum over a run, retaining at most six bundles.
- Preserve initial baseline, first queue/drop capture, and four recent other captures.
- 2 GiB retained-file-content budget, 5 GiB free-space reserve, 16 MiB per profile response.
- CPU/block/mutex/allocs delta profiles, heap without forced GC, goroutine binary/text snapshots, metrics, `ss`, process/thread scheduling and optional `pidstat`.
- Files remain local; no automatic upload. A fixed PID requires restarting capture after a collector restart.

Storage on the collector: `/home` had 44 GiB free and was chosen. `/tmp` is RAM-backed and was avoided. The storage cap excludes filesystem metadata/block overhead and an externally redirected shell log.

Verification completed before publishing: full Go tests; targeted race tests; 40 capture-script tests at d55116d; compiled-binary UDP/HTTP/profile smoke tests; artifact checksums; toolchain metadata. Production overhead was not independently benchmarked with identical replay traffic. The peak data confirms the instrumentation is present and yielding coherent rates, not that its overhead is zero.

## 7. Grafana work and data access

Dashboard: **GoFlow – Internals**, UID `4U5xQZPmz`, in the FLOP folder. Prometheus datasource UID: `000000003`.

Local access wrapper: `/Users/user/Work/Code/CDN77/grafana/overkill-grafana`. It already caches credentials in a private temporary file for eight hours with mode 0600. Do not print tokens or put them into this repository.

Changes made through the Grafana API:

- Version 9: added scoped bottleneck panels; corrected “worker rate,” which previously applied `rate` to quantiles and grouped by a nonexistent worker label; replaced duplicate timing panel; renamed misleading UDP2Kafka panels.
- Version 10: added queue occupancy/wait, actual worker states, stage timing/estimated occupancy, receive-socket distribution, Kafka request/batch/error metrics and detailed runtime panels.
- New queries were accepted by Prometheus; dashboard saves were read back and verified. Browser visual verification was blocked by login. Live metrics later confirmed the deployed diagnostic series.

Scope of added panels: this collector's `:8081` instance and listener `sflow://:9801`, with node exporter on `:6100`. Older fleet-wide panels have broader selectors.

## 8. September 23 diagnostic measurements

At about 21:55 CEST, five-minute sampled handling-time shares were:

| Stage | Share |
|---|---:|
| Kafka enqueue / Transport.Send | **88.7%** |
| Produce | 3.2% |
| Protobuf format | 3.1% |
| Producer metrics | 2.1% |
| Decode | 1.4% |
| Wrapper metrics | 1.2% |
| Other pipeline work | 0.3% |

At 21:00 CEST:

| Metric | Value |
|---|---:|
| Actual socket-received datagrams/s | 157,611 |
| Pipeline completions/s | 134,183 |
| Application drops/s | 23,428 |
| Queue length | 999,995 / 1,000,000 |
| Process CPU | 15.6 cores |

Receive minus completion rate almost equals the drop rate with a full queue. Completed throughput stays around 134–135k datagrams/s while input rises. Enqueue time corresponds to roughly 43 occupied workers by sampled scaling.

Other observations:

- Mean queue wait about **7.45 seconds**, consistent with queue length / drain rate.
- GC CPU about 0.39 cores at the later peak sample; mark assists about 0.012 cores.
- Runtime mutex-wait metric about 0.70 aggregate goroutine-seconds/s; this metric does not account for every runtime-internal channel-lock cost.
- Runnable scheduling p99 about 81 microseconds.
- Sarama records encoded about 728k/s, requests about 8.6/s across six brokers, about 85k records per request.
- Client-observed request latency around 25 ms mean / 30 ms p99 at the late sample. This includes a different boundary than broker-side request timing.
- Bond transmit about 130 MB/s versus reported aggregate bond speed 2.5 GB/s; physical links report 1.25 GB/s each. This does not show NIC line-rate saturation. It does not independently exclude remote shaping.
- Receive sockets are unevenly loaded; five were idle throughout the queried range. The shared queue still supplies all workers, so this is not itself proof of the main processing limit.

## 9. Captured profile evidence

Capture directory on server:

```text
/home/goflow/peaks/peak-20260923T074919Z/
```

Operator supplied a local copy at:

```text
/Users/user/Work/Code/goflow2/peak-20260923T074919Z/
```

Raw captures are not committed: they include operational addresses, stacks and system data. Retain the exact deployed binary for symbolization. Analysis used the official diagnostic artifact for d55116d, with checksums verified.

Retained bundles (all September 23; roughly 30 seconds each; metadata reports zero capture errors):

| Bundle | UTC start | CEST start | Queue before → after |
|---|---|---|---:|
| `01-baseline` | 07:49:19 | 09:49:19 | 366 → 33 |
| `03-queue` | 15:41:25 | 17:41:25 | 903,376 → 967,612 |
| `10-queue` | 19:11:47 | 21:11:47 | 999,999 → 999,995 |
| `11-queue` | 19:41:50 | 21:41:50 | 1,000,000 → 1,000,000 |
| `12-periodic-baseline` | 20:11:53 | 22:11:53 | 1,000,000 → 999,997 |
| `13-queue` | 20:41:56 | 22:41:56 | 999,977 → 1,000,000 |

The periodic-baseline label in bundle 12 does **not** mean low load; its queue is full.

### Worker channel waits

In `10-queue/goroutine.pb`, 44 of the 48 collector workers are in `KafkaDriver.Send`. The text snapshot also shows many workers in `[chan send]` at `transport/kafka/kafka.go:318`:

```go
d.producer.Input() <- &sarama.ProducerMessage{...}
```

Thirty-second block profiles attribute the following aggregate estimated delay to this call path:

| Bundle | KafkaDriver.Send blocked goroutine-seconds |
|---|---:|
| `01-baseline` | 270.69 |
| `10-queue` | 768.32 |
| `13-queue` | 763.72 |

768 seconds over 30 seconds corresponds to about 25.6 goroutines parked on average. This is not inconsistent with ~43 worker-equivalents of enqueue wall time: wall time also includes execution, scheduling and other synchronization, whereas the sampled block profile measures parked waits.

### Sarama dispatch work

In `10-queue/cpu.pb` (30.13 seconds wall time; 453.99 total sampled CPU-seconds):

| Function | Cumulative CPU-seconds |
|---|---:|
| `(*asyncProducer).dispatcher` | 23.01 |
| `(*topicProducer).dispatch` | 21.91 |

Source-line attribution in Sarama v1.38.1 `async_producer.go`:

- Producer dispatcher line 394, receiving from `p.input`: **14.97 CPU-seconds**.
- Producer dispatcher line 464, sending to the topic handler: **5.09 CPU-seconds**.
- Topic dispatcher line 499, receiving input: **4.29 CPU-seconds**.
- Topic dispatcher line 513, sending to partition handlers: **13.38 CPU-seconds**.

These are CPU costs of those paths, not blocked-duration measurements. Each dispatcher is a single goroutine, so approximately 23 CPU-seconds in a 30-second interval is substantial activity in a serial stage even while total host CPU remains low.

The corresponding block profile shows only about 1.18 seconds of receive wait for the producer dispatcher and 2.09 seconds for the topic dispatcher; it does not show large parked downstream-send waits in those two functions in that capture.

Sarama creates an unbuffered `input` channel at `async_producer.go:126` and starts its single producer dispatcher at line 136. A topic has one dispatcher (`newTopicProducer`, lines 484–495).

### Downstream partition handlers

The peak goroutine snapshot contains about 100 partition-handler goroutines. In `13-queue/block.pb`, their aggregate delay is approximately:

- **2,558.20 seconds receiving messages**.
- **22.53 seconds sending downstream**.

Large receive-wait totals here mostly mean idle consumers waiting for upstream input, not a blocked broker. Do not interpret the largest entry in an unfiltered block profile as the performance problem without identifying the wait direction.

### Other evidence

- Peak CPU samples include substantial channel, runtime lock, wakeup and scheduler work.
- Peak mutex profile is dominated by runtime/channel-internal locks, not Prometheus locks. Mutex profiling attributes delay to unlock paths; it should not be interpreted as a precise per-application-lock occupancy metric.
- GC/assist-focused CPU samples account for about 2.3% of peak sampled CPU.
- Six peak/baseline socket snapshots show seven Kafka TCP connections. All have zero Send-Q except one socket in bundle 12, with about 1.27 MB queued and a 4 MiB send-buffer limit. This does not establish persistent full send buffers.
- Kernel TCP retransmissions are only 1–4 across each 30-second capture. No UDP receive-buffer errors increase within those intervals.
- Voluntary per-thread switches rise from about 166k/s at baseline to 252–266k/s under queue pressure. Involuntary switches remain small.
- No OS thread is continuously at 100% CPU. This does not exclude a serial Go goroutine limit because goroutines can migrate between threads.

Example analysis commands, using the matching artifact binary:

```sh
go tool pprof -top -nodecount=25 "$BINARY" "$CAPTURES/10-queue/block.pb"
go tool pprof -top -cum -focus=sarama "$BINARY" "$CAPTURES/10-queue/cpu.pb"
go tool pprof -top -cum "$BINARY" "$CAPTURES/10-queue/goroutine.pb"
go tool pprof -top "$BINARY" "$CAPTURES/10-queue/mutex.pb"
```

## 10. Current diagnosis and confidence

**Confirmed observations:** application receive queue saturation/loss; dominant Kafka enqueue stage; many workers blocked at the unbuffered producer input; substantial serialized dispatcher/channel work; downstream partition handlers mostly waiting for input.

**Strongest interpretation:** the shared Sarama client dispatch/handoff path limits sustained processing capacity. This is more specific than “Kafka is slow”: the evidence is inside the collector process before partition handlers are kept busy.

**Not proved:** one unique runtime change or bug responsible for the version CPU difference; the exact gain from producer sharding; complete absence of brief network stalls or remote traffic shaping. A further controlled change is needed to validate the remedy.

Metrics and profiles make total host CPU saturation, protocol errors, sustained kernel receive loss, decoding, protobuf work, shared Prometheus summaries, and GC much weaker candidates for the primary observed peak limit. Some still consume resources and may become relevant after the dispatch limit is removed.

## 11. Producer-pool experiment: design and implementation

This means **two Sarama `AsyncProducer` instances**, not two Kafka brokers, not two topics, and not increasing GoFlow decoding workers.

Current:

```text
24 UDP sockets → shared queue → 48 workers → one AsyncProducer → existing brokers/topic
```

Proposed:

```text
24 UDP sockets → shared queue → 48 workers → producer selection
                                            ├─ AsyncProducer A ─┐
                                            └─ AsyncProducer B ─┴→ existing brokers/topic
```

Each instance owns its own producer and topic dispatch goroutines. Distributing messages between them removes the requirement that all ~730k messages/s pass through one instance's shared dispatch stages. Each flow message is submitted to **one selected producer**, not copied to both. This is a routing rule, not an exactly-once Kafka delivery guarantee; existing retry and acknowledgement semantics remain unchanged.

The captured d55116d baseline binary does not support this setting. The new `perf/508-producer-pool` branch implements `-transport.kafka.producers=2`, with a default of one so the experiment can switch back without changing binaries. Positive counts are supported. Nonempty keys use stable producer affinity when partition hashing is enabled; the supplied configuration uses round-robin producer selection.

Implemented design:

1. Refactor `KafkaDriver` to own a bounded slice of independently initialized producers with identical existing Kafka options.
2. Select one producer per message, using atomic round-robin selection without a shared hot-path mutex, or deterministic FNV-1a affinity for nonempty keys when hashing is enabled. Single-producer sends bypass the selector.
3. Give each instance its own Sarama client/registry and result-channel drainer, so dispatch and client-level locks are not inadvertently shared.
4. Add a `producer` label to producer-specific diagnostics. Aggregate rates/counts correctly in dashboards; do not add reservoir quantiles or average them into a fake global percentile.
5. Preserve terminal error accounting. Close/drain every producer on shutdown, and clean up already-created instances if a later initialization fails.
6. Test concurrent routing, exactly-one submission, failure cleanup, shutdown and metric registration. Build with the same Go version and dependencies.

Tradeoffs to measure: more connections, goroutines and internal producer buffers; possibly different request batch sizes and latency. Two producers do not guarantee twice the throughput. Independent partitioners/producers can also change relative ordering; the current round-robin, concurrent workload has no asserted end-to-end ordering requirement, but ordering requirements must be checked before adopting this as a general solution. Key-affine routing would be needed if preserving per-key producer affinity matters.

Controlled comparison:

- Same diagnostic binary with one producer versus two producers.
- Same 24 sockets, 48 workers, one-million-datagram receive queue, topic, brokers, compression, flush settings and acknowledgement requirements.
- Compare actual received/processed datagrams, drops, queue depth/wait, enqueue share, per-producer rates, CPU/memory and profile stacks under comparable input.
- Success means higher sustained completed throughput and lower queue/drop rates, not merely lower CPU or a temporarily empty queue after restart.
- Receiver capacity is unchanged, but total internal Kafka buffering increases; allow the system to reach steady state before drawing a conclusion.

CI for the producer-pool branch caught a pre-existing UDP shutdown/restart data race between `UDPReceiver.init()` replacing `r.q` and the socket watcher reading it. It was reproduced locally with a rapid-restart race test and corrected by capturing the session stop channel before starting the watcher. Steady-state processing and receive-queue policy are unchanged. This lifecycle fix is not claimed to explain the observed peak throughput limit.

Alternatives discussed but not selected as the first targeted experiment:

- Multiple independent collector processes sharing UDP 9801 via SO_REUSEPORT. This separates all shared resources, but hash-based receive distribution can be uneven and complicates comparison.
- More decoding workers: deferred because it can add senders waiting at the same producer handoff.
- Additional buffering: can absorb bursts or change handoff costs, but does not automatically raise sustained dispatch capacity.
- A different/newer Kafka client: a broader change to consider if a focused producer-instance experiment is insufficient.

## 12. Capacity and overflow alert candidates

**Deferred at the operator's request.** These are proposed read-only queries/rules, **not deployed alerts**. Return to this section after the producer-pool capacity experiment. Distinguish capacity headroom from buffer occupancy.

### Early warning: input approaches measured sustained capacity

The diagnostic build's sustained plateau with this traffic mix is about **134,000 UDP datagrams/s**. Use successful receive counts, before application enqueue/drop, divided by that empirical capacity:

```promql
sum by (instance, listener) (
  rate(goflow_diagnostics_socket_datagrams_total{
    instance="<collector>:8081",
    listener="sflow://:9801"
  }[5m])
) / 134000 > 0.80
```

Suggested pending period: **10 minutes**. This warns at about **107,200 datagrams/s**. A warning before the evening queue starts growing is intentional.

This is an **empirical capacity estimate**, not a measured utilization register. It depends on datagram sample counts/record sizes, mappings, build, hardware and producer configuration. Recalibrate it after a change such as producer sharding. A variable traffic mix can change work per datagram; also compare flow-message rates and stage costs. Do not apply this one collector's constant unconditionally to every instance.

Do not use `goflow2_flow_traffic_packets_total` as true arrival rate: it is updated after dequeue. It can flatten at processing capacity while excess received packets are dropped. The new socket metric counts before dispatch.

### Urgent backlog warning: queue occupancy

```promql
goflow_diagnostics_queue_length{
  instance="<collector>:8081",listener="sflow://:9801"
}
/
goflow_diagnostics_queue_capacity{
  instance="<collector>:8081",listener="sflow://:9801"
} > 0.80
```

An 80%-full queue means 800,000 queued datagrams, **not 80% of processing capacity**. Processing has already fallen behind. Consider a 30–60-second pending period, or no pending period if the operational priority is to catch short bursts. The measured peak deficit near 23k datagrams/s can consume the remaining 200k slots in about **9 seconds**, so a queue alert with a long pending period cannot reliably precede loss. Keep the early rate warning as a separate alert.

### Actual loss

```promql
sum by (instance) (
  rate(goflow2_flow_dropped_packets_total{instance="<collector>:8081"}[1m])
) > 0
```

Use according to the acceptable loss policy; do not suppress sustained loss behind an overly long pending interval. Standard series may be absent before the first drop. Keep scrape-health (`up`) alerts separate so missing telemetry is not interpreted as healthy zero loss.

The 90% queue threshold in `capture-peak.py` is only a **profile-capture trigger**. It does not set Grafana/Prometheus alert thresholds. Busy-worker and enqueue-time metrics provide supporting diagnosis, but are not a universal capacity percentage: waiting workers and bursty scrape-time states can be high even when input remains below the sustained throughput ceiling.

## 13. Open follow-ups

- [x] Implement the requested producer-count flag and independent producer pool on `perf/508-producer-pool`.
- [ ] Validate one versus two producers under comparable production load; implementation alone does not establish the gain.
- [ ] Resume alert design later, using section 12 and a newly measured capacity estimate.
- [ ] Extend the **queue-wait histogram bucket range**: its current largest finite bucket is 4.194304 seconds, below the observed 7.4–7.5-second queue wait. Current queue-wait p99 is capped and misleading. The mean from `_sum/_count` remains usable.
- [ ] Preserve raw captures and the exact d55116d binary outside this public source repository.
- [ ] If a later test changes worker count, update the older dashboard panel's fixed 48-worker reference; use the new actual worker-state metric in preference to the constant.
- [ ] If stages/profiles implicate kernel networking later, add TCP_INFO/qdisc or Linux perf evidence; saved `ss` output currently has socket memory but not full TCP_INFO.
- [ ] Validate producer throughput/delivery separately: records encoded into requests can include retries; successes notifications remain off in the low-overhead baseline.

The producer-pool branch changes collector capability, not the server's running configuration. Deploying the new binary and selecting two producers is a separate operator action. No worker-count, broker-setting, or alert-rule change has been applied to production by this work.
