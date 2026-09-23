# Kafka producer-pool experiment — issue 508

This build implements `-transport.kafka.producers` on top of the existing
v2.2.6 diagnostic build, retaining Go 1.25.5 and all module versions.

- [Investigation and evidence](performance-investigation-508.md)
- [Technical decision](adr/0001-independent-kafka-producers.md)
- [Profiling and capture instructions](diagnostics-508.md)

## Start with two producers

Use `goflow2-508-producer-pool-linux-amd64` in place of the current diagnostic
binary. Verify the artifact checksum before deployment:

```sh
sha256sum -c SHA256SUMS
chmod +x goflow2-508-producer-pool-linux-amd64
./goflow2-508-producer-pool-linux-amd64 -v
```

Keep the current arguments and add:

```text
-transport.kafka.producers=2
```

Specifically preserve `-listen 'sflow://:9801?count=24'`, binary formatting, the
same topic/brokers/TLS/SASL environment, and the existing diagnostic flags:

```text
-diagnostics=true
-diagnostics.sample-every=1024
-diagnostics.kafka=true
-diagnostics.pprof.addr=127.0.0.1:6060
-diagnostics.pprof.mutex-fraction=1000
-diagnostics.pprof.block-rate=10000000
```

The default is one producer. Values below one are rejected. Values such as two
and four are supported; begin with two. With the supplied round-robin configuration,
messages alternate between producers. Each producer has an independent input
channel, dispatcher and client connections, but both publish to the existing
topic. Messages are not copied to both producers.

When `-transport.kafka.hashing=true` and a key is nonempty, equal keys select the
same producer. Empty keys use round-robin selection. This does not add an
end-to-end ordering or exactly-once-delivery guarantee.

The first producer must be initialized before the next. If a member fails to
initialize, already created members are shut down and the collector fails startup
rather than silently using fewer producers. Shutdown initiates flushing on all
members before waiting for completion and returns collected shutdown errors.

## Confirm the deployment

After restarting GoFlow, check the existing `/metrics` endpoint for:

```text
goflow2_kafka_producers 2
```

Kafka metric families now have `producer="0"` and `producer="1"` labels, including
single-producer mode (`producer="0"`). Check that both have outgoing records:

```promql
sum by (instance, producer) (
  rate(goflow2_kafka_records_sent_total{broker="all"}[5m])
)
```

Scope this query to the collector instance in a shared Prometheus installation.
Counts are records encoded in requests, including retries, not acknowledgements.
Success notifications remain opt-in because they add per-message overhead.

Per-instance aggregate throughput:

```promql
sum by (instance) (
  rate(goflow2_kafka_records_sent_total{broker="all"}[5m])
)
```

For latency, batch size and records/request, display `producer`, `broker` and
`statistic` separately. Do not average reservoir percentile values into an
apparent global percentile or sum `broker="all"` with individual broker metrics.

## Capture a comparable peak

The existing capture script and storage settings remain compatible. After the
GoFlow restart, stop the old capture process and restart it with the new GoFlow
PID and a **new output directory under `/home`**. Preserve the old captures and
their matching d55116d binary for comparison. Keep this new binary with the new
profiles; build addresses can change even when most source is unchanged.

Evaluate after the queue has had time to settle:

1. Actual received datagrams/s and pipeline completions/s.
2. Application drop rate, receive-queue depth and mean queue waiting time.
3. Sampled Kafka enqueue share and estimated stage occupancy.
4. Per-producer throughput and request latency; CPU, heap and connection count.
5. Block/goroutine profiles for continued shared-input contention.

Success means higher sustained processing throughput and fewer drops under
comparable input. Lower drops immediately after restart are not sufficient:
the empty receive queue and additional internal producer buffering can hide
unchanged capacity temporarily.

To compare one versus two producers on the **same binary**, set
`-transport.kafka.producers=1` and repeat at comparable load. No receiver change
is needed. Do not assume two instances double capacity; an additional limit may
become visible.

## Deferred work

Alert design is explicitly deferred. Proposed capacity, backlog and loss alerts
remain in section 12 of the investigation record; none was created or enabled.
The old 134k-datagrams/s capacity estimate must be recalibrated after this test.

The queue-wait p99 histogram range remains too short for the previous multi-second
backlog. Use mean queue wait for comparison until that separate measurement issue
is corrected.

Existing initialization error-handling gaps (ignored SRV lookup errors and a
possible nil PEM block on invalid CA input) predate this experiment and remain
outside this change. They do not explain the measured steady-state bottleneck.
