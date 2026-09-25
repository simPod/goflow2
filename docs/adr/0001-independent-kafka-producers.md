# Independent Kafka producer instances

## Status

Accepted for the issue 508 experiment. Production throughput benefit remains to
be measured. Implemented on `perf/508-producer-pool`, based on the v2.2.6
diagnostic branch.

## Context

[The investigation](../performance-investigation-508.md) found that approximately
89% of sampled handler wall time is spent submitting messages to one Sarama
producer. Peak stacks show most receiver workers blocked at its unbuffered input.
The producer/topic dispatchers spend substantial CPU on serial channel handoffs,
while partition handlers mostly wait for input.

The operator wants an experiment that keeps one process, UDP port 9801, the same
receive queue and worker count, and the existing Kafka protocol/configuration.
Simply adding receiver workers does not split the shared dispatch path.

## Decision

Add `-transport.kafka.producers=N`, requiring a positive integer and defaulting to
one. Initialize N independent Sarama asynchronous producers, each with its own
client, configuration, metric registry and result-channel drainer.

For multiple producers, route submissions round-robin through an atomic index
unless partition hashing is enabled with a nonempty key. In that case, use a
deterministic FNV-1a hash of the key for producer affinity. Route each message to
one instance; do not duplicate messages across instances. Single-producer sends
bypass the atomic selector. Existing retries and acknowledgement semantics stay
unchanged; this does not introduce exactly-once delivery or new ordering guarantees.

Give producer diagnostics a bounded `producer` label (`0` through `N-1`) and
export `goflow2_kafka_producers`. Preserve individual reservoir statistics rather
than presenting an invalid aggregate percentile. Label single-producer metrics
too, so the schema is consistent across experiments.

On partial initialization failure, close previously initialized members and
remove their metrics. On normal shutdown, initiate all producer shutdowns before
waiting for their result channels to drain. Preserve the existing application
lifecycle contract: stop Send callers before Close. Do not add a shared hot-path
mutex to support concurrent shutdown that the application does not use.

Keep Go 1.25.5 and existing module versions pinned. Do not tune flush frequency,
compression, receiver workers, queue capacity, or Kafka acknowledgement settings
as part of this experiment.

## Consequences

- The single producer/topic dispatch path is divided among independent instances.
  Whether this raises sustained throughput is an experimental question.
- Connections, goroutines, metadata activity and internal buffering increase.
  A fresh receive queue can hide overload temporarily, so evaluate steady state.
- Round-robin producer selection adds one atomic operation per message when N>1.
  Key-affine selection costs a hash; N=1 uses neither.
- Independent producers can change cross-producer ordering and batch formation.
  Affinity keeps a nonempty key on one producer when hashing is enabled, but does
  not establish ordering among concurrent callers or across restarts/count changes.
- Diagnostics and dashboard legends must distinguish producer instances. Sum
  appropriate counters/rates, not percentile gauges or aggregate-plus-broker rows.
- The default remains one producer, allowing a one-versus-two comparison with the
  same binary. The initial deployment recommendation is N=2.
