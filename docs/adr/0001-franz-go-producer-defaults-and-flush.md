# Franz-go producer defaults and shutdown flushing

## Status

Accepted for the Kafka client migration on GoFlow2 v2.2.7.

## Context

The Kafka transport previously used Sarama settings. A single franz-go client
with its 10,000-record default buffer blocked processing workers under high
load. The operator requested a 100,000-record default, an adjustable limit,
current franz-go defaults, and timeout handling while retaining no compression.
GoFlow has no durable fallback for undelivered Kafka records.

## Decision

Use franz-go v1.22.1 and Go 1.26, with idempotent production, all-in-sync-replica
acknowledgements, adaptive keyed partitioning, and automatic API negotiation.
Preserve explicit version and linger overrides, and allow operators to ignore
keys. Keep compression disabled and do not enable automatic topic creation.

Default the client-wide buffered-record limit to 100,000 and expose it as both
a flag and a Prometheus gauge. Retain blocking Produce; terminal record failures
and unforwarded error notifications are counted separately from UDP drops.

Bound startup SRV lookup and Ping by a configurable 30-second budget. Flush in
five-second windows on shutdown, with optional overall and no-progress deadlines.
Leave both shutdown deadlines disabled by default to prioritize delivery.
When a configured deadline expires, return an error identifying outstanding
records before closing the client. Wait for all callbacks before closing the
error channel. Callers must stop Send before closing the transport.

## Consequences

- Go 1.26 is required by the current franz-go release.
- Client buffering, partition distribution, acknowledgement latency, and retry
  behavior differ from Sarama and require a comparable production-load test.
- Additional memory absorbs bursts; it does not establish higher drain capacity.
- Indefinite shutdown can wait during broker outages. Configured deadlines or
  external process termination can lose records; there is no recovery store.
- Callback failure metrics do not imply exactly-once delivery across restarts.
- Startup cancellation is bounded independently of the command's later runtime
  signal context; the existing transport interface has no Init context parameter.
