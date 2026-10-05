# Latency SLI shadow prototype (NBS-7885)

Ticket: https://st.yandex-team.ru/NBS-7885

This prototype implements the evaluator, a versioned diagnostic contract,
per-volume read/write counters and the external-vhost receiver. It is disabled
by default and does not connect any counters to SLOs or alerts.

## Scope and remaining integration

Native NBD, embedded vhost and server requests are finalized through
`TServerStats::RequestCompleted`. The shadow boundary is `RequestStarted` to
response readiness (`ResponseSent` when available, otherwise the final
completion callback). An independent clock and atomic finalization flag keep
the original operation separate from legacy processing-stage accounting.
NBD/vhost thresholds use original byte lengths before alignment.
The shared finalization state is owned by `TMetricRequest` in
`metric_request.h`; copies share that state so completion is counted once.
The branch's existing incomplete-request API and mount/access accounting remain
unchanged.
Cell-forwarded requests are omitted at the receiving server to avoid counting
the same originating request again.

**Quota producers and graph merging are deliberately still unimplemented.**
Without an explicitly complete graph, native operations become Unknown,
including failures. Existing `Postponed`, `Backoff`, `Shaping` and
`ExecutionTime` values are not evidence of quota provenance. The new response
header is a transport contract; automatic response-header consumption is not
enabled until producers, retry handling and split/unaligned request mergers
can supply a graph for the entire originating operation.

Before using Good/Total for an SLO, instrument original-profile IOPS, bandwidth
and profile burst separately from storage-pressure and temporarily reduced
limits. Compose child/retry graphs at their owning operation and publish once
through `TCallContext::SetLatencyDiagnostics`. Preserve all service work,
retry delays, queue gaps and launch dependencies. Flatten nested graphs into
non-overlapping pieces of work/wait on each chain; do not also include enclosing
durations. Memory is bounded to 4096 nodes and 16384 dependency edges per
evaluation; larger or malformed graphs become Unknown.

The graph contains explicit presence for version, completeness, producer
duration, exclusion origin and node timings. Every quota node also requires
an original-profile reason. A node starts after its dependencies plus its
observed launch gap. Replaying those dependencies with only quota-node
durations set to zero preserves parallel critical paths. Time outside the
producer boundary and the response tail remains charged to the service.

## Configuration and counters

`EnableLatency` enables shadow accounting. Removing it disables accounting
and receiver updates. Restart/remount to apply configuration and counter
registration consistently; this prototype does not provide dynamic reloading.

`LatencyThresholdVersion` and `LatencyThresholds` specify a fixed table keyed
by media kind, read/write and half-open original-byte intervals. Thresholds
are in microseconds. Empty, incomplete, overlapping or unversioned tables
produce Unknown. There are no invented defaults and no automatic adjustment.
Initial thresholds must be derived from the ticket's historical-histogram
procedure, with independent validation periods and a reviewed table version.
This prototype has no historical data access or threshold-generation job.

The existing volume/instance/cloud/folder/type tree gains read/write
`LatencyGoodOps`, `LatencyTotalOps`, `LatencyUnknownOps` and separate counters
for invalid client requests, confirmed client cancellation and confirmed
client limits. All operation counters are cumulative derivative counters.
Raw final errors are used independently of legacy error suppression. Equal
latency and threshold is Good only for a successful operation. Generic
throttling/cancellation error codes never establish an exclusion.

Use window deltas: `Good / Total`, and `Total / (Total + Unknown)` for coverage.
Zero denominators have no defined ratio. Aggregate by summing counters first.
Exclusions are outside both denominators. These completed-operation counters
do not detect hung requests or replace availability and telemetry health.

## External vhost supplement

The legacy batch contract and its delivery are unchanged. An optional root
`latency` object carries `version`, `threshold_version`, `generation`,
`sequence`, `captured_at_us`, and `read`/`write` objects. Each direction has
six mandatory cumulative fields: `good`, `bad`, `unknown`,
`invalid_client_request`, `client_cancellation`, and `client_limit`.
These count original completed operations since producer-generation start.
The producer must use the identical reviewed threshold table, count uncovered
sizes/media as Unknown, and apply the same completeness rules to every outcome.

Generation must increase durably across producer restarts, and sequence must
increase once per snapshot. Both directions belong to one atomic snapshot.
The receiver computes deltas, rejects decreasing counters, rejects replayed
generations/sequences, and checks freshness using `LatencyBatchMaxAgeMs`
(default 30000). Sequence gaps, first contact, stale snapshots and
incompatible versions charge recoverable cumulative deltas to Unknown.
Their high-water mark is consumed so re-delivery cannot credit Good later.
Producer and receiver clocks must be synchronized; future timestamps cannot
credit Good. Version/table changes within a generation are conservative.

Per-volume `LatencyTelemetryMissingBatches`, `LatencyTelemetryInvalidBatches`,
`LatencyTelemetryDuplicateBatches`, and `LatencyTelemetryUnknownBatches`
report receiver problems. Missing legacy metadata has no trustworthy operation
identity and is represented by telemetry health rather than fabricated
operation counts. A coverage ratio alone must not declare such a source healthy.

The external producer in `cloud/contrib/vhost` is not modified. Receiver
high-water marks live for the endpoint stats object's lifetime, so receiver
restart/recreation establishes a conservative Unknown baseline before trusting
subsequent snapshots. Persistence/receiver handoff and producer instrumentation are
required before production rollout. Unobserved operations lost across a
producer restart require independent telemetry-health accounting.

No test cases are included in this prototype.
