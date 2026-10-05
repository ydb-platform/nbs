# Latency SLI shadow implementation (NBS-7885)

Ticket: https://st.yandex-team.ru/NBS-7885

The feature measures original completed reads and writes. It is disabled by
`EnableLatency` by default and has no connection to SLOs or alerts. All existing
`ExecutionTime`, `Postponed`, `Backoff`, `Shaping`, error classification and
limiter decisions retain their existing paths.

## 1. Start one original operation

NBD and embedded vhost register the original operation in `TMetricRequest`.
`LatencyRequestState` stores an independent cycle clock, the original byte
length before alignment/splitting, and an atomic completion flag. Server and
cell forwarding may produce diagnostics but do not register another original
operation. External vhost counts its original `vhd_io` once at final completion,
including the final completion of compound AIO requests.

The boundary is NBS endpoint acceptance to final response readiness. For NBD,
this starts after the request header is decoded and includes payload acquisition
and endpoint queueing. `ResponseSent` is the existing response-readiness mark,
not acknowledgement by the VM. Embedded and external vhost stop their clocks
in the final completion callback before returning the response to vhost. This
is not application latency within the VM or network round-trip latency.

## 2. Record work and confirmed quota waits

`service/latency.*` owns an operation-local graph. Nodes carry start/duration,
SERVICE or QUOTA kind, and dependency indexes. A graph explicitly identifies
its version, completeness, duration and exclusion origin. Missing metadata
from an old peer is not equivalent to a zero wait.

At the volume, `TLatencyVolumeRequest` belongs to the request event, rather than
the shared legacy call-context clocks. This avoids mixing parallel fragments.
The volume finishes its graph on normal responses, early errors and cancellation
by the service. Storage-local, RDMA-device, SPDK and null leaves emit complete
SERVICE graphs because no purchased disk-profile limiter exists below those
leaf interfaces.

`volume_throttling_policy` maintains an independent diagnostic bucket using the
original disk performance profile's IOPS, bandwidth and burst/boost. It never
uses the temporary throttling-rule coefficients or storage backpressure
multiplier for that bucket. Actual admissions charge the diagnostic budget;
postponed decisions preview the original quota's wait. The removable interval
is bounded by both the real postponement and that quota wait. FIFO followers
receive only their overlap with these confirmed head-of-queue quota intervals.
Permission closes the interval; late wakeup/dispatch time remains SERVICE.

`PROFILE_LIMIT` denotes the combined original-profile budget, not a separate
measurement for each of IOPS, bandwidth and burst. Intervals that overlap are
unioned before graph construction. A policy reset invalidates outstanding
waiters rather than reusing stale quota evidence. Temporary-rule resets preserve
the independent original-profile budget. A fresh original-profile budget starts
with full original burst/boost credit, conservatively retaining waits whose
pre-restart original quota history cannot be established.

Media shaping happens after the volume graph finishes and remains in the outer
SERVICE tail. Storage-pressure throttling, temporary quota reductions, internal
queues, dependency waits, transport, retries/backoff and processing after quota
permission remain charged to the service. Merely being in a throttler does not
make an interval removable.

## 3. Compose splits, dependencies and retries

The durable client records each attempt before its retry decision. The final
response carries all attempts in sequence, retaining the observed backoff gaps.
Large aligned requests join sequential chunks. Read-modify-write joins its read
and write sequentially. The split service and compound storage join children
in parallel, preserving their launch offsets and nested graphs.

Each producer's local graph is shifted into its parent's time coordinate. Time
outside the child's producer boundary becomes SERVICE; enclosing durations
are not also added as work. Replay sets QUOTA-node durations to zero, then
recalculates the same dependency graph with the observed non-quota launch gaps.
For A=80 ms quota+10 ms work alongside B=60 ms work, adjusted latency is 60 ms.
Summing and subtracting waits from the wall time would incorrectly yield 10 ms.

The graph is bounded to 4096 nodes and 16384 edges. Missing/invalid child graphs,
invalid clocks, unsupported versions, policy changes during a wait, oversized
graphs and early parallel completion with incomplete children yield Unknown.
Incomplete data applies equally to success and failure. Some exceptional paths
without a complete graph intentionally remain Unknown rather than inventing
zero quota or an exclusion.

## 4. Classify the final result

`diagnostics/latency_sli.*` uses a table validated once at startup. Lookup is by
media kind, read/write, and the original request's half-open byte range.

- Good: complete supported measurement, a valid threshold, final success and
  adjusted latency less than or equal to the threshold.
- Bad: the same data requirements, and either slow success or a service error.
- Unknown: unavailable threshold, measurement, compatible version or provenance.
- Excluded: the same completeness gate and a separately confirmed client cause.

NBD's explicit write-to-checkpoint rejection records an invalid-client-request
origin without registering a legacy I/O request. Generic E_ARGUMENT,
E_CANCELLED or throttling error codes do not imply a client cause. Internal
normalization errors and service shutdown remain service failures when their
measurement is complete. The contract has distinct client-cancellation and
client-limit origins; paths without explicit origin evidence never set them.
Waiting for quota and eventually succeeding does not exclude an operation.

## 5. Export counters and external-vhost batches

The existing disk/instance/cloud/folder/type counter hierarchy gets separate
read and write `LatencyGoodOps`, `LatencyTotalOps` (Good+Bad), `LatencyUnknownOps`,
`LatencyExcludedInvalidRequestOps`, `LatencyExcludedClientCancellationOps` and
`LatencyExcludedClientLimitOps`. Counters are cumulative derivatives.
`LatencyThresholdVersion` identifies the configured table and
`LatencyThresholdTableValid` reports structural validity.

External vhost receives the selected media's versioned table from its parent
through `NBS_VHOST_LATENCY_CONFIG_V1`. Both processes use the same evaluator.
AIO/null report complete SERVICE timing; RDMA carries the composed response
graph. The original six legacy batch fields/histograms and interval deltas stay
unchanged. An optional root `latency` object adds version, threshold_version,
generation, sequence, captured_at_us and read/write cumulative outcome counts.

The producer increments `<socket>.latency-generation` under a file lock and
fsyncs it and its directory before emitting that generation. Failure produces
an invalid generation and cannot credit Good. The receiver checkpoints its
accepted high-water mark to `<socket>.latency-state` through an atomic rename
and fsync. Files require the service identity and persistent endpoint storage;
keep them across process restarts and do not copy them between endpoints.

The receiver checks generation, sequence, time, table version, counter
monotonicity and signed counter capacity. Replays do not add counts. First
contact, restarts, sequence gaps, stale snapshots and incompatible versions
convert recoverable cumulative deltas to Unknown. They still advance the
checkpoint, preventing later delivery from turning them into Good. A corrupt
or unwritable checkpoint fails collection closed. Producer and receiver clocks
must be synchronized; future samples cannot credit Good.

Missing, invalid, duplicate and Unknown batches have separate per-volume
`LatencyTelemetry*Batches` counters. `LatencyExternalTelemetryReady` is an
external-source gauge: only a valid contiguous fresh sample establishes
readiness; missing/invalid/Unknown samples clear it. Fresh duplicate deliveries
do not establish readiness. Polling retries after timeouts or malformed lines.
`LatencyBatchMaxAgeMs` defaults to 30000. A stopped producer/collector must also
be detected using monitoring scrape freshness and process/endpoint availability.

Checkpoint persistence precedes counter publication to prevent replay credit.
A receiver crash in that small interval can lose publication; this is not a
transactional metrics journal. Likewise, operations completed after the last
producer snapshot can be lost on a producer crash. Restart/telemetry health
must invalidate such observation windows; do not infer perfect coverage from
operation counters alone. Endpoint migration/reset is a new conservative
baseline. No diagnostic failure changes an I/O response or a limiter decision.

## 6. Derive and configure fixed thresholds

`derive_latency_thresholds.py` consumes exported per-bucket ExecutionTime
counts. Its module docstring defines the JSON input. It requires a 28–30-day
base period, a non-overlapping validation period with matching service paths
and bucket boundaries, existing non-overlapping size ranges, an explicit
version, minimum observation count and reviewer-selected latency ceiling.
It selects the smallest finite bucket boundary covering at least 99.9% of
base observations. It reports independent validation coverage without raising
a threshold to fit validation data. +Inf, insufficient data or an unacceptable
candidate cause an error; percentiles are never averaged.

Invoke it with `--base`, `--validation`, `--version`, `--min-observations`,
`--max-threshold-us` and a new `--output` file. The output contains the report
and a candidate textproto with EnableLatency=false. Review sample sufficiency,
service-path comparability, validation results and delay acceptability before
using the table. This helper neither queries monitoring nor deploys a table.
Historical exports and approved numerical thresholds are not bundled with the
implementation. ExecutionTime supplies an initial norm, not reconstruction of
the new SLI. Initial p99.9 does not set the SLO target to 99.9%.

Enable `EnableLatency` consistently on the endpoint/agent and serving volume
components; use the same reviewed `LatencyThresholdVersion` and
`LatencyThresholds` table. Restart/remount to apply configuration; dynamic table
reload is not implemented. An empty, overlapping, incomplete or unversioned
table produces Unknown. Clear EnableLatency and restart/remount to disable.

Use window deltas: SLI=Good/Total, Coverage=Total/(Total+Unknown). Exclusions are
outside both denominators. A zero denominator is undefined. For aggregation,
sum counters before dividing. Shadow rollout must inspect each media type's
coverage, telemetry freshness and availability independently. Completed-operation
latency never replaces availability, hung-request or collection-health checks.

No test cases were added or run for this implementation at the user's request.
