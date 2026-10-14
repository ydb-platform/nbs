# fastshard-loadtest

Load generator for the fastshard storage node protocol: the
`fastshard-client` counterpart for performance testing. Drives one device
with `WriteLogRecord` / `ReadPages` requests from a configurable number of
fibers and reports throughput and latency percentiles, in the same JSON
shape `filestore-loadtest` produces.

Only built in `OPENSOURCE` builds without `FORCE_FASTSHARD_IPC_STUB`:

```bash
./ya make cloud/fastshard/loadtest
```

## What a run does

1. `AcquireDevices` for `--device-uuid` with `--client-id` and
   `--generation` (skipped with `--no-acquire`).
2. `ReadJournalTail` to find the newest log sequence number the device
   holds, so written records continue the existing chain.
3. `--iodepth` fibers issue requests back to back until `--duration`
   elapses, `--requests` have been issued, or `SIGINT` / `SIGTERM`
   arrives (a second signal exits immediately). Each request is a
   `ReadPages` with probability `--read-percent`, otherwise a
   `WriteLogRecord`. Page groups start at a random page in
   `[0, --page-count)`. Every `--advance-every` written records the lsn
   low watermark is advanced to the newest one, so the journal on the
   storage node does not fill up.
4. A request that fails with a retriable error (`E_REJECTED` when the
   journal is full, `E_TIMEOUT`, ...) is repeated after a backoff growing
   from 1 ms to 100 ms, a write under the same lsn, until it succeeds or
   `--retry-timeout` elapses. Reads and watermark advances also give up
   when the run stops; a write keeps retrying past the end of the run,
   because a rejected record must not be skipped: once a later record
   lands, the records after the gap can never be chained, so the journal
   never flushes them and stays full.
5. `ReleaseDevices`.

## Options

 * `--host`, `--port` - storage node address; `--port` is **mandatory**
 * `--client-id`, `--request-timeout` - request headers
 * `--device-uuid` - device to load; **mandatory**
 * `--generation` - writer generation for `AcquireDevices`
 * `--no-acquire` - do not acquire / release the device around the run
 * `--iodepth` - requests in flight (default 1)
 * `--duration` - seconds to run; `--requests` - requests to issue; at
   least one is required, the run stops at whichever comes first
 * `--page-size` - logical page size (default 4096)
 * `--page-count` - the device page range requests address (default 1024)
 * `--write-pages`, `--read-pages` - pages per request (default 1)
 * `--read-percent` - share of reads, 0..100 (default 0, write-only)
 * `--advance-every` - watermark advance period in records (default 256,
   0 disables)
 * `--retry-timeout` - seconds to keep retrying a request that fails
   with a retriable error (default 30, 0 disables retries)
 * `--name` - test name in the results
 * `--results` - write the JSON results to a file instead of stdout
 * `--report-interval` - print progress to stderr every N seconds
 * `--verbose` - enable silk debug logging

## Output

A one-line summary per action goes to stderr:

```
WriteLogRecord: 48213 ok, 0 errors, 37 retries, 4821.3 req/s, 18.83 MB/s, latency us: mean 196 p50 189 p90 240 p99 412 max 2210
ReadPages: 48007 ok, 0 errors, 0 retries, 4800.7 req/s, 18.75 MB/s, latency us: mean 178 p50 171 p90 221 p99 380 max 1902
AdvanceLsnLowWatermark: 188 ok, 0 errors, 0 retries, 18.8 req/s, 0.00 MB/s, latency us: mean 124 p50 120 p90 150 p99 200 max 240
```

`ok` counts the requests that succeeded, `retries` the retriable failures
that were followed by another attempt. The latency of a retried request
is that of its last attempt alone, so backoffs do not show in the
percentiles, and `req/s` counts successes only: a run that spends its
time retrying shows up as a throughput drop and a high retry count.

The results go to stdout (or `--results`) as a `TTestStats` JSON
document: per action `Count`, `RequestBytes` and a `Latency` block with
`P50` .. `P999`, `Min`, `Max`, `Mean` and `StdDeviation` in microseconds.
The exit code is 0 when every request succeeded and 1 otherwise; the
first error per action is printed in the summary.

## Example

```bash
# 10 seconds of 4K writes and reads, 50/50, 16 in flight, against the
# journalled device served by a local disk agent (see FASTSHARD.md)
fastshard-loadtest --port 29900 --device-uuid $DEV --generation 1 \
    --iodepth 16 --duration 10 --read-percent 50 --page-count 65536 \
    --report-interval 1 --results results.json
```
