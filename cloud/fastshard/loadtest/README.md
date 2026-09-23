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
4. `ReleaseDevices`.

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
 * `--name` - test name in the results
 * `--results` - write the JSON results to a file instead of stdout
 * `--report-interval` - print progress to stderr every N seconds
 * `--verbose` - enable silk debug logging

## Output

A one-line summary per action goes to stderr:

```
WriteLogRecord: 48213 ok, 0 errors, 4821.3 req/s, 18.83 MB/s, latency us: p50 189 p90 240 p99 412 max 2210
ReadPages: 48007 ok, 0 errors, 4800.7 req/s, 18.75 MB/s, latency us: p50 171 p90 221 p99 380 max 1902
AdvanceLsnLowWatermark: 188 ok, 0 errors, 18.8 req/s, 0.00 MB/s, latency us: p50 120 p90 150 p99 200 max 240
```

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
