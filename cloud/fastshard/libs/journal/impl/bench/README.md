# journalled-device-bench

Benchmark of the journalled device. It builds the device stack in the
process and calls `ReadPages` and `WriteLogRecord` right on
`IJournalledDevice`, so the numbers do not include the network, the protocol
or a client. Use `fastshard-loadtest` with `fastshard-jdserver` for end-to-end
numbers.

```bash
./ya make -r cloud/fastshard/libs/journal/impl/bench
```

Always measure a release build (`-r`): a debug build is several times slower.

## Devices

* `--device v2` (default): the journalled device.
* `--device v1`: the device without a journal. A log record is written right
  to the data area, so this is the baseline: the price of one device write or
  read. Only the `write` and `read-data` workloads make sense for it.

## Workloads

| `--workload` | Request | State of the device |
|---|---|---|
| `write` | `WriteLogRecord` | empty; the watermark is advanced every `--advance-every` records, so the flusher runs during the test |
| `read-journal` | `ReadPages` | every page is in the journal |
| `read-data` | `ReadPages` | every page is in the data area, the journal is empty |
| `read-mixed` | `ReadPages` | the first half of every request is in the journal; with `--pages 1` every second page is |

Requests address a random slot of `--pages` pages inside `--working-set`.
`--iodepth` threads issue requests one after another until `--duration` or
`--requests` is reached.

## Typical runs

```bash
BENCH=cloud/fastshard/libs/journal/impl/bench/journalled-device-bench

# Latency: one 4K request in flight
$BENCH --workload write --pages 1 --iodepth 1 --duration 10 --spin

# IOPS: 4K requests at depth
$BENCH --workload write --pages 1 --iodepth 16 --duration 10

# Bandwidth: 1M requests
$BENCH --workload write --pages 256 --iodepth 4 --duration 10
$BENCH --workload read-data --pages 256 --iodepth 4 --duration 10

# The same on a file with O_DIRECT
$BENCH --backing file --device-path /path/to/bench.bin \
    --workload read-journal --pages 1 --iodepth 1 --duration 10
```

## Backings

* `--backing memory` (default): the pages live in the process memory. The
  result is the cost of the device code itself: CPU, locks and hand-offs
  between threads.
* `--backing file --device-path PATH`: the pages live in a file opened with
  `O_DIRECT`. **The benchmark overwrites the file.** It writes the whole file
  through before the run, so that the file system does not allocate blocks
  during the test. `--sparse` skips this step.

The device is split like in the server: `--log-meta-size`, `--log-data-size`,
`--data-size`.

## How to read the results

```
WriteLogRecord: 528256 ok, 0 retries, 175846.5 req/s, 686.90 MB/s, latency us: mean 86.3 p50 54.8 p90 60.9 p99 120.3 p99.9 5611.6 max 13450.0
```

* The latency is the time from the call to the moment the future gets its
  value. It is taken on the thread that completes the request. The request is
  built before the clock starts.
* Read the latency at `--iodepth 1`. At depth the latency is just
  iodepth / throughput.
* `retries` on `WriteLogRecord` are writes that the full journal rejected.
  They are repeated under the same lsn after a pause, and the latency
  includes the pauses. Many retries mean that the result shows the speed of
  the flusher and not the speed of the write path.
* For the write path alone, use `--advance-every 0 --requests N`, where N
  records fit into the journal: nothing is flushed during such a run.
* `--advance-every` counts records. With 1M records the default of 256 keeps
  up to 256M in the journal, so `--log-data-size` must be clearly larger.
* `--results FILE` writes the same numbers as JSON.

## Threads and cores

* `--executor-cores 2,3` pins the executor thread of the device. Use
  `taskset` for the rest of the process.
* `--spin` makes a client thread wait for the response in a busy loop. The
  device then does not spend time on waking the client up, which matters at
  `--iodepth 1` on the memory backing. It takes one core per request in
  flight.
* `--aio-threads` sets the number of AIO completion threads of the file
  backing.
