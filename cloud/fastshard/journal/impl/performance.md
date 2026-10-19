# Journalled device performance notes

Results of the performance investigation of the journalled device server
(`cloud/fastshard/journal`) made with `fastshard-jdserver` and
`fastshard-loadtest` in October 2026. Every number here comes from the
setup in the first section; absolute values are specific to that laptop
and that file-backed device, the comparisons between variants are what
carries over.

## Setup

* Server: `fastshard-jdserver` (`cloud/fastshard/jdserver`), one journalled
  device over a file on ext4 (NVMe behind LVM), parts 1G / 1G / 1G, 4K
  blocks, O_DIRECT via the AIO service. Release build with
  `-DFORCE_STATIC_LINKING -r`.
* Client: `fastshard-loadtest`, 4K pages, `--page-count 65536`, write only,
  one connection per request in flight (the sn client pools connections,
  so iodepth 16 means 16 TCP connections).
* Machine: Intel Core Ultra 7 165H (16 cores, 22 logical CPUs), Ubuntu.
  Server pinned with `taskset` to cores 2-3 (single executor runs) or
  2-9 (multi-executor runs), loadtest to core 5 or cores 10-11.
* Method: every comparison between two variants is an interleaved A/B,
  four alternating pairs of runs, same conditions, read at p50. Runs made
  minutes apart drift by up to 5 µs at iodepth 1, so non-interleaved
  numbers are not compared.
* Analysis tools: `strace -f -c` on the server to count syscalls per
  request (exact when nothing overlaps, a lower bound otherwise because
  pipe wakes coalesce), `perf stat -p` for CPU per request,
  `perf record -g` for profiles (`perf_event_paranoid=-1`,
  `kptr_restrict=0`, tracefs readable for scheduler events).

## Baseline and where the time goes

Release build, pinned, iodepth 1, sparse device file, p50 in µs:

| Request | p50 | Notes |
|---|---|---|
| Round trip without device I/O (watermark advance, no journal) | 27 | TCP over loopback, protobuf, coroutine listener, executor hop |
| Direct 4K write, no journal (V1 device) | 56 | one `io_submit`, one completion |
| Watermark advance, original code | 76 | one metadata entry write |
| Journalled record, original code | 107 | journal data write, then metadata entry write |

Pinning the server process to a core pair was worth about 8 µs and a
much tighter p90; pinning its two threads to separate cores helped
nothing and hurt the tail (at iodepth 1 the two threads never work at the
same time, so the cross-core wake costs more than sharing a core).

### Hand-offs, not CPU

Syscalls per request on the server (exact counts, no overlap):

| Request | io_submit | pipe write | epoll_wait | writev |
|---|---|---|---|---|
| Direct write | 1 | 1 | 2 | 2 |
| Journalled record, original | 2 | 4 | 5 | 2 |
| Watermark advance, original | 1 | 2 | 3 | 2 |

Every pipe write was a hand-off through the executor's dispatcher
(`TContLockFreeQueue` in `cloud/storage/core/libs/coroutine/queue.h`):
a byte written to a pipe that the coroutine engine's epoll loop polls,
after which the dispatcher creates a worker coroutine for the item.

* A same-thread hand-off (`Execute` from a coroutine already on the
  executor) costs a couple of µs and removing it alone measured flat.
* A cross-thread hand-off (AIO completion thread -> executor through
  `TExecutor::WaitForI`, which schedules a coroutine just to signal an
  event) costs 10-12 µs each. These were the whole gap: the record had
  two, the advance one.

CPU per request from `perf stat` (iodepth 1): direct write 32 µs, advance
28 µs, record 59 µs, with 3 context switches per request for the first
two and 5 for the record. The advance uses less CPU than the direct
write; it was slower only because of the hand-offs. Profiles are flat:
about 70% kernel (`io_submit` through the ext4 direct-IO path, `tcp_sendmsg`,
epoll); no user-space hot spot; the entry page CRC32C is below 0.1 µs.

## Changes and their effect

All at iodepth 1, pinned, sparse file, p50 in µs, each row an interleaved
A/B against the previous one.

### Watermark advance

| Step | p50 |
|---|---|
| Original | 76 |
| No `Execute` wrapper in `TJournalledDeviceV2::AdvanceLsnLowWatermark` | 70 |
| Continuation instead of `WaitFor` in `TJournal::AdvanceLsnLowWatermark` | 64 (flusher off) |
| Direct write for reference | 56-61 |

The continuation version released the single-advance guard too early
(the `Y_DEFER` ran when the future was returned); the guard is now released
inside the continuation. `ShouldRejectAConcurrentAdvanceOfTheLsnLowWatermark`
caught it as a timeout. Syscalls per advance are now identical to the
direct write (1 / 1 / 2 / 2); the remaining 2-3 µs are the key buffer
store's bookkeeping (page allocation, checked-mode validation, entry page
build, freeing the previous entry's page).

### WriteLogRecord

| Step | p50 | pipe / epoll per record |
|---|---|---|
| No `Execute` wrappers, future-returning `DoWriteLogRecord` | 105 | 3 / 4 |
| `TJournal::WriteLogRecord` as a continuation chain, one hop back to the executor for the chain/index tail | 94 | 2 / 3 |
| Tail on the completion thread under a new journal-level `ChainLock` (zero hops) | 89 | 1 / 2 |
| Response serialized on the completing thread, one socket write per response | 88 | 1 / 2 |

`ChainLock` is needed because the chain and the page index are safe call
by call but not sequence by sequence: finishing a record (mark ready,
index, complete promise), erasing flushed records and reading the tail
each span several calls that used to be atomic only because they all ran
on the one executor. With completions from several threads (threaded AIO
service, disk agent actor threads) two tails would race in
`IndexChainedRecords` and hit its `STORAGE_VERIFY`. Promises are completed
under the lock on purpose, so `CleanupFlushedRecords` can never fail a
promise the tail is about to set; the consequence is that promise
subscribers run under `ChainLock` and must not call back into the journal.

The remaining difference to the direct write is the second device write
itself: with both writes issued in parallel (not safe, see below) the
record measures 68 µs on a sparse file and 54 µs on a prewritten one.

### Out-of-order records

Writing lsn 2 before lsn 1 on one connection does not deadlock in the
current code or the original one: nothing waits for a record; the lsn 2
promise stays pending until lsn 1 lands, then both complete. The
intermediate version where `DoWriteLogRecord` blocked the receive
coroutine did deadlock on one connection. Nothing times out a held record
with a missing predecessor; a client at iodepth 1 that waits for lsn 2
before sending lsn 1 hangs itself by protocol.

## Throughput at depth

At iodepth N with the server saturated, mean latency is exactly
N / throughput (16 in flight at 49k req/s = 326 µs); latency at depth is
the inverse of throughput, not a property of the request path.

### Single executor

The executor thread (`JD`) is one OS thread running one `TContExecutor`
with everything on it: listener, all connection coroutines, journal
executor-side work, flusher. Its CPU per request sets the ceiling:
direct write 19 µs -> ~52k req/s, journalled record 21 µs -> ~48k.
Work moved off that thread shows up directly as throughput:

* Sequential writes beat parallel writes at depth with one executor
  (41k vs 35k) because the sequential version submits the second write
  from the completion thread.
* Serializing the response on the completing thread: +11% at depth,
  -3 µs at iodepth 1.
* Redirecting `Journal->Write` to a two-thread `CreateThreadPool`: +17%
  at depth when the pool threads have their own cores, -25% when they
  share two cores with the executor, +4 µs at iodepth 1 (the futex hop),
  +25% total CPU. Experiment only; not kept.

### Several executors

`TServerConfig::ConnectionExecutors` spreads connections round robin
over a list of executors; the listener and the device stay on the first
one. `fastshard-jdserver --executors N --aio-threads M`. Journalled
record, iodepth 16, server on 8 cores, 100000 requests per point:

| Executors | AIO threads | Sparse file | Prewritten file |
|---|---|---|---|
| 1 | 1 | 49k | 63k |
| 2 | 1 | 64k | 98k |
| 2 | 2 | 64k | 94k |
| 4 | 1 | 62k | 99k |
| 4 | 2 | 50k | 101k |
| 4 | 4 | 58k | 80k |
| 8 | 2 | 57k | - |

With parallel writes (unsafe variant) on the prewritten file: 56k / 92k /
90k / 111k / 87k / 87k for the first six rows. Best safe: 101k (4 / 2);
best overall: 111k (4 / 1, parallel). More than one AIO thread does not
help and hurts at four executors: the completion threads contend on
`ChainLock` and the store locks, and the round robin splits a record's
two completions across threads.

### The sparse-file artifact

A sparse device file makes ext4 allocate blocks on every direct write,
under the inode's write lock. With several executors all writers
serialize on it: the profile showed `rwsem_spin_on_owner`, `osq_lock` and
`rwsem_down_write_slowpath` as the top three symbols, 22% of all server
CPU. Writing the file through once (`dd ... oflag=direct`) before the run
removed it: 62k -> 103k at four executors, and 11-14 µs off every write at
iodepth 1 (record 89 -> 78, parallel 68 -> 54). All iodepth 1 numbers in
this document other than those marked prewritten include that cost,
including the 56-60 µs direct-write baseline. A raw block device, which
is what a disk agent serves, has no allocation and no such lock.

## Other findings

* **Flusher cost.** Each released record costs the flusher a read-back of
  its pages from the journal data region and a write into the data region,
  two device operations and two hand-offs; the superblock write in
  `CleanupFlushedRecords` happens once per 100 ms cycle, not per advance.
  The read-back is avoidable: the pages were in memory when the record
  was written.
* **Metadata part size.** A 16M metadata part fills within seconds at
  iodepth 16 with the watermark advanced every 256 records and triggers
  the full-store rejection described in `known-issues.md`; 1G never filled
  in any run. Restore of a fresh 1G part from a sparse file takes about
  a second.
* **Executor budget at depth, single executor, direct write** (~19 µs per
  request): response send via `writev` 28%, `io_submit` 21%, socket
  receive 10%, epoll and coroutine switching the rest. This is the
  protocol floor; the journal adds about 2 µs of executor time to it in
  the sequential form.
* **Debug builds** roughly double every latency (record 265 µs at iodepth
  1); only release builds were used for the tables above.

## Measurement pitfalls met

* Comparing runs made minutes apart: 5 µs drift at iodepth 1; use
  interleaved A/B.
* 5-second runs at depth write ~2 GB into a fresh sparse file and delete
  it; by the third pair ext4 is still freeing the previous files and both
  arms show 100-200 ms stalls. Use a fixed request count (60000-100000),
  `sync; sleep 4` between runs, alternate the order, or prewrite the file.
* Two loadtest processes against one device both read the same journal
  tail and write the same lsn sequence; the second one's writes are
  duplicates answered from the first one's promises without I/O, so the
  summed throughput is meaningless. With parallel writes the setup hung.
* `pkill -f` with a pattern that appears in the calling shell's command
  line kills the shell; a bare `wait` after `server &` waits for the
  server. Both cost real time here.
* The server's `SIGINT` handling (blocked everywhere, `sigwait` in the main
  thread) is fine; a server that "ignores" SIGINT was always a script that
  never sent it.

## State of the code (uncommitted at the time of writing)

* `cloud/fastshard/journal/impl/journal.cpp`: asynchronous
  `WriteLogRecord`, `OnLogRecordWritten`, `ChainLock`, advance guard
  released in the continuation.
* `cloud/fastshard/journal/server/server.{h,cpp}`,
  `builder.{h,cpp}`: connection executors, response serialization on the
  completing thread.
* `cloud/fastshard/jdserver`: the standalone server, `--executors`,
  `--aio-threads`.
* Experiments measured and reverted: parallel page and record writes;
  journal writes on a thread pool.

## Next steps

1. **Parallel page and record writes, made safe.** Store a CRC32C per
   page in the record (`SerializeRecord`, format version bump) and have
   the restore verify every restored record's pages, dropping a record
   whose pages do not match together with everything chained after it.
   Write-path cost ~0.3 µs per page; restore reads the journal data part
   once more. Then issue both writes together: -14 µs at iodepth 1, 111k
   at iodepth 16 with 4 executors and 1 AIO thread.
2. **Completion-side parallelism without lock contention**: one
   completion thread per executor with the journal state partitioned or
   its locks split, so that `--aio-threads` above one stops losing.
3. **Flusher read-back**: keep the record's page buffers until the flush
   (within a memory budget) instead of reading them back from the journal.
4. **`io_submit` batching**: the AIO service submits one iocb per call;
   two per call for the record's two writes saves ~2-3 µs of executor
   CPU, about 8% at depth.
5. **Device executor sharding** for several devices per server (disk
   agent): bind each device to one executor; the connection sharding
   above already supports it.
6. **jdserver `--prewrite`** (or a raw block device path) to take the
   allocation artifact out of every future measurement.
