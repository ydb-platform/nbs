# Blob block checksums

The scope is the existing replicated partition backed by BlobStorage: merged and
batched mixed writes, fresh blocks when flushed, reads of stored blobs and
compaction. Zero markers, fresh log reads, nonreplicated/mirrored DiskAgent I/O
and file-local storage do not gain a new integrity mechanism.

## Configuration

No new storage-service field or default is introduced. In storage service configuration:

```protobuf
DiskPrefixLengthWithBlockChecksumsInBlobs: 562949953421312
CheckBlockChecksumsInBlobsUponRead: true
```

The prefix is in **bytes per partition**. 562949953421312 is 512 TiB: it covers
all 2^32 addresses even with the maximum supported 128 KiB block size.
A smaller value limits the prefix; 1073741824 is the historical first GiB.
The previous conversion to a 32-bit block count wrapped at 16 TiB with 4 KiB
blocks. Boundary comparisons now retain the 64-bit quotient.

Set both fields to 0/false to disable checksum generation and read verification.
Setting only the read flag to false still allows compaction verification.
Defaults remain prefix=0, read=false; existing explicit configurations retain
their meaning. A request that starts below a partial-prefix boundary can cover
more blocks, as before.

Apply persistent settings through the existing storage config delivery and
restart the service/tablets using the new configuration. The existing Immediate
Control Board can override both fields for newly prepared operations without
reboot; in-flight actors finish with their captured settings. ICB overrides are
process-local and must not be mistaken for persistent configuration. Keep the
persistent configuration synchronized before a service restart.

At partition InitSchema, a nonzero checksum prefix selects a 512-byte limit
per embedded executor redo record and a 2 MiB admission budget for retained
embedded payload. Records that exceed either limit use the existing external
log representation; short records may still be embedded. Transaction batching
remains enabled. A zero prefix restores the historical 2048-byte record limit
and unlimited aggregate budget.

The aggregate budget includes replayed embedded records but excludes entry
overhead, transient compression/snapshot copies and whole-process memory.
Existing records retained before a smaller budget was applied are reclaimed
at normal snapshot edges, so 2 MiB is not an immediate resident-memory bound.
InitSchema restores the default 16 MiB LogOverheadSizeToSnapshot policy on
MergedBlocksIndex, BlobsIndex, CompactionMap, UsedBlocks and LogicalUsedBlocks,
including tablets that previously persisted the experimental 64 KiB policy.

ICB changes affect checksum processing immediately. Executor policies follow
the configuration at the next tablet initialization; apply persistent settings
and restart for a consistent policy.

## Existing data, restart and rollback

CRC32C and TBlobMeta.BlockChecksums are unchanged. Metadata is committed in the
partition's tablet Local DB; block payload remains in BlobStorage. No migration
of user data or change to the CRC encoding is introduced. The executor schema
log gains optional MaxRedoBytesToEmbed and MaxRedoBytesInSnapshot policies.
Their defaults are 2048 bytes and unlimited, respectively. Both are persisted
and restored independently of block metadata; absent settings preserve the
executor's historical defaults.

Previously stored nonzero sums continue to be checked. Missing sums and the
historical zero sentinel are **unverified**, never proof of integrity. The read
completion reports them separately through tablet cumulative counters
UserRead/ChecksumBlocksUnverified and UserRead/ChecksumBlocksVerified. These
count actually compared blob blocks, not fresh blocks, zero markers or a claim
that the whole disk has been verified. Counters reset on tablet restart.

Normal writes establish new sums; normal compaction now establishes a baseline
for blocks that had no sum. Establishing a baseline cannot discover corruption
that happened before that baseline. There is no full-disk scan on enable and no
claim that pre-existing untouched data has already been checked. Full coverage
of old populated ranges requires compaction of those ranges. Its existing
transaction/commit protocol makes interruption safe: after reboot a blob either
has the committed baseline or remains unverified until processed again.
Disabling and rewriting never reuses the old blob's checksum for new data.

A nonzero mismatch still emits BlockDigestMismatchInBlob and returns E_REJECTED;
read retries and existing corruption diagnostics remain in place. The historical
zero-checksum exception is unchanged.

The previous binary can read all resulting metadata and payload, since the
format and CRC are unchanged. Before binary rollback, restore a legacy prefix
that does not trigger its 32-bit boundary wrap (for example 1 GiB) and the
corresponding read setting. The checksum field, CRC and external-redo representation remain compatible;
the previous executor ignores the new optional embedding policies. Validate
rollout and rollback with reads and writes on the intended service topology.

## Cost and alternatives

* Embedded redo has a 512-byte individual limit and a 2 MiB retained-payload
  admission budget for checksum-enabled partitions, with transaction batching
  preserved. This limits admission of CRC-heavy metadata into repeated log
  snapshots; it does not reduce persistent CRC storage or eliminate all copies.
  Normal table compaction policies are retained. CPU, resident memory and I/O
  effects require the recorded service measurements, not a payload-size claim.
* Metadata is read/parsed once per blob per read transaction, instead of once per
  requested block. Transaction retries clear the cache.
* Contiguous immutable BlobStorage data is checksummed before copying to the
  caller. The caller's mutable destination is never used as the checksum source.
* Fragmented ropes use at most one block of reusable scratch per response.
* Read/write checksum vectors reserve the known number of blocks.
* Blob metadata reserves the known checksum count before protobuf insertion.
* CRC32C is retained: changing the checksum width or polynomial is unnecessary
  for these avoidable allocation/copy/parse costs and would add compatibility
  work. This is not a claim that all possible algorithms have been benchmarked.
* Persistent checksum storage is not reduced; parsed uint32 sums still require
  at least four bytes per protected block, plus protobuf/container/Local DB
  overhead. Avoided transient copies must not be advertised as an equal
  reduction in resident tablet memory.

## Component benchmark

`ya make -j64 --build=release --ignore-recurses cloud/blockstore/tools/testing/checksum-bench`
builds `checksum-bench`. Run each case in a new process:

```sh
checksum-bench optimized read 1048576 67108864 1
```

Arguments are mode, operation, request bytes, working-set bytes, duration in
seconds. Modes: off; prefix (first GiB with the original process); original
(full surface with per-block scratch and metadata parsing); scratch (reuse
scratch/reserve vectors but keep repeated metadata parsing); optimized
(contiguous source and one metadata parse per request). CRC32C is identical in
all enabled modes. This isolates the current contiguous read and write costs;
the real fragmented path is covered by the actor test. The benchmark deliberately
does not model the whole tablet or BlobStorage network. Its checksum array is a
measurement fixture, **not** the service's resident data structure.

Use 4 KiB random requests and 1 MiB sequential requests; repeat each mode and
operation at least three times for 16 MiB, 64 MiB and 2 GiB working sets. The
2 GiB case distinguishes prefix coverage. The timed loop includes checksum
comparison/write, copies and metadata parsing; source generation is outside it.
Initial checksum-array population CPU is reported separately. CSV includes
process CPU seconds, processed bytes, average cores, peak process RSS and the
actual serialized, constructed, parsed and reserved-construction checksum
increments of a 1024-block TBlobMeta.
First-fill versus steady overwrite and end-to-end tails require the disk test
below. Random addressing uses a deterministic permutation within the working
set, so all blocks remain reachable.

For logical bytes V and block size B, n=ceil(V/B). Decimal volumes are exactly
1,000,000,000, 1,000,000,000,000 and 256,000,000,000,000 bytes; divide by 2^30 or
2^40 to express GiB/TiB. A 4 KiB representation needs 244141, 244140625 and
62500000000 sums respectively (last partial block included). Four-byte payload
alone is 976564, 976562500 and 250000000000 bytes. It is not a RAM estimate.

If measured extra CPU is c CPU seconds per decimal GB, one full pass costs
c*V/1e9 CPU seconds; at R bytes/s it consumes c*R/1e9 average cores. Derive
separate c for calculation and verification by subtracting the off case under
the same workload. Include repeats and their range; do not infer CPU from
stored capacity alone. Extrapolate checksum metadata using the measured
serialized and parsed deltas per blob, with blob count, fragmentation,
checkpoints, overwritten generations, Local DB cache and compaction separately.
Persistent and peak service RAM require process measurements, not just those
payload coefficients.

## Required disk experiment

The component CSV is not acceptance evidence for disk performance. For a
BlobStorage-backed service, build baseline/current nbsd and the existing
`cloud/blockstore/tools/testing/loadtest` target, use isolated disks with the
same allocation and backend, and run these four modes:

| Binary | Prefix bytes | Read check |
| --- | ---: | --- |
| baseline | 0 | false |
| baseline | 1073741824 | true |
| baseline | disk/partition capacity below its 32-bit wrap | true |
| current | 562949953421312 | true |

Record exact revisions, binaries, configuration, backend topology, erasure,
CPU/RAM, block size, per-disk capacity, disk count and host/process placement.
Use at least 2 GiB logical disks for direct beyond-prefix comparisons. Measure
first fill, full read, overwrite and repeated read, both random 4 KiB
(iodepth 32) and sequential 1 MiB (iodepth 16); add mixed 70/30 read/write and
overlapping I/O as appropriate. Keep useful offered IOPS/bandwidth identical;
the historical write rates were 32000 IOPS and 450 MiB/s, not universal targets.
Include compaction and metadata-cache warmup/cooldown, and repeat with identical
data seeds at least three times. Capture client IOPS, bytes/s, p50/p95/p99/p99.9,
process CPU, RSS/peak RSS and queue depth through background drain. Compare
performance against the original first-GiB mode.

For resource extrapolation include several logical capacities and counts of
disks/processes. One 256 TB logical estate may use many disks; replicas and
service overhead are additional, not part of those 256 TB. Report baseline
absolute memory, incremental resident/peak memory, all cached/serialized
metadata, temporary buffers and fixed per-disk/process costs. Preserve raw
results. A noisy measurement or a microbenchmark speedup does not establish
absence of disk regression.

## Network driver

`run_network.py` runs the existing loadtest over gRPC against an already
configured BlobStorage-backed service. It creates unique new disks, preserves
native latency/IOPS results, samples all specified local server PIDs (CPU,
current/peak RSS and process identity), and observes background drain.
Run it separately for each pinned binary/configuration pair:

```sh
python3 run_network.py --label optimized --loadtest /path/to/blockstore-loadtest \
  --host 127.0.0.1 --port 9000 --server-pid 12345 \
  --storage-config /path/to/storage.txt --output /path/to/new-results --cleanup
```

The PID above is an example, not an established test instance. Multiple
`--server-pid` flags account for multiple local processes. The driver pins
process start times and binary/configuration hashes but does not claim the
given file is the service's effective configuration; verify that when starting
the test service. Native loadtest has no deterministic seed option. Record this
limitation and use repeated comparable profiles. Disk IDs are recorded before
creation; failed runs retain their newly created disks for diagnosis.
`--dry-run --label optimized --output /path/to/new-plan` generates all
textproto profiles without accessing a service. It is not an executed disk test.

## Recorded results

See [RESULTS.md](RESULTS.md) and [component-results.csv](component-results.csv)
for the September 2026 component runs, exact identity, raw values, extrapolation
and remaining network acceptance work.
