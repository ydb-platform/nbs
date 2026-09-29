# Compressed Merged blobs: implementation plan and release gates

NBS-7823. This document is the implementation's version of the plan referenced
by the ticket. It does not claim to reproduce the unavailable historical draft.
The source revision and dirty-tree fingerprint must accompany every result.

## Scope and defaults

Partition v1 only. Both direct Merged writes and Mixed-to-Merged compaction use
the same helper. Fresh, Mixed, and compaction results published as Mixed retain
the existing raw representation. Reader support is unconditional.

Defaults:

| Setting | Default |
|---|---:|
| CompactionMergedBlobCompressionPercentage | 0 |
| DirectMergedBlobCompressionPercentage | 0 |
| MergedBlobCompressionCodec | lz4 |
| MergedBlobCompressionChunkSize | 32768 |
| MergedBlobCompressionMinSavingsPercentage | 10 |

The MergedBlobCompression feature requires an explicit matching whitelist and
no matching blacklist. Generic cloud/folder probabilities must not bypass that
requirement. Each path then samples a stable FNV-1a hash of disk id, commit id,
ordinal, and path. Invalid percentages, codec, or chunk settings disable new
selection. Codec is a string configuration item; numeric values may be changed
through the existing immediate control board.

Writer disable does not disable reading, confirmation, recovery, cleanup, or
GC. An operation already selected finishes with its retained decision.

## Durable format

One BlobStorage object holds concatenated independent raw LZ4 chunks, normally
32768 logical bytes each; the last chunk can be shorter. There is no decoded
chunk cache. A blob is entirely encoded or entirely legacy raw.

TBlobCompression v1 records version, codec, logical size, chunk size, block size,
encoded chunk lengths, and checksums of the encoded chunks. Encoded checksums
are mandatory even when optional logical block checksums are disabled.
The reader validates the complete table and all arithmetic before allocating
buffers or issuing reads. Logical size is bounded by 128 MiB. Unknown, empty,
malformed, inconsistent, or truncated descriptors fail closed.

The descriptor is stored in MergedBlocksIndex and in TBlobMeta by the same
AddBlobs transaction. A read cross-checks presence and serialized descriptors
in both copies, together with range/skip metadata. Empty and absent fields are
distinct. The UnconfirmedBlobs record preserves the final physical BlobId,
descriptor, and logical checksums across confirmation and recovery.

The savings test is:

    saved_bytes = raw_payload - (encoded_payload + extra_durable_metadata)
    extra_durable_metadata =
        descriptor.ByteSizeLong()
        + encoded_TBlobMeta.ByteSizeLong() - raw_TBlobMeta.ByteSizeLong()

This includes both durable descriptor copies and enclosing protobuf framing.
The temporary UnconfirmedBlobs copy is excluded. The accepted total must save
at least the configured percentage and the encoded payload must be smaller
than raw. The BlobId size is the actual payload size; its channel and other
identity fields are preserved after the initial allocation.

Logical ranges, skip masks, blob offsets, and optional checksums use complete
uncompressed blocks. CleanupQueue persists logical block count separately from
physical bytes, with legacy rows using the prior physical-size accounting.
Index verification reports inconsistent copies.

## Read and maintenance paths

ReadBlob plans and deduplicates complete chunks needed by logical block
offsets. It checks encoded checksums and exact decoded lengths, gathers into a
private output buffer, and publishes to the guarded destination only after
the entire response validates. Optional checksums are calculated on complete
logical blocks, including 64/128 KiB blocks spanning multiple chunks.

Compaction disables Patch for every selected compression attempt and every
compressed or inconsistent source. Raw fallback does not restore Patch.
Compaction reads source blocks through the same decoder and rewrites them.

ScanDisk validates the complete descriptor and availability of the first
logical block. For 64/128 KiB blocks this requires multiple chunks. It is not a
full scrub. Requests in a scan batch are sequential to bound admission.

DescribeBlocks requests advertise SupportedBlobFormatVersion. Responses
acknowledge the weakest supported format across contributing partitions.
Each blob piece carries the descriptor and full logical blob length.
Striped splitting forwards capability; merging copies complete piece metadata
before translating only ranges. An old consumer receives an explicit error
for compressed pieces. A new base-disk consumer without an acknowledgement
retries raw-only, so an old intermediary cannot silently strip descriptors.
ExecuteAction describeblocks requires an explicit version from a new tool.
VolumeProxy forwards the full protobuf for local and interconnect routes.

## Admission and accounting

Four separate process-wide pools isolate foreground/background encode/decode.
Each foreground pool has four slots and a 512 MiB reservation limit; each
background pool has two slots and a 256 MiB limit. Total reservation ceiling is
1536 MiB, in addition to ordinary NBS buffers. This is a prototype default,
not an experimentally approved production budget.

Writers rejected by admission retain raw data; readers return a retryable
error. There is no unbounded compression work queue. RAII reservations live
with actor-owned buffers. No detached task survives actor destruction.
Synchronous codec work is bounded by the per-blob logical-size limit.

MergedBlobCompression counters distinguish attempts, accepted blobs, raw
fallback, admission rejection, logical/payload/metadata bytes, thread CPU,
decoded chunks, read bytes, and errors. Acceptance means an encoding decision,
not a committed durable write. Storage inventory after matching cleanup/GC
is authoritative for space savings. BlobCompressionRate remains sampling of
raw input and never samples already encoded payload.

ReadPhysicalBytes measures requested BlobStorage payload, not the complete
network protocol. Full-path experiments must separately capture network and
process boundaries. CPU counters include helper-side copying/checksum work;
they do not replace complete NBS/YDB process CPU measurements.

## Reader-first deployment and downgrade fence

AllowedNodeIDs alone is insufficient: external tablet boot and fallback from
boot-information backup do not enforce the Hive placement filter. Hive tablet
migration may also omit that filter. Do not use a new tablet protobuf flag as
a downgrade guard.

Use an isolated, enumerated compatible host cohort. Its service entry point
must enforce an immutable reader-binary allowlist before starting NBS. The
reader_fence launcher hashes an open ELF inode, checks a root-owned policy and
host membership, and executes that same inode. The launcher, interpreter,
policy, service unit, binaries, and parent directories must be protected from
the service account. Service-manager and deployment permissions must prevent
an alternate direct entry point. Network/node-registration admission must
exclude hosts outside the cohort. Apply the same rule to local-mount,
external-boot, disaster-recovery/fallback hosts and every relevant cell.

Example policy (replace with measured digests and complete host inventory):

    {
      "format": 1,
      "minimum_blob_reader_version": 1,
      "cohort_id": "approved-reader-cohort",
      "allowed_hosts": ["reader-host-1"],
      "reader_binary_sha256": ["<64 lowercase hex characters>"]
    }

Configure the existing service manager to invoke the protected launcher with
--policy and --binary plus the existing NBS arguments. This document and the
launcher do not install that configuration or claim that it is already active.

Before enabling any writer, retain these deployment proofs:

1. Every eligible host/consumer runs the approved reader; all start/restart,
   failover, local mount and fallback entry points use the fence.
2. An actual previous NBS binary from before this change is refused before
   startup on an isolated host; the approved binary starts through the same
   entry point. Also verify an unlisted host and a replaced binary path fail.
3. Tablet relocation and node restart cannot reach an unlisted host.
   Inter-cell/base/overlay readers either preserve the descriptor or refuse.
4. Deployment rollback cannot remove the launcher/policy or lower the reader
   floor. The protected policy remains after writer disable and after reboots.

Then enable the whitelist with both shares zero, followed by a small compaction
share. Expand compaction only after conservation, CPU/latency/memory budgets
and mixed-format reads pass; direct Merged writes come last. To stop writing,
set both shares to zero. Do not downgrade readers. No bulk data migration is
required. Rolling back source code is not evidence of format compatibility.

## Experiments and evidence

The native benchmark compares LZ4, Snappy, Zstd fast/1/3 and historical FastLZ
at 16/32/64/128/256 KiB and whole 4 MiB. Only LZ4/32 KiB is persistent v1 and
that candidate calls the production helper. Experimental codecs/chunks are
never accepted by the production format validator.

Use the micro runner with a release binary, a pinned physical CPU and idle
sibling, working sets larger than LLC, at least ten repeats, and all corpus
classes: random, repeated, code/VM, anonymized working Merged data. Preserve
origin, sampling period/rule, exact offset/size traces, hashes, compiler/flags,
hardware and raw operation rows. Working data must be owner-only artifacts.
A VM smoke run is useful for correctness but cannot satisfy the physical-core
performance gate. Repetition percentiles are not user-latency percentiles.

The full-path runner operates on an explicitly acknowledged isolated disk.
It neither creates/destroys disks nor changes server configuration. Use the
same revision, corpus, hardware, cache policy, volume state and cleanup/GC
boundary for baseline, enabled and mixed runs. Capture:

- 4096-byte writes reaching Mixed, continuous compaction and finite-series
  drain; foreground CPU, background CPU, and total CPU.
- 4194304-byte direct Merged writes and actual blob splitting.
- Sequential/random 4096-byte and 4194304-byte reads, boundary-crossing and
  multi-piece traces, alone and with direct writes/continuous compaction.
- Actual per-operation p50/p95/p99, requested/achieved IOPS and MiB/s, errors,
  queue delay, logical/physical bytes, decoded chunks, CPU seconds per
  operation/GiB, average busy cores, network bytes and peak held memory.
- Fraction of reads reaching compressed Merged, accepted/fallback fractions
  by count and raw bytes, raw/payload/two-copy metadata totals by corpus.

The runner saves DescribeBlocks inventories, process/counter/network samples,
and individual operation timestamps. Process restart invalidates a performance
run. A finite Mixed series must finish its configured compaction drain before
CPU accounting ends. CPU boundaries and idle/background baseline must be
reported explicitly; no process-wide CPU total is labelled codec-only.

A successful generator exit is not a release decision. Keep writers disabled
outside the isolated experiment until data-integrity tests, all required
corpora/CPU classes, concurrent workloads, deployment negative tests, and
agreed operational budgets pass. No positive performance conclusion or budget
is implied by this implementation plan.

## Admission telemetry and measurement tools

The four process-wide foreground/background encode/decode pools export ReservedBytes, ActiveOperations, their peaks and limits, admission attempts/rejections/wait time, and QueuedOperations=0. Writer reservation includes 3*logical bytes plus 1 MiB of per-blob workspace; reads reserve logical output, physical payload, 1 MiB workspace and bounded query/scatter/checksum vectors before allocation. Per-disk counters separate foreground/background read bytes, decode CPU/chunks, raw Merged reads, accepted logical bytes and raw-fallback bytes. DecodeErrors counts failed compressed read requests; FormatErrors identifies descriptor/payload/response validation failures. Neither counter is a current on-disk inventory.

See cloud/blockstore/tools/testing/merged_blob_compression/README.md for reproducible input generation, the 36-variant codec runner, native two-copy inventory, full-path workload manifests and matched baseline/enabled/mixed reports. These tools retain negative or incomplete evidence and do not declare a release gate passed.
