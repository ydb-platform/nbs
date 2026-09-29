# Merged blob compression experiments

Build the product and the tools from the same recorded source snapshot:

```sh
./ya make --build=release --ignore-recurses --no-src-links -j16 -o /absolute/artifacts/release cloud/blockstore/apps/server cloud/blockstore/tools/testing/merged_blob_compression/bench cloud/blockstore/tools/testing/merged_blob_compression/workload
```

Keep the exact command, compiler identity, binary hashes, source diff/untracked
hashes, configuration, CPU model/affinity, storage placement and cache preparation
with each series. Debug functional tests are not performance measurements.

## Codec matrix

The native `bench/merged-blob-bench` executable accepts
`CORPUS READS.tsv CODEC CHUNK_BYTES REPEATS`. It reads whole 4 MiB blobs (the last
blob may be shorter and must be 4 KiB aligned), validates every decoded user byte,
and writes one TSV row per encode/read operation. CPU and wall time are in
nanoseconds. The LZ4/32 KiB candidate uses the production format helpers. Other
codec/chunk combinations are isolated experiments and must not be persisted by
NBS. Storage cost includes both durable descriptor copies, the enclosing
protobuf tag/length, and whole-blob raw fallback.

`prepare_inputs.py` produces reproducible random/repeated smoke corpora and
sequential/random 4 KiB/4 MiB traces, including 4 MiB reads crossing chunk and blob
boundaries. Its required binary/source/compiler/build-command arguments bind
the generated manifest to a recorded release build:

```sh
python3 cloud/blockstore/tools/testing/merged_blob_compression/prepare_inputs.py --output /absolute/artifacts/inputs --binary /absolute/artifacts/release/cloud/blockstore/tools/testing/merged_blob_compression/bench/merged-blob-bench --source-fingerprint RECORDED_FINGERPRINT --compiler RECORDED_COMPILER --build-command RECORDED_BUILD_COMMAND
python3 cloud/blockstore/tools/testing/merged_blob_compression/micro_runner.py /absolute/artifacts/inputs/micro-smoke.json --binary /absolute/artifacts/release/cloud/blockstore/tools/testing/merged_blob_compression/bench/merged-blob-bench --cpu 2 --smoke --output /absolute/artifacts/micro-smoke
python3 cloud/blockstore/tools/testing/merged_blob_compression/report.py --micro /absolute/artifacts/micro-smoke --output /absolute/artifacts/micro-report
```

For a release experiment remove `--smoke`, set at least ten repeats, and supply
all four corpus classes: random, repeated, code/VM and anonymized working Merged
blobs. Working sets must exceed LLC. Record corpus origin, period, selection rule,
hashes and the real read trace used for the working corpus. The runner requires a
physical core, pins affinity and checks that sibling CPU busy time stays below
1%. Repeat on every production CPU class. Synthetic inputs and this KVM host
cannot substitute for these gates.

## Full NBS path

Start with an isolated, prefilled disk and immutable expected corpus. Use the same
product binary in baseline/enabled/mixed runs. The runner mounts the named disk;
write phases overwrite it. It does not create/delete disks or change server
configuration. Fill every placeholder in `examples/full-path.json`, including the
actual monitored process IDs, effective config expectations, cache/hardware
identity and exact counter selectors from the recorded endpoint response.

```sh
/absolute/artifacts/release/cloud/blockstore/tools/testing/merged_blob_compression/workload/merged-blob-workload /absolute/artifacts/baseline.json --allow-write-disk-id ISOLATED_DISK --output /absolute/artifacts/baseline-0
```

Use distinct immutable output directories for each repetition and mode.
Writer percentages are zero for baseline, enabled independently for direct and
compaction profiles, and recorded explicitly for mixed data. Keep the allowlist
and all other configuration fixed. Capture effective per-disk config before and
after, not just a file hash. The runner checks these responses, process executable
hashes, process lifetime, network namespaces and CPU affinity.

Run write-4k, write-4m, read and concurrent profiles. For reads use separate
sequential/random 4 KiB/4 MiB traces and boundary traces from
`read-profiles.json`, then replace the synthetic trace with the recorded real
trace for working-data measurements. Concurrent reads compare against the same
immutable corpus used for writes. Prefill the entire working set before measuring
reads. Confirm actual Fresh/Mixed/Merged routes using counters and DescribeBlocks.

For finite write-4k/concurrent series, the drain action invokes `compactrange`,
takes its returned OperationId and polls `getcompactionstatus` to completion.
CPU accounting continues through this drain. Configure flush/cleanup and wait
for the independently recorded Fresh, compaction and GC completion conditions;
compactrange completion alone does not prove GC completion. For steady state
measure continuous compaction over a sufficiently long interval as well.

Raw operation records include service latency, arrival-to-completion latency,
queue delay, errors, size and offsets. Samples retain process CPU/RSS/HWM,
application metrics, and interface counters for each process network namespace.
An idle CPU interval is saved separately; negative noise after subtraction is
retained. Select the complete NBS/YDB process boundary. NIC bytes describe all
traffic in that namespace; they cannot alone identify one disk or prove a
reduction across the full client/NBS/BlobStorage route.

## Inventory and comparison

```sh
/absolute/artifacts/release/cloud/blockstore/tools/testing/merged_blob_compression/bench/merged-blob-bench --inventory /absolute/artifacts/enabled-0/after-describe.json 4096
python3 cloud/blockstore/tools/testing/merged_blob_compression/report.py --full /absolute/artifacts/baseline-0 /absolute/artifacts/enabled-0 /absolute/artifacts/mixed-0 --inventory-binary /absolute/artifacts/release/cloud/blockstore/tools/testing/merged_blob_compression/bench/merged-blob-bench --output /absolute/artifacts/full-report
```

Inventory validates capability acknowledgement, whole descriptors, ranges and
physical BlobId sizes, deduplicates blob pieces and rejects conflicting copies.
It counts complete live referenced blobs; hidden checkpoint and garbage blobs
are outside this API's scope. A post-GC space claim requires independent GC
proof and a controlled disk without checkpoints. Do not treat cumulative
compression-attempt counters as current disk occupancy.

The report rejects mismatched source/binary/corpus/load/hardware/cache identities
or non-compression effective config changes. It records each repetition and
absolute/delta CPU, p50/p95/p99 latency, throughput, queue and memory values.
Counter selectors must match exactly one sensor; omitted or ambiguous evidence
must be corrected before drawing conclusions. Report compressed-read fraction,
raw fallback costs, decoded chunks, physical bytes and full-route network bytes.

No tool declares rollout safe. Review measurements and choose CPU/latency/memory
budgets with headroom. Default writer shares remain zero until release gates and
the protected reader-cohort deployment described in
`doc/blockstore/storage/partition_compression_plan.md` are proven.

`CompatibilityRejections` counts protocol rejection boundaries for legacy DescribeBlocks providers and consumers. A single request may cross more than one boundary; this is not a unique-request count.
