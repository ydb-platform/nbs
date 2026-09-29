# Merged blob compression: measured results

This report accompanies the partition-v1 prototype. Measurements were collected on 23–29 September 2026. It distinguishes the latest tested source from earlier experiments and preserves both successful and failed runs. Publication does not certify production rollout readiness.

## Source and environment

Latest tested source fingerprint: `ba7c82a1064ccf13753d036ba6481c4b5cce772b302f1db34a1049c8b75f8901`, based on `de73c57a091dcbc3a9330bfec0b9c6243d43ef24`. The source snapshot is included at `artifacts/acceptance/develop-3f210d1a-v2/snapshot.json` inside the archive. Publication preserves its code/test bytes and adds this measurement package; it is not a new test run.

Latest nbsd SHA-256: `5bdfefbd2a90277f0c85add895a5cb4205da41b568dd3dfeacf1316ff220e381`. Functional runs used a debug build on a 64-vCPU Intel Ice Lake KVM host, isolated local NBS/YDB with file-backed PDisks and loopback networking. Build flags, toolchains, source identities, configurations and commands are preserved in each run's plan/identity files. The earlier release microbenchmark has its own binary identity; do not combine its timing with debug full-path results.

## Latest functional verification

42 unit/actor tests passed with no failures or skips; complete nbsd linking passed. The 20 external scenarios passed, with 3,850 API actions, 756 recorded byte-equality checks, 12 metadata rejections, 12 CheckIndex rejections, 12 ScanDisk metadata rejections, 19 successful scans, and 6 exact failure-boundary hits. These are recorded event counts, not unique test counts.

[All scenario counts](functional-results.tsv) include malformed metadata (truncated, empty, unknown version/codec, copy mismatch, wrong logical size), direct-write payload/index/ack boundaries, compaction boundaries, checkpoints, 64/128-KiB blocks, striped and overlay volumes, cleanup/GC, configuration, saturation, concurrent IO/tablet death and corrupt compaction. The complete result inventory and per-scenario events are in `artifacts/acceptance/develop-a74a6e80/` and `artifacts/acceptance/develop-3f210d1a-v2/runs/`.

### Actual payload sizes on synthetic repeating data

| Writer path | Logical bytes | Encoded payload bytes | 32-KiB chunks | Payload-only reduction |
| --- | ---: | ---: | ---: | ---: |
| Direct 4-MiB Merged write | 4,194,304 | 17,792 | 128 | 99.5758% |
| 128 writes of 4 KiB, then Mixed-to-Merged compaction | 524,288 | 2,224 | 16 | 99.5758% |

These values come from the latest pilot's `describe-enabled.json` and `small-writes-after-compaction.json`. They measure payload, excluding both durable metadata copies and post-GC storage accounting. They are **not total storage savings** or a production-corpus estimate. The random-data case selected raw fallback and passed byte-equality checks.

### Concurrent workload on the latest source

Eight volumes completed 96 compactions, 384 direct writes and 630 successful reads with 138 expected rejected reads under backpressure. Foreground encode/decode admission rejections were 4/138; background encode/decode rejections were 1/20. Corresponding cumulative admission-wait counters were 679/1,217/132/138 microseconds. These cumulative counters are not client latency percentiles. The tablet-death and data-recovery oracle passed. Complete worker timings, metric time series, counter deltas and cleanup observations are retained in `concurrent-tablet-54f6c9d8/`.

## Coverage and older measurements

Latest selective instrumentation measured 100/107 changed executable lines (93.46%) in `part_database.cpp` relative to the base; that diff includes earlier feature lines. The cleanup regression ran, but `part_cleanup_logic.cpp` was outside the selective instrumentation. This is not coverage of the entire patch.

An earlier independent 84-test run on fingerprint `6cacf149875a163287e04396dd01043090862d8f1937316a454241b83f1677fc` measured 33 of 43 scoped paths: 7,175/10,702 unique executable lines (67.04%), 921/1,112 changed executable lines (82.82%), and 2,957/8,484 branch outcomes (34.85%). The read-blob guard covered 6/6 lines and 8/8 outcomes; changed lines in that file were 8/8. These figures remain historical and are not relabeled as current whole-patch coverage. Other historical reports are preserved under their original run directories.

[Microbenchmark samples](microbenchmark-samples.tsv) preserve all 1,728 extracted rows, with original artifact path, repeat, logical/payload/metadata bytes, acceptance, chunk count and CPU/wall nanoseconds. The matrix includes LZ4, Snappy, Zstd fast/1/3 and FastLZ with 16/32/64/128/256-KiB and whole-4-MiB chunks. Only LZ4/32-KiB is the prototype's persistent format. Smoke timings, small synthetic inputs, older revisions and repeat-level statistics must not be presented as production CPU cost or per-operation client latency percentiles.

Historical failures remain in the archive. In particular, an older concurrent oracle incorrectly required every output to remain compressed despite valid admission raw fallback; its corrected run is separate. Metadata parsing failures on older source are not evidence of failure on the latest corrected source.

## Complete measured-data attachment

- [Raw measurements and provenance, tar.gz](measurements.tar.gz): 3,101 files, 568,082,801 uncompressed bytes; archive 20,306,839 bytes.
- [SHA-256 and size of each artifact](manifest.tsv).
- [Machine-readable bundle summary](bundle-summary.json).
- Archive SHA-256: `13fd2554957686a45ff6cc995840b3915496cde06f9824cdf0a6ee60a5fc449b`.

The archive retains original bytes and relative paths for recorded JSON/JSONL/CSV/TSV/XML results, monitoring samples, counters, test traces, available coverage exports/profdata and measurement-related output, along with plans and source/binary identities. It excludes build caches/binaries, raw corpus/pdisk contents, raw profiling directories, rendered HTML coverage and 60 compiler build-stat files. The attachment contains measured results, not a copy of the entire experiment filesystem. Archives were reread and every entry verified against the manifest.

Extract with `tar -xzf measurements.tar.gz`. Paths formerly rooted at the experiment directory map to `artifacts/`; consult the run's source and binary identities before comparing two files. Older experiments have different inputs and are not pooled into the latest results.

## Interpretation

The data demonstrates physical compression and the tested functional paths. A full release off/on/mixed comparison on a qualified working corpus with real offset/size traces, physical CPU classes, both durable metadata copies and matched cleanup/GC is not established by these debug/smoke runs. Protected cross-cell/eligible-cohort and actual-old-binary deployment checks remain separate. No end-to-end CPU improvement, production storage-saving percentage, client p50/p95/p99 improvement or rollout budget is inferred. Both writer percentages remain disabled by default.
