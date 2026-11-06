# Snapshot backup acceptance stand (NBS-7923)

The stand exercises the public DM API against local NBS, YDB and two independent
S3 endpoints. It changes tests and the opt-in recipe only. The feature code from
PRs #7175 and #7222 is not modified. Ordinary recipes keep backup disabled.

## Run

From the repository root, on a Linux development host with the repository's
normal ya toolchain and test dependencies:

```sh
./ya make cloud/disk_manager/internal/pkg/facade/backup_service_test --build=debug -j64 -ttt
./ya make cloud/disk_manager/internal/pkg/facade/backup_disabled_test --build=debug -j64 -ttt
```

Use `--test-filter=*TestName*` to run only a scenario whose inputs changed.
Each test process owns its recipe processes, ports, S3 buckets and YDB database.
The large target uses four chunks, 8 CPUs, 24 GiB RAM and 200 GiB disk per chunk;
allow the scheduler to account for aggregate resources. The recipe starts five
DiskAgents for SSD NRD. Do not point fault proxies at production endpoints.

The PR CI matrix selects cloud/disk_manager for DM changes. Large tests require
the existing `large-tests` label (see .github/scripts/pr_build_and_test_matrix_plan.py).
Compiling the target alone does not execute it or prove the fault matrix.

## Matrix and oracle

| Test | Property |
| --- | --- |
| TestBackupRestoreSSD | Zero/full/incremental snapshots, overwrite/zero/inherited chunks, source and base deletion, both copies |
| TestBackupRestoreNRD | Zero/full/changed SSD NRD generations and both copies; current DR path takes full snapshots |
| TestBackupTemporaryFailuresSSD | Checkpoint/read barriers, primary write, follower metadata/source read/chunk/map failures, accepted PUT with lost reply |
| TestBackupPermanentFailureAndCancelSSD | Fixed deadline under permanent source outage, explicit cancellation, irreversible checkpoint-create error, next successful request |
| TestBackupDeleteAndResizeRacesSSD | Source deletion before checkpoint and during copy; resize before checkpoint and concurrent with copy |
| TestBackupNRDCheckpointFailure | Transient I/O recovery and irreversible checkpoint loss, no false success, next successful NRD request |
| TestBackupSlowSourceAndSnapshotWritesSSD | Measured 10x slower reads; snapshot writes below both configured limits, overlap with live writes and checkpoint oracle |
| TestBackupPublicIdempotencyAndInvalidSources | Lost Create reply, same operation on retry, Operation.Get, conflicting ID, missing source, repeated Delete |
| TestBackupImagesAllPublicSources | Images from disk/snapshot/image/URL, inherited maps, primary and independent backup restoration, parent deletion |
| TestBackupInvalidImageAndDiskSources | Missing image/snapshot/disk sources fail without Ready |
| TestBackupFollowerFailureIsolatedFromIndependentSnapshot | A bad backup does not prevent another independent copy or either primary restoration |
| TestBackupFollowerPermanentOutageKeepsPrimary | Incomplete follower at the fixed deadline; surviving primary remains restorable |
| TestBackupDeleteAndCancelDuringCopy | Public completion is separate from backup; delete barrier, explicit non-cancellable Delete result, surviving objects and residual-copy inspection |
| backup_disabled_test | Existing public snapshot/restore behavior with backup disabled |

Every successful restoration verifies the described disk size and all logical
blocks against a deterministic xorshift oracle. SSD uses 16 MiB and SSD NRD 1 GiB,
4 MiB chunks and 4096-byte blocks. Generations include zero chunks, stable data,
overwrites and data changed to zero. Incremental preparation skips unchanged
writes so inheritance is actually exercised. DR-based snapshots deliberately
use the product's full-copy path.

The backup reader consumes only backup meta.json, chunk map, chunk objects,
compression and checksum metadata. It checks exact sizes and CRC, creates an
empty disk through DM and writes its logical blocks through NBS. It never fills
gaps from primary S3 or the source metadata database. The primary endpoint is
faulted during independent backup restoration. This is an external test adapter,
not a public RestoreBackup or a claim that product DR/failover is implemented.

A full backup requires valid metadata, a complete map and all reachable chunks.
Metadata alone, public operation completion and a timeout never prove it.
Delete/cancel races inspect incomplete residual copies instead of silently
treating them as complete; TODO #7237 remains a product limitation to observe.
A concurrent Resize rejected with the precise exclusive-volume-operation error
is recorded explicitly; the captured snapshot must remain wholly consistent and
a new Resize after completion must succeed. Other errors are not accepted.

Before each fault series the fixture performs three controls and fixes
T0=max(duration) separately for create/backup/restore and, where used,
delete/cancel. P=10 seconds is the maximum relevant configured scheduler/retry/
poll period. D=max(60 seconds,3P), W=10*T0+5*P. The test logs the values before
injecting the fault. Measured injection duration is accounted for in the slow
mode; deadlines are never extended after a failed attempt.

## Fault control and observations

The optional `--backup-test-stand` recipe starts backup-fault-proxy and a separate
backup S3 service. The proxy controls DM's NBS and primary/follower S3 traffic;
test data preparation uses the direct NBS endpoint.

`PUT /rules` replaces rules and releases prior gates. `GET /events` returns the
observed sequence. Rules match route (nbs/primary/backup), exact RPC/HTTP method,
optional disk_id and key substring. Modes are error, permanent, gate, rate and
lost-reply. Lost-reply forwards the PUT before dropping its acknowledgement.
Rate rules use a shared budget per rule. For NBS CreateCheckpoint, permanent
returns application E_ARGUMENT; gRPC InvalidArgument is retryable in the SDK.
Rules are reset on cleanup.

Preserve ytest.report.trace, test stdout/stderr, generated DM/NBS configurations
and backup-fault-events.jsonl. Events include rule ID, route, method, key,
timestamps, outcome and transferred bytes. Assertions require observed fault
hits; configuration alone is not evidence that a fault crossed the intended phase.
Record the exact product/test/toolchain/configuration/data fingerprints with
each result. A failed test must not be called successful based on process exit
or on another test's result.

## Unit coverage

See ../backup_coverage/README.md. Unit/component profiles, rather than E2E
profiles, must establish the weighted 90% threshold for the two accepted PR
deltas. Keep failed harness results with their original hashes, explain any
oracle correction and rerun only the affected scenarios.
