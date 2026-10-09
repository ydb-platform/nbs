# Automatic backup recovery after terminal retry exhaustion

This is a strict regression target, separate from required PR checks and `RECURSE_FOR_TESTS`. It reproduced a recovery failure at base revision `f32903815a6466835c3116c3c07630fa18b9696f`. No assertions are skipped or weakened. A passing primary suite does not prove recovery after a prolonged object-store outage.

## Requirement and method

A snapshot must receive its backup automatically even when the store recovers only after the backup task exhausts its retries and becomes cancelled. An operator must not need to create a replacement task or edit storage queues.

1. Start isolated service processes and local storage emulators. Put the backup store behind the loopback fault proxy.
2. Set `TasksConfig.MaxRetriableErrorCountByTaskType` to `3` only for `snapshots.BackupSnapshot`. This limits task retries, not HTTP SDK retries; other task types retain recipe defaults.
3. Create a disk and snapshot through the API. Return `503 ServiceUnavailable` only for that snapshot's metadata PUT.
4. Read `GetTaskByIdempotencyKey("backup_snapshot_<id>", "")` until the persisted status is `TaskStatusCancelled`. Require exactly three retries, the expected error, and at least four fault hits: the initial attempt plus three retries.
5. Reset the fault. Create a second independent disk and snapshot, then verify its automatic backup. This proves the store and normal scheduler are working again without shared chunks.
6. Without manual rescheduling or database changes, wait up to two minutes for the original snapshot's backup and verify metadata, map, checksums, and data.

The fault TTL is 180 seconds with cleanup reset. Cancellation must be observed within 60 seconds of snapshot creation while the fault remains active.

## Run

```bash
./ya make -ttt -j8 \
  cloud/disk_manager/internal/pkg/facade/snapshot_service_backup_retry_exhaustion_test \
  --test-filter '*TestSnapshotServiceAutomaticBackupAfterRetryExhaustion*'
```

The recipe uses `--backup-task-max-retriable-errors 3`. Without the filter, the target also runs the baseline round-trip test whose helpers it shares.

## Interpret the result

- Failure before confirmed cancellation, an unexpected retry count, or an expired fault means the required failure scenario was not established.
- An unreadable second backup means recovery of the store or scheduler was not established.
- A readable second backup but no final map for the first snapshot within two minutes violates the recovery requirement. The report identifies the terminal task, retries, error, and remaining queue entries.
- Two complete matching backups satisfy this scenario, not the other integrity and race requirements.

## Observed result

The isolated Linux run on 2026-10-09 reached the recovery assertion and failed with `backup map was not published` for the original snapshot. The build command returned exit code `10`; the structured test report records one failed test.

- The original task reached persisted cancellation with exactly three retriable errors while the injected `503` fault was active.
- The pending snapshot backup queue was empty after cancellation.
- After resetting the fault, a new snapshot on an independent disk received a complete backup. Its metadata, map, checksums, and restored data matched the expected content.
- The original snapshot's map did not appear within the next two minutes. Its content verification was not reached.

This confirms failure to recover within the tested interval, not indefinite absence of recovery. The earlier run that stopped at an incorrect test-storage folder is not used as evidence. The helper now uses the same `snapshots` folder as the recipe; all five primary API targets passed again after this correction.

## Suspected cause

The task runner applies the per-type retry limit and starts cancellation. `backupSnapshotTask.Cancel` calls `SnapshotBackupCancelled`, which removes the snapshot from `backup_queue`. The normal scheduler selects that queue and therefore no longer sees the missing backup.

Keeping the queue entry alone may not suffice: the scheduler uses a stable idempotency key, and task storage returns the existing terminal task instead of restarting it. Any runtime correction belongs in a separate change; this target only tests the requirement.
