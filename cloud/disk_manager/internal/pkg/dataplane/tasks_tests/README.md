# Snapshot backup failure tests

These component tests use the local database and S3 emulator provided by the Disk Manager recipe. They need no cloud credentials, shared buckets, or service VMs. Runtime code and backup-retention policies are unchanged.

## Run

From the repository root on a configured Linux test host:

```bash
./ya make -tt -j8 \
  cloud/disk_manager/test/mocks/s3_fault_proxy/tests \
  cloud/disk_manager/internal/pkg/dataplane/tasks_tests \
  cloud/disk_manager/internal/pkg/services/snapshots/tasks_tests
```

The recipes start all dependencies. Do not replace their endpoints with a shared object store; the proxy intentionally accepts only loopback HTTP upstreams.

## Component coverage

- HTTP 403, 429, 503, and S3 SlowDown do not mark unwritten chunks complete. A retry succeeds after fault removal. SDK retries are disabled here, so this does not measure SDK backoff.
- A lost response after a successful chunk write preserves data and metadata on replay without double-counting completed chunks.
- Failure to persist completion after an object write permits safe replay by a new task instance.
- A failing snapshot does not block healthy work in the same queue in the bounded mixed-work scenario.
- Context cancellation interrupts a stalled request; a later attempt succeeds after access is restored.
- Failure to save progress after enqueueing chunks does not duplicate work after reloading the last checkpoint.
- Loss of the final map response or its task checkpoint permits replay of the same map and queue cleanup.
- Deletion while a chunk PUT is held prevents the deleted snapshot's task from publishing a final map. This does not specify retention of partial objects.
- Fault sequences with fixed seeds verify zero chunks, compression, and both supported primary chunk-storage backends.
- Reading a completed backup after deleting its source does not fall back to primary storage.
- Shared parent/child chunks remain readable after deleting both sources when their backups completed first.
- Empty and all-zero snapshots do not require payload copying.
- Modified encrypted objects, malformed compressed content, bad checksums, and missing backup chunks fail validation.
- Control-plane metadata, checkpoint, and child-task failures do not produce successful completion. Metadata presence alone is not success.
- A DEK checkpoint failure precedes the first metadata write. Replay from a successful checkpoint reuses the same DEK and passes it to the data plane.

## Failure model and limits

`cloud/disk_manager/test/mocks/s3_fault_proxy` filters local requests by method and path prefix. It can return an error before writing, lose a successful response, or hold a request until an explicit release or cancellation. Request and response bodies are not logged.

`test.NewFaultyS3Client` gives the backup client a five-second timeout and disables SDK retries so faults reach the backup task. Primary-store access and validation reads are unaffected.

`backupCheckpoint` retains only successfully persisted task-state bytes. A new task instance calls `Load`; unsaved in-memory state is discarded. This models task replay, not process termination. Fixed seeds determine fault/data selection, not every concurrent timing.

The component reader uses `backup.S3.GetObject` for authenticated decryption and production decompression/checksum code. It is not a server-side disk restore API or complete disaster-recovery test. Encryption keys are generated locally for the fixture.

Separate integration or deployment checks are needed for real credentials/TLS, SDK retry behavior, key delivery/rotation, server-side restore, retention/garbage collection, and extended process disruption. Maximum-disk and memory-profile testing are outside this change.

## Strict regressions and observed failures

At base revision `f32903815a6466835c3116c3c07630fa18b9696f`, with these tests added and runtime code unchanged, the separate regression target exposed two defects: missing queue entries can starve healthy work, and a child backup can complete before inherited data is available.

The report contained 44 passing entries and five failing entries. The five failures represent one queue case, one parent test result, and three child-data subcases, not five independent defects. The regular data-plane target had 44 passing entries and the control-plane target had 13.

These regressions are not in required PR checks or `RECURSE_FOR_TESTS`. They contain no skips, expected-failure markers, or weakened assertions. Their failures remain release concerns even when the primary suite passes.

```bash
./ya make -tt -j8 cloud/disk_manager/internal/pkg/dataplane/backup_regressions_tests
```

### Missing chunks starve a healthy snapshot beyond the queue window

`TestBackupFaultMissingQueueWindowDoesNotStarveHealthySnapshot` creates a real healthy source chunk and queues it with 1001 entries whose source data is missing. A test wrapper fixes an admissible ordering of the actual queue records: missing chunks occupy the first 1000-entry window and healthy work follows it. An assertion verifies this order. The production worker then runs three times with the queue preserved.

Observed result: reading the healthy backup chunk fails with `s3 object not found` at `backup_regressions_test.go:77`. The test does not reach successful healthy-backup content verification.

The worker in `backup_chunks_task.go` requests at most 1000 entries and shuffles only that selected window. When it copies nothing, it ends the attempt instead of advancing. The storage query uses LIMIT without a cursor or ordering guarantee. Repeatedly returning the same window is admissible; the fixture does not fake chunk-read or write results.

Snapshot deletion can remove source chunks while their backup queue entries remain. The fixture constructs that final queue state directly; it does not exercise deletion of all 1001 chunks through the API. This proves a missing progress guarantee for an admissible ordering, not the frequency of that ordering in a deployed database.

A correction must let healthy work progress without marking missing data copied or losing transiently failing work.

### Child backup completes without an inherited parent chunk

`TestBackupFaultChildWaitsForInheritedChunk` creates a parent chunk containing `abc`. The child shares it through the real `ShallowCopyChunk` path and adds a new `def` chunk. The shared reference changes the actual source reference count.

Three variants are checked:

- `never_backed_up`: parent backup has not started.
- `copy_delayed`: the inherited chunk is queued but not copied.
- `deleted_while_queued`: the parent is deleted, but the child's reference keeps source data alive. The fixture separately verifies that source data is readable.

The proxy blocks only the inherited chunk's backup PUT. Before retrying the child task, the test confirms that its new chunk exists in the backup store and the shared chunk does not. It expects `InterruptExecutionError`, because the backup still lacks required data.

Observed result: all three child calls return `nil` at `backup_regressions_test.go:126`, failing `requireBackupWaiting` in `backup_faults_test.go:55`. This is premature successful completion, not a timeout or decryption error.

The task enqueues only the child's new chunks and compares completion only against that enqueued count. Its final map includes inherited references. Publication of an incomplete map follows from the successful runtime path; the later explicit map-read and recovery assertions were **not executed**, because the test stopped at unexpected success.

Before final-map publication, all referenced data must be available, including inherited data whose parent backup never ran or whose parent was deleted. Otherwise the backup cannot restore the complete disk after primary data is lost.

The regression target also runs baseline task tests because it shares their fixtures. After a separate runtime fix, rerun the entire target and move these checks into the required set. No runtime fix or successful real-environment restore is claimed here.
