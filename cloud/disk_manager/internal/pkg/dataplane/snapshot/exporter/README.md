# Snapshot and image export tasks

`SnapshotService.Export` schedules `snapshots.ExportSnapshot`, which validates
resource ownership and schedules `dataplane.ExportSnapshotToS3`. The public
operation returns `ExportMetadata` and, when finished, `ExportResponse` with
`bucket` and `manifest_key`.
`ImageService.Export` similarly schedules `images.ExportImage`, followed by
`dataplane.ExportImageToS3`. Both transfer task types share the resumable exporter.
The separate image task type prevents older snapshot-only workers from claiming
an image export whose request they cannot interpret.

The exporter writes `part-00000001.raw[.lz4]`, subsequent parts and `manifest.json`
under the requested bucket/prefix. Optional unpack tools, their checksum and
README are shared in the bucket root. It does not add a UUID to the path.
Compute must prevent overlapping exports to that destination
and retain the source until the operation ends. DM does not lock the destination.
A previous manifest is removed before overwriting parts; the new manifest is
published only when every part and companion file has been uploaded.

## Server roles

On controlplane hosts (`GrpcConfig` is present), the minimum configuration is:

```protobuf
ExportConfig {
}
```

This enables the API and coordinator. `ForbiddenFolderIds` can restrict source
folders. The child task is registered only for serialization; no Compute client,
source storage, transfer metrics or transfer limits are initialized here.

Executor hosts have no `GrpcConfig`. They require `ExportConfig` with
`DiskServiceClientConfig` and the selected source configuration. `PRIMARY` uses
`ExportConfig.SnapshotConfig`, falling back to `DataplaneConfig.SnapshotConfig`.
`BACKUP` requires `SnapshotStorageBackupConfig`; `AUTO` requires both sources.
Other worker settings have protobuf defaults. In particular, the default
per-node limit is one task of each transfer type unless explicitly
overridden in `TasksConfig.InflightTaskPerNodeLimits`.

For a slow destination, a conservative worker configuration can use
`UploadWorkerCount: 1` and `S3CallTimeout: "15m"`. These are per-executor settings,
applied to task attempts; they are not fields of the public Export request.

## Credentials and encryption

The task calls Compute `inner.DiskService.GetExportCredentials` with:

- `task_id`: the Compute export task ID received in the `x-operation-id`
  gRPC header of `SnapshotService.Export` or `ImageService.Export`;
- exactly one of `snapshot_id` and `image_id`, matching the resource kind.

The Export call requires this header. DM persists it in task metadata and
restores it after restarts; the parent task copies it into the child request.
The operation ID returned by DM remains the separate DM task ID.

Compute must associate its export task with the authorized destination. It returns
`credentials` (permanent access key ID and secret access key). DM obtains them
at the beginning of each task attempt; its request and checkpoint do not contain
credentials. Compute must implement and deploy this RPC before export execution
is enabled.

For local export runs, the standalone
[compute-mock](../../../../../test/mocks/compute/README.md) binary can serve
`GetExportCredentials` using permanent keys from a protobuf text config.
Set `DiskServiceClientConfig.Endpoint` to its address and `Insecure: true`.

## Admin test command

The `yc-disk-manager-admin snapshots export --async` command calls
`SnapshotService.Export`; `images export --async` calls `ImageService.Export` with
the same flags. It requires a client config (`--config`) with the DM
endpoint and uses the usual `IAM_TOKEN` environment variable for authorization.
Without `--async`, `export` retains its synchronous behavior and reads
`--server-config`. With `--async`, it does not read a local server config or
need a destination S3 credential file. Source selection, worker counts, part
size, compression and retries come from the server's `ExportConfig`; the
corresponding local flags are rejected with `--async`.
Configure `ExportConfig`, including `DiskServiceClientConfig` and the source,
on the DM hosts before invoking it. Its presence enables export and task
execution. Workers obtain S3 keys from Compute or the configured compute-mock.

Example client config for a local plaintext DM API:

```protobuf
Host: "127.0.0.1"
Port: 9797
Insecure: true
LoggingConfig {
    LoggingStderr {}
    Level: LEVEL_INFO
}
```

For a TLS endpoint, omit `Insecure` and provide `ServerCertFile` when a custom CA
is required. Example command:

```bash
sudo --preserve-env=IAM_TOKEN /usr/bin/yc-disk-manager-admin \
  --config "$HOME/client-config.txt" snapshots export --async \
  --id fd8ulpd46d6sitkin1r5 \
  --folder-id '<snapshot-folder-id>' \
  --compute-task-id '<compute-export-task-id>' \
  --s3-endpoint https://hb.ru-msk.vkcloud-storage.ru \
  --s3-bucket apkobzev-good --s3-prefix disk02
```

For an image, replace `snapshots export --async` with `images export --async` and
pass the image's `--id` and `--folder-id`. Configure an `ImageId` entry in the
compute-mock config. No `--src-disk-id` is needed. The image must be READY and
its folder must not be listed in `ForbiddenFolderIds`. Both PRIMARY and BACKUP,
as well as AUTO selection, are supported. Backup image metadata is read from
`images/<image-id>/meta.json`; the backup chunk map uses the image ID.
The existing manifest format retains the `snapshot_id` field for an image's
dataplane storage ID, which equals the exported image ID.

With compute-mock, use an arbitrary nonempty `--compute-task-id`. The command
prints the DM operation ID and returns after scheduling; add `--wait` to wait for
completion and print `bucket` / `manifest_key` as JSON. Interrupting the waiting
CLI leaves the server task running. Use `operations get --id <dm-operation-id>`
or `operations cancel --id <dm-operation-id>` with the same client config to
inspect or cancel it.

Once the parent schedules the transfer task, its ID is persisted in
`DataplaneTaskId`. Read it using a server config with access to this cluster's
task YDB:

```bash
sudo /usr/bin/yc-disk-manager-admin \
  --server-config '<server-config.txt>' \
  tasks get --id '<dm-operation-id>' --max-depth 0 |
  jq -r '.state.DataplaneTaskId // empty'
```

The field is absent until the transfer task is created. It remains available
after completion while the parent task is retained in YDB. Unlike dependencies,
this saved ID does not disappear when the transfer task finishes.

The default idempotency key is
`disk-manager-admin-export_<compute-task-id>`: repeating the same command returns
the same DM operation. Use a new Compute task ID for a new export. An explicit
`--idempotency-key` overrides the default. `--cloud-id` is optional.
Image exports use `disk-manager-admin-image-export_<compute-task-id>` as their
default key, so a snapshot export does not share an image operation's key.

For an encrypted snapshot, pass `--decryption-info-file decryption-info.json`:

```json
{"kek_id":"kek-id","kek_version":"kek-version-id","encrypted_dek":"AQID"}
```

This file contains decryption metadata for the manifest. `encrypted_dek` is an
already encoded Base64 string and is sent unchanged as the contents of the API
`bytes` field. The file is parsed as JSON strings, without ProtoJSON decoding
of `encrypted_dek`. Source selection, part size, compression, read/upload worker
counts and retries are controlled by the DM server's `ExportConfig`.

## Snapshot metadata

Export requires a `READY` snapshot. The controlplane task checks resource
readiness before scheduling the dataplane copy task. `CREATING` and other
non-ready states end the export operation without starting a transfer, for all
source modes. Primary storage also checks readiness before reading source data.

The historical source disk ID comes from DM snapshot metadata; reading a backup
does not require that disk to exist. Block size is recorded at snapshot creation
in both controlplane and dataplane metadata and in snapshot backup `meta.json`.
Shallow copies preserve it. For older snapshots with no recorded block size,
the parent export task reads the source disk's metadata from controlplane YDB
and persists the value in the child task request. Deleted disks can still have
metadata until cleanup; no live disk or Compute call is needed for this lookup.
If snapshot, backup and retained disk metadata all lack block size, DM logs a
warning and continues the encrypted export without `block_size` in the manifest.
The user must supply the block size manually for decryption. DM does not assume
4096 bytes.

The request's `DecryptionInfo` is copied into `manifest.json` as `decryption_info`.
Compute supplies `encrypted_dek` as Base64 text in the existing API `bytes` field;
DM preserves that text in both manifest fields without encoding it again.
Existing `kek_id` / `encrypted_dek` fields and companion files are also populated
for unpack compatibility. The `encrypted.dek` companion file contains the decoded
encrypted DEK bytes. DM removes the backup storage envelope and decompresses
source chunks, but does not decrypt disk
ciphertext. Without `DecryptionInfo`, the request describes an unencrypted
snapshot. Encrypted source metadata requires `DecryptionInfo`; an unknown block
size does not prevent exporting the ciphertext.

## Resume and failures

The task saves the chosen source, source backup metadata/chunk-map hashes, part
layout, destination preparation and completed part descriptors in its YDB task
state. Each successful part PUT is checkpointed separately. Retries on the same or
another node read and upload only uncheckpointed parts. A PUT whose response or
checkpoint was lost can repeat that part. A new Export operation starts a new
export; it does not reuse the checkpoint of an older operation.

The task pins the part size and compression for its lifetime. Source changes
between attempts fail rather than combining versions. Primary storage holds a
snapshot reference until completion/cancellation. Backups must remain available
for the whole operation; hashes detect changes, but cannot restore deleted data.
Cancellation releases the primary reference and leaves partial destination objects
without publishing a completion manifest.

Target authentication errors end the task. Other transient target errors,
including PUT deadlines, have at most `MaxRetries` retries after the initial
attempt (three retries by default). Exhaustion ends the task. Destination SDK
retries are disabled so they do not multiply this budget; the legacy
`S3MaxRetriableErrorCount` field is not used by task destination calls.
`S3CallTimeout` bounds one attempt, not the complete export or all retries.

Source S3 read timeouts checkpoint progress and yield the task to the scheduler
without consuming the task retry limit or ending the export. The task retries
until the source can be read or the operation is canceled. Each S3 call retains
its configured timeout. This also applies to HTTP 408/504 and S3 `RequestTimeout`.

Other transient source S3 failures, including EOF, retry for at most
`SourceRetryTimeout` (default `30m`). The first failure time and object identity
are checkpointed across retries and node changes. Successfully reading that
object resets the window; unrelated successful reads do not prolong it. A read
timeout also clears the bounded window; a later non-timeout failure starts anew.
Non-timeout `InternalError`, `NoSuchKey` and `NotFound` end the task without
entering this window. Source clients retain their configured SDK retries within
one call.

PUT concurrency starts at `UploadWorkerCount`. A transient PUT failure halves
the limit, down to one; five subsequent successful PUTs increase it by one, up to
the configured maximum. Successes from requests started before the last failure
do not count towards recovery. Existing PUTs finish normally; waiting uploads and
retries use the new limit. The learned limit is saved with the task checkpoint.
Adaptive concurrency does not extend the destination retry budget.

Checkpointed parts survive task retries and process restarts. A new Export
operation has a new task state and starts a new transfer. Terminal failures do
not automatically create another export. The operation remains cancellable.

## Metrics

Export workers expose these metrics through the existing monitoring endpoint
(`/metrics/` in Prometheus), with `component="snapshot_export"`. Transfer metrics
also have bounded `source` (`primary` / `backup`), `compression` and
`resource_kind` (`snapshot` / `image`) tags. No
snapshot, operation, bucket or prefix IDs are used as labels.

- `export/uploadWorkerLimit`: sum of current adaptive PUT limits of active
  tasks; `export/activeDestinationPuts`: actual in-flight destination PUT calls.

- `export/active`: currently running task attempts, including initialization.
  `runAttempts`, `runSucceeded`, `runErrors`, `retryableErrors`, `runInterrupted`
  and `resumedAttempts` count outcomes of Run calls; `runTime` measures their
  duration. Canceled contexts, task yields and lost generations are interruptions.
  `cancelSucceeded` counts successful cancellation cleanup calls.
- `export/readBytes`: logical bytes assembled, including zero chunks.
  `uploadedLogicalBytes` and `uploadedBytes` count raw and stored part payload
  bytes after successful PUTs; `uploadedParts` counts these PUTs. Rates of byte
  counters give throughput. Repeated reads or uncheckpointed uploads count as
  new work; these are operational counters, not unique exported disk size.
- `export/reusedParts` and `export/reusedBytes`: parts reused from a checkpoint
  in each attempt. They do not increase new read/upload byte counters.
- `export/completed`: transfers whose manifest was published and completed
  state saved. A subsequent lock-release retry does not count another transfer.
- `export/operationRetries`: whole source/destination calls retried by the
  exporter. SDK retries remain in the existing S3 metrics with `call` labels;
  resumed task attempts are separate from these operation retries.
- `export/stageActive`, `export/stageTime` (duration histogram) and
  `export/stageErrors` have a `stage` tag: `read`, `assemble`, `compress`, `upload`,
  `buffer_wait`, `read_slot_wait`, `upload_slot_wait`, `checkpoint`. Gauges add
  concurrent work and return to zero when it ends; times include retries or
  capacity waits. Canceled siblings do not add extra stage failures.

Metric names containing `/` are exposed with `_` in Prometheus, for example
`export_readBytes`. Divide their rate in bytes/s by 1048576 for MiB/s. Comparing
read/upload rates, stage activity and wait histograms reveals a slow source,
destination, compression or checkpoint persistence.

## Configuration

The presence of `ExportConfig` enables snapshot export and task execution on
the host. Its absence leaves the host without export execution; the public
Export RPC rejects new requests. Configure the DiskService client and the
selected source alongside the usual server, authentication and task persistence
configuration. Incomplete export configuration fails server initialization.

```protobuf
ExportConfig {
    DiskServiceClientConfig {
        Endpoint: "compute-inner.example.net:443"
        RootCertsFile: "/etc/yc/compute-root-ca.pem"
        CallTimeout: "30s"
    }
    SourceStorage: AUTO
    PartSize: 268435456
    ReadWorkerCount: 16
    UploadWorkerCount: 2
    Compression: "lz4"
    S3CallTimeout: "10m"
    S3MaxRetriableErrorCount: 3
    MaxRetries: 3
    MaxIdleConnsPerHost: 64
    ForbiddenFolderIds: "folder-that-must-not-be-exported"
}
```

Transfer settings above are the defaults. The DiskService endpoint/certificates
and source storage must be configured for the host. The default
per-node limit for each of `dataplane.ExportSnapshotToS3` and
`dataplane.ExportImageToS3` is one; an explicit
`TasksConfig.InflightTaskPerNodeLimits` entry overrides it. Each running export
holds at most `UploadWorkerCount + 1` raw part buffers plus compression buffers.
The two task limits are independent: a node may run one image export and one
snapshot export concurrently. Set runner counts and both limits according to
the node's memory budget. Setting a type's limit to zero disables it.

Source modes:

- `PRIMARY`: use `ExportConfig.SnapshotConfig`, or
  `DataplaneConfig.SnapshotConfig` when the former is absent. The primary source
  can contain chunks in both S3 and YDB.
- `BACKUP`: use `SnapshotStorageBackupConfig`, including its bucket, prefix and
  envelope key configuration; no primary YDB configuration is needed.
- `AUTO` (default): configure both sources. Use primary when its snapshot
  metadata exists; fall back to backup only when it is absent. A primary service
  error or a chunk error does not change sources during the export.

Dedicated export nodes can omit `DataplaneConfig`, using
`ExportConfig.SnapshotConfig` for primary reads or only
`SnapshotStorageBackupConfig` for backup reads. Before deploying the new code, run the usual DM schema management
command on controlplane and snapshot databases to add the optional `block_size`
columns. Dedicated export nodes also need task tables before starting runners. Export adds no destination-lock tables.

Optional `UnpackArchivePath` and `UnpackReadmePath` retain their existing admin
behavior and can also include unpack files in API exports. S3 credentials must
permit bucket/object metadata checks, part/manifest writes, removal of a previous
manifest and reading the manifest to confirm an upload with a lost response.
