# Snapshot export submission and parent tasks

`SnapshotService.Export` validates an export request and schedules
`snapshots.ExportSnapshot`. Its operation exposes `ExportMetadata` and returns
`ExportResponse` with `bucket` and `manifest_key` when the transfer completes.
Image export remains a separate API stub.

## Parent orchestration

The parent task checks that the snapshot is ready, belongs to the requested
`folder_id` and is not in `ExportConfig.ForbiddenFolderIds`. It obtains the
historical source disk ID and block size from controlplane metadata, then
schedules `dataplane.ExportSnapshotToS3` with a stable child idempotency key.
The source disk need not exist; older snapshots can use block size from a
retained disk row in controlplane YDB.

The child task ID is saved in parent state. Task retries reuse that child;
metadata and the final response are forwarded to the public operation.
Canceling the parent requests child cancellation and waits for it to finish.

The parent executes on controlplane nodes with `ExportConfig`. Other
controlplane nodes register it only for reading operation state. The child
contract is registered for serialization on all nodes and is excluded from
`TaskTypesForExecution` in this change. S3 transfer execution is a dependent
change. Until an executor is deployed, the parent waits for its queued child.
Cancellation of that child also requires an executor.

## Compute task identity and credentials

The Export call requires `x-operation-id` containing the Compute export task ID.
The scheduler persists this header in task metadata and restores it on retries.
The parent copies it into the child request. The operation ID returned by DM is
its separate task ID.

The child contract supplies this Compute task ID and `snapshot_id` to the future
DiskService client via `inner.DiskService.GetExportCredentials`. Destination S3
keys are permanent and do not belong in the public Export request or task state.
Credential retrieval and S3 connections are part of transfer execution.

The request's optional `DecryptionInfo` is passed to the child unchanged for
inclusion in `manifest.json`. `encrypted_dek` uses Base64 in JSON. Its absence
describes an unencrypted snapshot; export does not decrypt disk contents.

## Admin command

The temporary `yc-disk-manager-admin snapshots export-task` command calls the
API using a client config (`--config`) and the usual `IAM_TOKEN` environment
variable. It requires no local server config or destination S3 credential file.

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

For TLS, omit `Insecure` and provide `ServerCertFile` when a custom CA is required.
Example command:

```bash
sudo --preserve-env=IAM_TOKEN /usr/bin/yc-disk-manager-admin \
  --config "$HOME/client-config.txt" snapshots export-task \
  --id fd8ulpd46d6sitkin1r5 \
  --folder-id '<snapshot-folder-id>' \
  --compute-task-id '<compute-export-task-id>' \
  --s3-endpoint https://hb.ru-msk.vkcloud-storage.ru \
  --s3-bucket apkobzev-good --s3-prefix disk02
```

The command prints the DM operation ID after scheduling. `--wait` waits for the
executor and prints the final response as JSON. Interrupting the waiting CLI
leaves the operation running. Use `operations get --id <dm-operation-id>` or
`operations cancel --id <dm-operation-id>` with the same client config.

The default idempotency key is
`disk-manager-admin-export_<compute-task-id>`. Repeating the request returns the
same DM operation; use a new Compute task ID for a new export. An explicit
`--idempotency-key` overrides the default. `--cloud-id` is optional.

For an encrypted snapshot, pass `--decryption-info-file decryption-info.json`:

```json
{"kek_id":"kek-id","kek_version":"1","encrypted_dek":"AQID"}
```

## Configuration and schema

`ExportConfig` presence enables submission and parent execution on controlplane:

```protobuf
ExportConfig {
    ForbiddenFolderIds: "folder-that-must-not-be-exported"
}
```

The shared config already declares DiskService, source, part size, parallelism,
compression and retry settings for the dependent transfer executor. This part
does not open source storage or destination S3 connections.

Before deployment, update the controlplane snapshot schema with the usual DM
schema management command to add optional `block_size`. New snapshots retain
source geometry in controlplane metadata, and the parent passes it to the child.
For old snapshots, the retained disk row supplies a fallback. Primary snapshot
storage and backup metadata support are part of the transfer change.
