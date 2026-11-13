# Compute mock

Standalone gRPC binary implementing Compute `inner.DiskService.CreateToken` and
`GetExportCredentials`. The latter selects permanent S3 keys by `snapshot_id` or
`image_id`, independently of `task_id`. It uses plaintext gRPC
and ignores authorization metadata.

Build without running tests:

```bash
./ya make -r cloud/disk_manager/test/mocks/compute
```

Copy `config.example.txt` to `compute-mock-config.txt` and replace the placeholder
keys. The file uses protobuf text format:

```protobuf
ListenAddress: "127.0.0.1:9798"
ExportCredentials {
    SnapshotId: "snapshot-id-1"
    Credentials {
        access_key_id: "snapshot-1-access-key-id"
        secret_access_key: "snapshot-1-secret-access-key"
    }
}
ExportCredentials {
    SnapshotId: "snapshot-id-2"
    Credentials {
        access_key_id: "snapshot-2-access-key-id"
        secret_access_key: "snapshot-2-secret-access-key"
    }
}
ExportCredentials {
    ImageId: "image-id"
    Credentials {
        access_key_id: "image-access-key-id"
        secret_access_key: "image-secret-access-key"
    }
}
```

`ListenAddress` defaults to `127.0.0.1:9798`. Use an explicit address such as
`[::1]:9798` or `[::]:9798` when needed. Add as many `ExportCredentials` entries as
needed. Each entry selects exactly one non-empty `SnapshotId` or `ImageId` and
requires both credential fields. Duplicate IDs within the same resource type
are rejected at startup. Snapshot and image namespaces are independent.
Configuration is read at startup; restart the server to change the keys.
Credential values are not printed in logs or configuration errors.

A request must contain exactly one of `snapshot_id` or `image_id`; otherwise the
RPC returns `InvalidArgument`. An unknown resource returns `NotFound`. Changing
`task_id` does not change which configured credentials are returned.

Launch:

```bash
chmod 600 compute-mock-config.txt
./compute-mock --config compute-mock-config.txt
```

Add these client settings to the executor's full `ExportConfig`. Source storage
settings are also required, as described in the
[export task configuration](../../../internal/pkg/dataplane/snapshot/exporter/README.md#server-roles):

```protobuf
ExportConfig {
    DiskServiceClientConfig {
        Endpoint: "127.0.0.1:9798"
        Insecure: true
        CallTimeout: "30s"
    }
}
```

Existing recipe mode `./compute-mock --port 4201` still serves `CreateToken`.
Without `--config`, `GetExportCredentials` returns `FailedPrecondition`.
SIGINT/SIGTERM stops the server gracefully.
