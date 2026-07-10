# disk-manager-export-snapshot

Standalone tool that reads a ready snapshot (or image) directly from the
dataplane storage — chunk map in YDB, chunk data in S3 or YDB — and assembles
it into a local raw disk image, without going through the Disk Manager service.

Typical use cases: recovering data from a snapshot when the regular path
(create disk from snapshot, attach, copy) is unavailable, or inspecting
snapshot/image content offline.

## Requirements

* A host with network access to the dataplane YDB database and to S3 — usually
  a host where the Disk Manager dataplane is deployed.
* The regular Disk Manager server config (`--config`, default
  `/etc/disk-manager/server-config.txt`). The tool only uses:
  * `DataplaneConfig.SnapshotConfig` — dataplane YDB endpoint/database and the
    S3 endpoint/credentials/bucket (`PersistenceConfig` and its `S3Config`),
    plus the storage folder and the S3 key prefix;
  * `AuthConfig` — credentials, same as the service uses.
* The access may be read-only: the tool only reads the `snapshots`,
  `chunk_map` and `chunk_blobs` tables and only gets objects from the S3
  bucket.
* Free disk space for the output file. Zero chunks are skipped, so the actual
  disk usage is proportional to the number of data chunks (4 MiB each), not to
  the snapshot virtual size.

## Build

```
ya make cloud/disk_manager/cmd/disk-manager-export-snapshot
```

Plain `go build` does not work: the generated proto packages are not committed
to the repository.

## Run

```
disk-manager-export-snapshot \
    --config /etc/disk-manager/server-config.txt \
    --snapshot-id <snapshot or image ID> \
    --output /path/to/image.raw
```

* `--snapshot-id` accepts both snapshot and image IDs: images are stored in
  the same dataplane snapshot storage.
* Incremental snapshots are exported the same way as full ones: their chunk
  map is complete, unchanged chunks are shallow copies of the base snapshot
  chunks.
* `--worker-count` (default 32) is the number of chunks fetched concurrently;
  each worker holds a 4 MiB buffer.
* Progress is logged every 1024 chunks (4 GiB of data). `-v` additionally logs
  every chunk read.

## Result

The output is a raw disk image truncated to the snapshot virtual size. The
checksum of every chunk is verified during the export; a mismatch fails the
whole export. Zero chunks are not written, so the file is sparse where
possible (compare `du -h` with `du -h --apparent-size`).

The image can be verified and converted with qemu-img:

```
qemu-img info image.raw
qemu-img convert -f raw -O qcow2 image.raw image.qcow2
```

## Limitations

* The snapshot must be ready (fully created and not being deleted).
* Snapshots of encrypted disks are exported as stored, i.e. encrypted (the
  tool prints a warning).
* The legacy snapshot storage (`LegacyStorageFolder`) is not supported.
* The output must be a regular file, not a block device: the tool truncates
  it to the snapshot virtual size and relies on skipped ranges reading back
  as zeroes.
