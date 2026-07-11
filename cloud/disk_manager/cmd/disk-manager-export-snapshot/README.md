# disk-manager-export-snapshot

Standalone tool that reads a ready snapshot (or image) directly from the
dataplane storage — chunk map in YDB, chunk data in S3 or YDB — and writes it
to stdout as a raw disk image stream, without going through the Disk Manager
service.

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
* A receiver for the raw stream. When redirecting stdout to a file, make sure
  there is enough free disk space for the snapshot virtual size. Stream output
  cannot create sparse holes by itself, so zero chunks are written explicitly.

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
    > /path/to/image.raw
```

The tool writes only raw image bytes to stdout. Logs and progress messages are
written to stderr, so stdout can be redirected or piped safely:

```
disk-manager-export-snapshot \
    --config /etc/disk-manager/server-config.txt \
    --snapshot-id <snapshot or image ID> \
    | qemu-img convert -f raw -O qcow2 - image.qcow2
```

* `--snapshot-id` accepts both snapshot and image IDs: images are stored in
  the same dataplane snapshot storage.
* Incremental snapshots are exported the same way as full ones: their chunk
  map is complete, unchanged chunks are shallow copies of the base snapshot
  chunks.
* Progress is logged every 1024 chunks (4 GiB of data). `-v` additionally logs
  every chunk read.
* `--read-workers` controls how many data chunks are read in parallel from
  S3/YDB before being written to stdout in order. Default is 16. Increase it
  if S3 latency/bandwidth is the bottleneck; decrease it to reduce memory and
  storage pressure. The prefetch window is four times larger than the worker
  count, so buffered chunk data is bounded at roughly
  `--read-workers * 4 * 4 MiB`.

## Result

The output stream is a raw disk image with exactly the snapshot virtual size.
The checksum of every data chunk is verified during the export; a mismatch
fails the whole export. Zero chunks are emitted as zero bytes.

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
* Stream output is sequential. If you redirect it to a regular file, the file
  will be fully allocated by the filesystem/write path instead of relying on
  sparse skipped ranges.
