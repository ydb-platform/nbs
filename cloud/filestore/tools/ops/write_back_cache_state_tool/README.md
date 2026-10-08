# Write-back-cache state tool

This recovery tool lists, validates, dumps, and carefully patches Filestore
write-back-cache state files.

## Build

```bash
./ya make cloud/filestore/tools/ops/write_back_cache_state_tool/bin
```

The resulting program is named `filestore-write-back-cache-state-tool`.

## Commands

- `list` lists state files below `--state-dir` and prints a JSON summary. It
  accepts `--state-dir` only, not `--state-file`, `--fs-id`, or `--session-id`.
- `check` validates a write-back-cache state file.
- `dump` prints an editable JSON representation. Request payloads are not
  exposed; only their checksums and selected request metadata are included.
- `patch` applies validated changes from a previous `dump`.

State files can be selected either directly with `--state-file`, or through
`--state-dir`, `--fs-id`, and optional `--session-id`. The default state
directory is `/Berkanavt/nfs-vhost/state`. A directly supplied file must be a
write-back-cache state file or a disposable copy of one.

Other options:

- `-I`, `--input`: read patch JSON from a file instead of standard input.
- `-O`, `--output`: write `list` or `dump` JSON to a file instead of standard
  output. Both commands require a new output file to prevent overwriting state
  files.
- `--unsafe-ignore-lock`: continue when the advisory state-file lock cannot be
  acquired.
- `--unsafe-ignore-corruption`: allow `patch` to attempt recovery of a corrupt
  state file.

`check` and `dump` take a shared advisory lock. `list` briefly probes each file
with an exclusive lock, and `patch` takes an exclusive lock. Commands that open
a state file map the exact locked file descriptor, preventing pathname
replacement between discovery, locking, and mapping. `--unsafe-ignore-lock`
bypasses that coordination and can race with Filestore or another tool
invocation.

## Patching

1. Stop or quiesce the service that owns the state file so its lock is
   available.
2. Run `dump`, preferably with `-O`.
3. Edit the dumped JSON.
4. Run `patch` with `-I`.
5. Run `check` and inspect a fresh `dump` before restarting the service.

The entire dump is a compare-and-swap snapshot: a patch is rejected if the
state file changed after it was dumped. Re-dump the file before retrying.
Edit the complete dump instead of constructing a partial JSON object. Unknown
JSON fields are rejected so misspelled field names cannot be silently ignored.

Allowed changes:

- Header: `ReadPos`, `WritePos`, and `MetadataChecksum`.
- Entry header: `DataChecksum`, `Tag`, and `FreeFlag`.
- Decoded write request: `NodeId`, `Handle`, and `Offset`.

`ReadPos` and `WritePos` must be aligned, within `DataCapacity`, and select a
contiguous range of entries present in the dump. Setting both to zero clears
the ring buffer. `MetadataChecksum` and `DataChecksum` can only be changed to
their corresponding calculated checksum. Entry sizes, entry identity/order,
request size, request-info presence, and payload contents cannot be changed.

Example recovery scenarios:

1. Clear all entries by setting both `ReadPos` and `WritePos` to zero.
2. Remove an unwanted entry by setting its `FreeFlag` to `true`.
3. Repair an entry checksum by setting `DataChecksum` to
   `ActualDataChecksum`.
4. Repair the metadata checksum by setting `MetadataChecksum` to
   `ActualMetadataChecksum`.
5. Recover from `E_FS_BADHANDLE` by changing `Handle` to a live handle, or by
   changing `Tag` from `0` to `1`.

All changes remain subject to the validation rules above.

## Warning

Write-back-cache state contains unflushed client requests and may contain
sensitive information. Work on a protected disposable copy whenever possible.
