# fastshard-client

Command line client for a fastshard storage node: one subcommand per
`IStorageNode` method (`sn/iface/storage_node.h`), sent over the journalled
device TCP protocol (`cloud/storage/core/protos/device.proto`) to a
blockstore-disk-agent journalled device server or the in-process `sn/server`.
The analog of `blockstore-client` for the storage node protocol.

```bash
./ya make cloud/filestore/libs/storage/fastshard/sn/fastshard_client
fastshard-client <command> --port PORT [options]
```

Only built in `OPENSOURCE` builds without `FORCE_FASTSHARD_IPC_STUB`, like
everything else that needs the real silk runtime.

## Common options

 * `--host` - storage node host; defaults to `localhost`
 * `--port` - storage node port; **mandatory**. In the local setup from
   [FASTSHARD.md](../../../../../FASTSHARD.md) disk agent N listens on
   `29900 + N`
 * `--client-id` - `ClientId` sent in the request headers. The disk agent
   uses it as the disk id of the acquire session, so a device acquired
   with one client id must be released and written with the same one
 * `--request-timeout` - `RequestTimeout` (milliseconds) sent in the request
   headers
 * `--proto` - read the whole request from input as protobuf text and print
   the whole response as protobuf text; `--client-id` / `--request-timeout`
   still override the headers when given
 * `--input` - file to read from instead of stdin: page data for
   `WriteLogRecord`, or the request protobuf with `--proto`
 * `--output` - file to write to instead of stdout: page data for
   `ReadPages`, or the response protobuf with `--proto`
 * `--verbose` - enable silk debug logging
 * free argument: the command; either camel case (`ReadPages`), a single
   lowercase word (`readpages`) or words separated by hyphens or underscores
   (`read-pages`)

The exit code is 0 when the response carries no error and 1 otherwise. Outside
`--proto` mode the error is printed to stderr. `SIGINT` / `SIGTERM` abandon a
request that hangs and exit with 1.

## Commands

### AcquireDevices

 * `--device-uuid` - device to acquire; **mandatory**, may be repeated
 * `--generation` - writer generation; defaults to 0

Prints `OK`.

### ReleaseDevices

 * `--device-uuid` - device to release; **mandatory**, may be repeated

Prints `OK`.

### ReadPages

 * `--device-uuid` - device to read from; **mandatory**
 * `--first-page-no` - number of the first page; defaults to 0
 * `--page-count` - number of consecutive pages; **mandatory**
 * `--page-size` - logical page size in bytes; defaults to 4096

Writes the raw page contents to output. Use `--proto` to see
`LastAckedLogSequenceNumber` or to request several page groups at once.

### WriteLogRecord

 * `--device-uuid` - device to write to; **mandatory**
 * `--first-page-no` - number of the first page; defaults to 0
 * `--page-size` - the input is split into pages of this size; defaults
   to 4096. The input size must be a positive multiple of it
 * `--lsn` - log sequence number of the record; defaults to 0
 * `--prev-lsn` - log sequence number of the previous record; defaults to 0

Reads the page data from input and prints `OK`.

### ReadJournalTail

 * `--device-uuid` - device to read the journal of; **mandatory**
 * `--after-lsn` - only records with a strictly greater log sequence number
   are returned; defaults to 0
 * `--max-record-count` - maximum number of records; 0 (the default) means
   no limit

Prints `LastAckedLogSequenceNumber` and one summary line per record (LSN,
previous LSN, page groups). Page contents are only available via `--proto`.

### AdvanceLsnLowWatermark

 * `--device-uuid` - device whose watermark is advanced; **mandatory**
 * `--lsn-low-watermark` - records below this log sequence number may be
   applied and dropped from the journal; defaults to 0

Prints `OK`.

## Examples

```bash
# take the device, write two 4K pages at page 10 and read them back
fastshard-client acquiredevices --port 29900 --client-id tool --device-uuid $DEV --generation 1
head -c 8192 /dev/urandom > pages.bin
fastshard-client writelogrecord --port 29900 --client-id tool --device-uuid $DEV \
    --first-page-no 10 --lsn 1 --input pages.bin
fastshard-client readpages --port 29900 --client-id tool --device-uuid $DEV \
    --first-page-no 10 --page-count 2 --output readback.bin
cmp pages.bin readback.bin
fastshard-client releasedevices --port 29900 --client-id tool --device-uuid $DEV

# the same read as protobuf text
printf 'DeviceUUID: "%s"\nPageGroupRefs { FirstPageNo: 10 PageCount: 2 PageSize: 4096 }\n' "$DEV" \
    | fastshard-client readpages --port 29900 --client-id tool --proto
```
