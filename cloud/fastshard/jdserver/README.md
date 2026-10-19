# fastshard-jdserver

Standalone journalled device server: brings up the server from
`cloud/fastshard/journal/server` over one file-backed device, so the
journalled device can be measured with `fastshard-loadtest` without a
disk agent, a disk registry or ydb.

```bash
./ya make cloud/fastshard/jdserver cloud/fastshard/loadtest
```

## What it does

The device is one file, grown to the sum of its three parts if it is
shorter (a new file is sparse, so the parts cost nothing until written)
and opened with `O_DIRECT`, so it has to live on a filesystem that
supports it (not tmpfs). The parts follow each other on the device, as
the disk agent lays them out:

| part             | option            | default |
|------------------|-------------------|---------|
| journal metadata | `--log-meta-size` | 1G      |
| journal data     | `--log-data-size` | 1G      |
| data             | `--data-size`     | 1G      |

On start the journal is restored from the metadata part: a fresh (zeroed)
file restores as an empty journal, a file from an earlier run keeps its
records. The server then listens for the device protocol and serves until
`SIGINT` / `SIGTERM` (a second signal kills the process).

Device ownership works as on a disk agent with `AcquireRequired`: a
client owns the device from `AcquireDevices` to `ReleaseDevices`, only
the owner may read or write it, a client with a higher generation takes
it over and one with a generation below the last acquire or release is
rejected (a zero generation is not checked). `fastshard-loadtest`
acquires the device by default, so a run needs no setup beyond starting
the server.

## Options

 * `--port` - listen port; **mandatory**
 * `--host` - listen on this host only (default: every interface)
 * `--device-path` - file that backs the device; **mandatory**
 * `--device-uuid` - UUID the device is served under (default
   `jdserver-device`)
 * `--block-size` - device block size (default 4K; a multiple of 4K,
   as the file is opened with `O_DIRECT`)
 * `--log-meta-size`, `--log-data-size`, `--data-size` - the parts
   (default 1G each; multiples of the block size, a `K`/`M`/`G` suffix
   is accepted)
 * `--no-journal` - serve the device without a journal: writes go
   straight to the data and the journal parts are not allocated; use it
   for a baseline
 * `--executors` - executor threads (default 1); the first runs the device
   and the listener, the connections are spread over all of them
 * `--aio-threads` - AIO completion threads (default 1)
 * `--verbose` - debug logging

## Example

```bash
# the server; the device file lands on the current filesystem
fastshard-jdserver --port 29900 --device-path ./jd.img

# 10 seconds of 4K writes and reads, 50/50, 16 in flight, over the whole
# 1G data part (262144 pages of 4K)
fastshard-loadtest --port 29900 --device-uuid jdserver-device \
    --generation 1 --iodepth 16 --duration 10 --read-percent 50 \
    --page-count 262144 --report-interval 1 --results results.json
```

`--page-count` addresses the data part: it must not exceed
`--data-size / --page-size`, the records that land past it are rejected.
