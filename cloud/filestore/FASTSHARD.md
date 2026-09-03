# Persistent fastshards in the local setup

Fastshard is the YDB-BlobStorage-free storage backend for file shards (see
[doc/filestore/design/storage/fastshard](../../doc/filestore/design/storage/fastshard/README.md)).
This document lists the extra steps needed to run the local filestore setup
([README.md](README.md)) with persistent fastshards, whose devices are hosted
by blockstore-disk-agent from the [example](../../example) NBS setup.

## Prerequisites

* Linux 5.17 or newer.
  There is no runtime fallback - the whole setup below needs a newer kernel.
* the local filestore setup is up and running as described in
  [README.md](README.md) - its ydbd will also serve as the storage node for
  NBS, do not start a second one
* NBS binaries are built:

```bash
./ya make --build=profile -- cloud/blockstore/buildall
```

* `configurefastshards.py` drives NBS through `blockstore-client`:

```bash
ln -svf ../../blockstore/buildall/cloud/blockstore/apps/client/blockstore-client \
    cloud/filestore/bin/
```

## 1. Enable the fastshard runtime

Add to `cloud/filestore/bin/nfs/nfs-storage.txt`:

```
FastShardRuntimeEnabled: true
```

and restart filestore-server. Without this flag persistent fastshard configs
silently degrade to stub shards.

## 2. Launch the NBS example on the shared ydbd

```bash
cd example

# prepare dirs, certs, device files and configs; the generated disk agent
# configs include JournalledDeviceTcpServerListenAddress - the journalled
# device servers fastshard talks to
./0-setup.sh

# do NOT run 1-start_storage.sh - the filestore ydbd on grpc://localhost:9001
# is the storage node

# create the /Root/NBS tenant on the running storage node
./2-init_nbs_storage_tenant.sh

# start nbsd (keep the tab open)
./3-start_nbs.sh

# start the disk agents (keep the tab open); the port override is needed
# because agent 0 would otherwise collide with filestore-vhost on 29012
IC_PORT=30100 ./4-start_disk_agent.sh
```

Agent N gets interconnect port `IC_PORT + N*100`, so the base must keep all
five clear of the journalled device ports 29900..29904.

## 3. Create the filesystem and configure fastshards

```bash
cd cloud/filestore/bin

./initctl.sh create
./configurefastshards.py --file-shard-count 4
./initctl.sh mount
```

`configurefastshards.py` creates one mirrored NBS volume (`fastshard0`) as an
allocation holder, reads its device layout from the DiskRegistry and turns
the trailing shards of the filesystem into persistent fastshards - shard k
mirrors across the k-th device of every replica of the volume. Run it with
`--help` for the knobs (shard counts, ports, volume geometry).

When re-running the script, destroying `fastshard0` marks its devices dirty;
they only become allocatable again after the DiskRegistry finishes their
secure erase (~1 min).

## Notes

* the `fastshard0` volume must never be mounted - fastshards write to its
  devices directly over the journalled device protocol
* restarting the disk agents drops their device acquisitions
  (`AcquireRequired: true`) - restart filestore-server afterwards, or the
  fastshards keep stale sessions and every request fails with
  `E_BS_INVALID_SESSION`
* monitoring: filestore-server http://localhost:8767, nbsd
  http://localhost:8766/blockstore/service, disk agents
  http://localhost:9100..9104/blockstore/disk_agent
