# Setup for local debugging

## Build binaries from sources
To build `nbsd`, `blockstore-client`, `diskagentd`, `blockstore-nbd` and `ydbd` binaries run the following command from the repository root folder:
```bash
./ya make cloud/blockstore/buildall -r
```
The package contains all required binaries and QEMU artifacts; see [BUILD.md](BUILD.md).

## Prepare current directory
Move to example directory
```bash
cd example
```

```bash
./0-setup.sh
```

Setup creates sparse YDB disks (64 GiB + 64 GiB + 32 GiB), file devices for
five disk agents, and self-signed development certificates. Runtime files are
stored in `$HOME/tmp/nbs` and linked as `data`, `certs`, and `logs`.
Override `PERSISTENT_TMP_DIR` when running setup to use another location.
Setup recreates the YDB disks; run it only when initializing a fresh stand.

Run all scripts from this directory. Binaries are taken from
`cloud/blockstore/buildall` relative to the repository root.
The default ports must be free, including those used by another local stand.

## Start ydbd storage node
Run the following command in a new tab:
```bash
./1-start_storage.sh
```

OR skip this step if you already have a configured ydbd instance - e.g. after
going through the filestore manual.

## Initialize storage node configuration
```bash
./2-init_storage.sh
```

OR if you already have a configured ydbd instance you should run another command
which skips storage pool and root storage initialization. The existing YDB must
use domain `Root`, ClusterUUID `local`, and storage pool kinds `ssd` and `rot`:
```bash
./2-init_storage.sh --tenant
```

## Start NBS and disk agents
Run the following command in a new tab to start two NBS nodes and five disk agents:
```bash
./3-start_nbs.sh
```

All processes run under this script and stop together on Ctrl+C or if any
process exits. Monitoring ports are 8766 and 8786; API ports are 9766 and 9786.
Logs and profiles are separate for each node.
To start only the first node, pass `--one-nbs`:

```bash
./3-start_nbs.sh --one-nbs
```

Use `--skip-da` to start only NBS, or `--skip-nbs` to start only disk agents
when NBS is already running:

```bash
./3-start_nbs.sh --skip-da
./3-start_nbs.sh --skip-nbs
```

These flags can be combined with `--one-nbs`.
The script waits for NBS to answer and configures Disk Registry before starting
agents. If both `--skip-nbs` and `--skip-da` are passed, it exits without starting
services.

Wait for agents to register and for the initial secure erase to finish before
creating nonreplicated or mirrored disks. Secure erase starts after a 30-second
cooldown; device readiness is visible in Disk Registry monitoring.
The agent monitoring ports are 9100–9104.

The example preserves the source stand's experimental storage settings,
including RDMA and disk-agent chaos injection. For deterministic debugging,
disable `ChaosConfig` in `0-setup.sh` before setup. On a host without RDMA,
set `UseRdma` and `UseNonreplicatedRdmaActor` to `false` in
`nbs/nbs-storage.txt` and `nbs/nbs-storage2.txt`, and disable the RDMA endpoints
in `nbs/nbs-rdma.txt`.

## Create and attach disk
Run the following command in a new tab to create new disk:
```bash
./5-create_disk.sh -k ssd
```
Storage kind may by overridden by ```-k``` option, following values are supported:
* **ssd** (default) - replicated network disk, ```id=vol0```
* **nonreplicated** - nonreplicated disk, ```id=nbr0```
* **mirror2** - x2 mirror (based on nonreplicated disks), ```id=mrr0```
* **mirror3** - x3 mirror (based on nonreplicated disks), ```id=mrr1```

Run the following command to attach created disk to your machine via nbd:
```bash
./6-attach_disk.sh --disk-id vol0 -d /dev/nbd0
```

For example, nonreplicated disk could be created and attached with the following commands:
```bash
./5-create_disk.sh -k nonreplicated --disk-id nrd10
./6-attach_disk.sh --disk-id nrd10 -d /dev/nbd1
```

## Try to access your disk
```
sudo dd oflag=direct if=/dev/urandom of=/dev/nbd0 count=5 bs=4096
sudo dd iflag=direct if=/dev/nbd0 of=./result.bin count=5 bs=4096
```

## See what's happening inside
Go to http://localhost:8766/blockstore/service and check a particular volume (eg ```vol0```)

If the blockstore server is running on a virtual machine, you must first configure port forwarding:
```bash
ssh -L 8766:localhost:8766 <vm_ip>
```

## Additional scripts

- `blockstore-client.sh` passes arguments to the local client (API 9766).
- `disk-agent.sh` starts one agent using `LOCATION_FILE` and `DISK_AGENT_FILE`.
- `7-run_qemu.sh --diskid vol0` starts the packaged QEMU guest with a vhost endpoint;
  it requires KVM and the QEMU binary and guest image from `buildall`.
- `8-make-disks.sh` creates a selection of disks and placement groups.
- `copy_volume.py` exercises volume links to copy a volume.
- `cleanup.sh` removes generated configs, backups, and runtime symlinks. Stop all
  processes first. The persistent files in `$PERSISTENT_TMP_DIR` remain on disk.

YDB monitoring: http://localhost:8765/. NBS logs: `logs/nbs.1.log` and
`logs/nbs.2.log`; agent logs: `logs/remote-da0.log` through `logs/remote-da4.log`.
