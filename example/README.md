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

Setup prepares both cells. For each cell, it creates sparse YDB disks
(64 GiB + 64 GiB + 32 GiB), file devices for five disk agents, and self-signed
development certificates. The first cell's runtime files are stored in
`$HOME/tmp/nbs` and linked as `data`, `certs`, and `logs`; the second
cell's files are stored in `$HOME/tmp/nbs-cell2` and linked under `cell2/`.
Override `PERSISTENT_TMP_DIR` and `CELL2_PERSISTENT_TMP_DIR`, respectively,
when running setup to use other locations.
Setup recreates both cells' YDB disks; run it only when initializing a fresh stand.

`prepare_cells.py` generates the NBS cells configurations for both cells and
the second cell's YDB/NBS configurations from the shared source files.
To regenerate these configurations without initializing disks or certificates:

```bash
python3 ./prepare_cells.py
```

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

These flags can be combined with `--one-nbs`, including on the second cell.
The script waits for NBS to answer and configures Disk Registry before starting
agents. If both `--skip-nbs` and `--skip-da` are passed, it exits without starting
services.

Wait for agents to register and for the initial secure erase to finish before
creating nonreplicated or mirrored disks. Secure erase starts after a 30-second
cooldown; device readiness is visible in Disk Registry monitoring.
The agent monitoring ports are 9100–9104.

The example preserves the source stand's experimental storage settings,
including RDMA and disk-agent chaos injection. For deterministic debugging,
disable `ChaosConfig` in `setup_cell.sh` before setup. On a host without RDMA,
set `UseRdma` and `UseNonreplicatedRdmaActor` to `false` in
`nbs/nbs-storage.txt` and `nbs/nbs-storage2.txt`, and disable the RDMA endpoints
in `nbs/nbs-rdma.txt`.

## Create and attach disk
Run the following command in a new tab to create new disk:
```bash
./5-create_disk.sh -k ssd
```
Add `--cell2` to create the disk in the second cell. By default, it is created
in the first cell.

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
  Add `--cell2` to use the second cell, its socket directory, and its QMP/SSH ports.
- `8-make-disks.sh` creates a selection of disks and placement groups in cell-1.
  Add `--cell2` to create the same set in cell-2, with `-cell2` appended to
  disk and placement-group names.
- `copy_volume.py` exercises volume links to copy a volume.
- `cleanup.sh` removes generated configs, backups, and runtime symlinks. Stop all
  processes first. The persistent files in `$PERSISTENT_TMP_DIR` remain on disk.

YDB monitoring: http://localhost:8765/. NBS logs: `logs/nbs.1.log` and
`logs/nbs.2.log`; agent logs: `logs/remote-da0.log` through `logs/remote-da4.log`.

## Run two cells (shards)

Each cell has its own independent YDB cluster, NBS, and five disk agents.
YDB does not use cell IDs. NBS uses `nbs/nbs-cells.txt` to identify its local
cell and reach the peer NBS over gRPC. The first cell is `cell-1`; the second
is `cell-2`.

Start the first cell with the commands above. If it is already running,
restart NBS with the updated `3-start_nbs.sh` to load the cells configuration;
do not rerun setup on an existing stand.

Both cells are prepared by the single `./0-setup.sh` command above.
The generated configurations and shared-script links for the second cell are
under `cell2/`. If the first cell already has data, run
`python3 ./prepare_cells.py` to refresh the configurations and initialize only
the new cell's disks with `./run-cell2.sh setup_cell.sh`.

Start the second YDB in a separate terminal:

```bash
./1-start_storage_cell2.sh
```

Initialize it once, then start NBS and disk agents together in a separate terminal:

```bash
./2-init_storage_cell2.sh
./3-start_nbs_cell2.sh
```

`./2-init_storage_cell2.sh --tenant` also supports initializing only the
NBS tenant when the second cell's YDB is already configured.

The second cell's ports are the first cell's ports plus 10000:

| Service | cell-1 | cell-2 |
| --- | --- | --- |
| YDB gRPC | 9001 | 19001 |
| YDB interconnect | 29501 | 39501 |
| YDB monitoring | 8765 | 18765 |
| NBS API (nodes 1 / 2) | 9766 / 9786 | 19766 / 19786 |
| NBS data (nodes 1 / 2) | 9767 / 9787 | 19767 / 19787 |
| NBS TLS (nodes 1 / 2) | 9768 / 9788 | 19768 / 19788 |
| NBS interconnect (nodes 1 / 2) | 29502 / 29522 | 39502 / 39522 |
| NBS monitoring (nodes 1 / 2) | 8766 / 8786 | 18766 / 18786 |
| Disk-agent monitoring | 9100–9104 | 19100–19104 |

Use `--cell2` to create a disk in the second cell and the client wrapper to
inspect it:

```bash
./5-create_disk.sh --cell2 -k ssd --disk-id cell2-volume
./blockstore-client_cell2.sh describevolume --disk-id cell2-volume
# Mount the second cell's volume through the first cell's NBS:
./blockstore-client.sh startendpoint --ipc-type vhost \
    --socket "$PWD/data/cell2-volume.sock" --disk-id cell2-volume \
    --client-id cell1-client --instance-id local
```

Disk creation and ordinary `DescribeVolume` target the selected cell.
Endpoint creation searches for the volume across cells and mounts it through
NBS's cell routing. Use different disk IDs across cells to avoid ambiguous
lookups. As with the first cell, wait for device secure erase before creating
nonreplicated or mirrored disks.

`3-start_nbs_cell2.sh` starts both NBS nodes and all five disk agents within
cell-2. `--one-nbs`, `--skip-nbs`, and `--skip-da` work the same way as in the
first cell.
`run-cell2.sh` also supports `6-attach_disk.sh`, `7-run_qemu.sh`, and `cleanup.sh`.
Start QEMU against the second cell directly:

```bash
./7-run_qemu.sh --cell2 --diskid cell2-volume
```

QEMU uses separate QMP and SSH ports, 18678 and 18679. Select a different NBD
device when attaching disks from both cells. Logs and generated device
configurations for the second cell are under `cell2/`.
