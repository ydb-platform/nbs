# Setup for local debugging

## Build binaries from sources
To build `nbsd`, `blockstore-client`, `diskagentd`, `blockstore-nbd` and `ydbd` binaries run the following command from the repository root folder:
```bash
./ya make cloud/blockstore/buildall -r
```
For more info check [build instruction](BUILD.md)

## Prepare current directory
Move to example directory
```bash
cd example
```

```bash
./0-setup.sh
```

## Start ydbd storage node
Run the following command in a new tab:
```bash
./1-start_storage.sh
```

## Initialize storage node configuration
```bash
./2-init_storage.sh
```

## Start nbsd
Run the following command in a new tab:
```bash
./3-start_nbs.sh
```

## Configure and start disk agent (optional, used only by nonreplicated disks)
```bash
./4-start_disk_agent.sh
```

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
