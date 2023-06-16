# Setup for local debugging

\# Build binaries from sources
1. Build ydbd using the instructions from https://github.com/ydb-platform/ydb/blob/main/BUILD.md
2. Build nbsd, blockstore-client, blockstore-disk-agent, blockstore-nbd in a similar way using nbs repo instead of ydb - these programs are located at cloud/blockstore/apps/{client,disk_agent,server} and cloud/blockstore/tools/nbd
3. Copy all those tools to current directory

\# Prepare current directory
```bash
./0-setup.sh
```

\# Start ydbd storage node
Run the following command in a new tab:
```bash
./1-start_storage.sh
```

\# Initialize storage node configuration
```bash
./2-init_storage.sh
```

\# Start nbsd
Run the following command in a new tab:
```bash
./3-start_nbs.sh
```

\# Create disk
```bash
./4-create_disk.sh
```

\# Attach this disk to your machine via nbd at /dev/nbd0
Run the following command in a new tab:
```bash
./5-attach_disk_via_nbd.sh
```

\# Try to access your disk
```
sudo dd oflag=direct if=/dev/urandom of=/dev/nbd0 count=5 bs=4096
sudo dd iflag=direct if=/dev/nbd0 of=./result.bin count=5 bs=4096
```

\# See what's happening inside
```
Go to localhost:8766 -> BlockStore -> Service -> vol0
```
