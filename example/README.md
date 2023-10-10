# Setup for local debugging

\# Build binaries from sources
1. Build ydbd using the instructions from https://github.com/ydb-platform/ydb/blob/main/BUILD.md
2. Build nbsd, blockstore-client, diskagentd, blockstore-nbd using the instructions from https://github.com/ydb-platform/nbs/blob/main/example/BUILD.md
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

\# Configure and start disk agent(optional, used only by nonreplicated disks)
```bash
./4-start_disk_agent.sh
```

\# Create disk and attach this disk to your machine via nbd at /dev/nbd0

By default replicated disk with id vol0 is created, optional parameter [-n] allows to create nonreplicated disk with id nbr0
Run the following command in a new tab:
```bash
./5-create_and_attach_disk.sh [-n]
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
