# run RDMA target with local storage device connected
```bash
./rdma-test \
    --test Target \
    --host localhost \
    --port 1234 \
    --wait BusyWait \
    --storage Local \
    --storage-path /dev/nvme3n1
```

# run RDMA target with in-memory storage
```bash
./rdma-test \
    --test Target \
    --host localhost \
    --port 1234 \
    --wait BusyWait \
    --storage Memory
```

# run RDMA target without any storage
```bash
./rdma-test \
    --test Target \
    --host localhost \
    --port 1234 \
    --wait BusyWait \
    --storage Null
```

# run RDMA initiator against RDMA target
```bash
./rdma-test \
    --test Initiator \
    --host localhost \
    --port 1234 \
    --wait BusyWait \
    --iodepth 1
```

# run RDMA initiator agains local storage device
```bash
./rdma-test \
    --test Initiator \
    --storage Local \
    --storage-path /dev/nvme3n1 \
    --iodepth 1
```
