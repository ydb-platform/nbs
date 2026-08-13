# NBS disk-agent RNR reproduction

This tool runs the production NBS RDMA client/server and disk-agent RDMA target
against a memory-backed device. It submits 2,048 128-KiB reads, blocks the first
client response callback to stop CQ processing, and verifies that the server's
512 send WRs become saturated without an RDMA error.

Build on Linux:

```bash
./ya make cloud/blockstore/tools/testing/rdma-disk-agent-repro --build=release
```

Run with an existing RDMA device, passing an IP address backed by that device:

```bash
sudo cloud/blockstore/tools/testing/rdma-disk-agent-repro/nbs-disk-agent-rnr-repro \
    <rdma-interface-ip>
```

For Soft-RoCE:

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev eth0
sudo cloud/blockstore/tools/testing/rdma-disk-agent-repro/nbs-disk-agent-rnr-repro \
    <eth0-ip>
sudo rdma link delete rxe0
```

The process needs enough `RLIMIT_MEMLOCK` for registered RDMA buffers. Running
it through `sudo` is sufficient on typical test hosts.
