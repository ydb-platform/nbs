# NBS RDMA client CQ progress regression test

This test runs the production NBS RDMA client/server against a memory-backed
device inside a fresh Linux QEMU guest. It uses the disk-agent RDMA target as a
realistic traffic fixture, but the behavior under test is client CQ progress.
The guest uses Soft-RoCE over its virtual Ethernet interface, so the test does
not require RDMA hardware on the test host.

The harness submits 2,048 128-KiB reads and blocks the first client response
callback to stop CQ processing. The test fails when it detects the regression:

- the server's 512 send WRs remain saturated for one second;
- requests remain queued while neither side reports an RDMA error.

The Python QEMU wrapper uses the harness exit status directly and does not parse
its output. A zero exit status means the traffic remained live and every request
completed after the callback resumed.

Run the QEMU test on a Linux host with KVM:

```bash
./ya make -tt cloud/blockstore/tools/testing/rdma-client-cq-stall
```

The binary uses `CreateRdmaTarget`, `TDeviceClient`, `TStorageAdapter`, and the
real NBS RDMA protocol, but it is not a full `diskagentd` deployment: it does
not start the actor/control plane, Disk Registry, or a physical storage device.

To build only the harness binary:

```bash
./ya make cloud/blockstore/tools/testing/rdma-client-cq-stall/bin --build=release
```

It can be run manually with an existing RDMA device by passing an IP address
backed by that device:

```bash
sudo cloud/blockstore/tools/testing/rdma-client-cq-stall/bin/\
nbs-rdma-client-cq-stall-test <rdma-interface-ip>
```
