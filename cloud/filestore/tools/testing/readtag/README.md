## readtag

Helps with finding virtiofs mount tag.

### 1. Find your virtiofs devices

```
$ for vio in /sys/bus/virtio/devices/virtio*; do echo $vio && cat "${vio}/uevent" | grep DRIVER; done
/sys/bus/virtio/devices/virtio0
DRIVER=virtio_blk
/sys/bus/virtio/devices/virtio1
DRIVER=virtio_blk
/sys/bus/virtio/devices/virtio2
DRIVER=virtio_blk
/sys/bus/virtio/devices/virtio3
DRIVER=virtiofs
/sys/bus/virtio/devices/virtio4
DRIVER=virtiofs
/sys/bus/virtio/devices/virtio5
DRIVER=virtio_net
/sys/bus/virtio/devices/virtio6
DRIVER=virtio_console

```

So our prospective devices are:
* `virtio3`
* `virtio4`

### 2. Find PCI BDFs for those devices

```
$ readlink -f /sys/bus/virtio/devices/virtio3
/sys/devices/pci0000:00/0000:00:02.0/0000:02:01.0/virtio3
$ readlink -f /sys/bus/virtio/devices/virtio4
/sys/devices/pci0000:00/0000:00:02.0/0000:02:02.0/virtio4
```

Our BDFs:
* `0000:02:01.0`
* `0000:02:02.0`

### 3. Find BAR no and DeviceCfg offset inside it

```
$ sudo lspci -s 02:01.0 -vv
02:01.0 Mass storage controller: Red Hat, Inc. Virtio file system (rev 01)
        Subsystem: Red Hat, Inc. Virtio file system
        Physical Slot: 1-2
        Control: I/O+ Mem+ BusMaster+ SpecCycle- MemWINV- VGASnoop- ParErr- Stepping- SERR+ FastB2B- DisINTx+
        Status: Cap+ 66MHz- UDF- FastB2B- ParErr- DEVSEL=fast >TAbort- <TAbort- <MAbort- >SERR- <PERR- INTx-
        Latency: 0
        Interrupt: pin A routed to IRQ 23
        Region 1: Memory at fe600000 (32-bit, non-prefetchable) [size=4K]
        Region 4: Memory at e1200000000 (64-bit, prefetchable) [size=16K]
        Capabilities: [98] MSI-X: Enable+ Count=3 Masked-
                Vector table: BAR=1 offset=00000000
                PBA: BAR=1 offset=00000800
        Capabilities: [84] Vendor Specific Information: VirtIO: <unknown>
                BAR=0 offset=00000000 size=00000000
        Capabilities: [70] Vendor Specific Information: VirtIO: Notify
                BAR=4 offset=00003000 size=00001000 multiplier=00000004
        Capabilities: [60] Vendor Specific Information: VirtIO: DeviceCfg
                BAR=4 offset=00002000 size=00001000
        Capabilities: [50] Vendor Specific Information: VirtIO: ISR
                BAR=4 offset=00001000 size=00001000
        Capabilities: [40] Vendor Specific Information: VirtIO: CommonCfg
                BAR=4 offset=00000000 size=00001000
        Kernel driver in use: virtio-pci

$ sudo lspci -s 02:02.0 -vv
02:02.0 Mass storage controller: Red Hat, Inc. Virtio file system (rev 01)
        Subsystem: Red Hat, Inc. Virtio file system
        Physical Slot: 2-2
        Control: I/O+ Mem+ BusMaster+ SpecCycle- MemWINV- VGASnoop- ParErr- Stepping- SERR+ FastB2B- DisINTx+
        Status: Cap+ 66MHz- UDF- FastB2B- ParErr- DEVSEL=fast >TAbort- <TAbort- <MAbort- >SERR- <PERR- INTx-
        Latency: 0
        Interrupt: pin A routed to IRQ 20
        Region 1: Memory at fe601000 (32-bit, non-prefetchable) [size=4K]
        Region 4: Memory at e1200004000 (64-bit, prefetchable) [size=16K]
        Capabilities: [98] MSI-X: Enable+ Count=34 Masked-
                Vector table: BAR=1 offset=00000000
                PBA: BAR=1 offset=00000800
        Capabilities: [84] Vendor Specific Information: VirtIO: <unknown>
                BAR=0 offset=00000000 size=00000000
        Capabilities: [70] Vendor Specific Information: VirtIO: Notify
                BAR=4 offset=00003000 size=00001000 multiplier=00000004
        Capabilities: [60] Vendor Specific Information: VirtIO: DeviceCfg
                BAR=4 offset=00002000 size=00001000
        Capabilities: [50] Vendor Specific Information: VirtIO: ISR
                BAR=4 offset=00001000 size=00001000
        Capabilities: [40] Vendor Specific Information: VirtIO: CommonCfg
                BAR=4 offset=00000000 size=00001000
        Kernel driver in use: virtio-pci
```

So it's BAR 4 in both cases, BAR size is 16K in both cases, the offset for
`DeviceCfg` is `0x2000` in both cases.

### 4. Use readtag to read BAR contents at the specified offset

```
$ sudo ./readtag 0000:02:01.0 4 0x4000 0x2000
myfs1
$ sudo ./readtag 0000:02:02.0 4 0x4000 0x2000
myfs2
```
