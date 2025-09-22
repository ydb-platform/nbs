# Attach and detach HW devices in Disk Agent on CMS actions.

## Problem

The Path is an hardware device file.

Right now, the Disk Agent attaches all Path's at startup and holds a file descriptor until the process stops. Some infrastructure automation doesn't work because our process holds a file descriptor on non-allowed files. Because of this, infrastructure has to restart the agent to manipulate these devices (which are not placed into operation by CMS actions).

Also, in the future, we want hot reload device replacement, where infrastructure can replace broken hw devices without disk agent restarts.

## Overview

We can add the ability to attach and detach Paths in the Disk Agent in runtime and do it on CMS events.

## Detailed Design

### Concurrent attach detach requests

To avoid bugs, when the Disk Registry thinks that the Disk Agent has attached Path, but because of a race condition, the Path is detached, we should handle all attach or detach requests with some generation number. We can keep a counter in RAM for all Paths lets call it Path Generation, and increment it with each CMS action like AddHost/AddDevice or RemoveDevice/RemoveHost. When attaching or closing Path via TEvAttachPathRequest/TEvDetachPathRequest, or as a result of registration, we should pass Path and Disk Registry tablet generations. A Path Generation needs to order attach and detach requests sent in one DR generation. In this way, we can order all attach and detach requests and reject outdated ones.

### Agent start and registration

First of all, we need to introduce API for attaching and closing Path's in the Disk Agent. When the Disk Agent starts, it will scan all files and attach them as usual. Then the DA will try to register in the Disk Registry. The Disk Registry will provide information about which Paths need to be attached or detached by the Disk Agent in response to registration. After that, the Disk Agent should detach the corresponding Path's. We attach all Path's at startup to ensure that data plane works normally if the Disk Registry is down or unavailable. In the future, we can also introduce a local cache to avoid attaching unknown Path's at start.

```mermaid
sequenceDiagram
  DiskAgent ->>+ DiskRegistry: Register
  DiskRegistry ->>- DiskAgent: Register response  (pass UnknownPaths and AllowedPaths)

  loop for each allowed Path
    DiskAgent ->> DiskAgent: AttachPath
  end

  loop for each unknown Path
    DiskAgent ->> DiskAgent: DetachPath
  end
```

This also guarantees that Disk Agent will eventually receive the right configuration and attach all the correct Paths.

### AddHost/AddDevice

First of all, we should introduce some new states for the device.
- AttachingPath: Disk Agent should attach Path under this device. Disk Registry should try to send AttachPathRequest to the Disk Agent. Disk Registry can't allocate disks on this device.
- AttachedPath: Disk Agent should attach Path under this device. Disk Registry can allocate disks on this device.
- DetachingPath: Disk Agent should detach Path under this device. Disk Registry should try to send DetachPathRequest to the Disk Agent. Disk Registry can't allocate disks on this device.
- DetachedPath: Disk Agent should detach Path under this device. Disk Registry can't allocate disks on this device.

So, Add actions will be executed in two stages:
- In the first stage, we execute a transaction, increase the Path Generation, and mark the device as AttachingPath. After that, we can reply to the infrastructure that the Path is in operation, but it is not allowed to allocate disks yet.
- In second stage, we asynchronously send an AttachPathRequest to the Disk Agent. Only after a successful response, we can execute another transaction to mark the device as AttachedPath. After that we can start to allocate disks on the device.

These two steps are necessary to ensure that the Disk Agent will not detach the Path, if some disks have been allocated on them.

```mermaid
sequenceDiagram
  Infra ->> DiskRegistry: AddDevice/AddHost Request
  rect rgb(191, 223, 255)
      Note over DiskRegistry: AddDevice/AddHost Transaction
    DiskRegistry ->> DiskRegistry:  Increase Path Generation
    DiskRegistry ->> DiskRegistry:  Switch to AttachingPath state
  end
  DiskRegistry ->> Infra: AddDevice/AddHost Response

  loop Until successfull response
    DiskRegistry ->>+ DiskAgent: AttachPathRequest
    DiskAgent ->>- DiskRegistry: AttachPathResponse
  end

  rect rgb(191, 223, 255)
      Note over DiskRegistry: DeviceAttached Transaction
      DiskRegistry ->> DiskRegistry:  Switch to AttachedPath
  end
```

### RemoveDevice/RemoveHost

In the same way, we execute RemoveDevice/RemoveHost in two stages:

- In the first stage, we perform a transaction to check that all devices have been migrated, and if so, we increase the PathGeneration and mark them as DetachingPath. After that, no disks will be allocated on these devices.
- In the second stage, we try to send a DetachPathRequest to the disk agent. Only after receiving a successful response, do we mark the device as a DetachedPath.
- After this, we can respond to subsequent RemoveDevice/RemoveHost requests with S_OK.

Right now, we respond with E_TRY_AGAIN to RemoveDevice/RemovedHost requests until all devices are migrated. After adding Attach Detach Paths feature, we should respond with E_TRY_AGAIN until all the Paths are detached.

```mermaid
sequenceDiagram
  loop Until all devices are migrated
    Infra ->>+ DiskRegistry: RemoveDevice/RemoveHost Request
    DiskRegistry ->>- Infra: RemoveDevice/RemoveHost Response E_TRY_AGAIN
  end

  Infra ->> DiskRegistry: RemoveDevice/RemoveHost Request
  rect rgb(191, 223, 255)
      Note over DiskRegistry: Successful RemoveDevice/RemoveHost Transaction
    DiskRegistry ->> DiskRegistry:  Increase Path Generation
    DiskRegistry ->> DiskRegistry:  Switch to DetachingPath state
  end
  DiskRegistry ->> Infra: RemoveDevice/RemoveHost Response E_TRY_AGAIN

  loop Until successful Path detach
    Infra ->>+ DiskRegistry: RemoveDevice/RemoveHost Request
    DiskRegistry ->>- Infra: RemoveDevice/RemoveHost Response E_TRY_AGAIN

    DiskRegistry ->>+ DiskAgent: DetachPathRequest
    DiskAgent ->>- DiskRegistry: DetachPathResponse
  end

  rect rgb(191, 223, 255)
      Note over DiskRegistry: DiskDetached Transaction
      DiskRegistry ->> DiskRegistry:  Switch to DetachedPath
  end
    Infra ->>+ DiskRegistry: RemoveDevice/RemoveHost Request
    DiskRegistry ->>- Infra: RemoveDevice/RemoveHost Response S_OK

```

### Suspended or broken devices

As a first approximation, we will not introduce any specific logic for attaching or closing when the device is broken or suspended. There's no need for that right now.
