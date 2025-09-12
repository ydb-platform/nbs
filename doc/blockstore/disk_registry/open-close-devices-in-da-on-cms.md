# Open and close devices in Disk Agent on CMS actions.

## Problem

Right now, the Disk Agent opens all file devices at startup and holds a file descriptor until the process stops. Some infrastructure automation doesn't work because our process holds a file on non-allowed files. Because of this, infrastructure has to restart the agent to manipulate these devices (which are not placed into operation by CMS actions).

Also, in the future, we want hot reload device replacement, where infrastructure can replace broken devices without disk agent restarts.

## Overview

We can add the ability to open and close devices in the Disk Agent in runtime and open and close devices on CMS events.

## Detailed Design

### Concurrent open close requests

To avoid bugs, when the Disk Registry thinks that the Disk Agent has opened a device, but because of a race condition, the device is closed, we should handle all open or close requests to devices with some generation number. We can keep a counter in RAM for all devices lets call it DeviceGeneration, and increment it with each CMS action like AddHost/AddDevice or PurgeHost. When opening or closing devices via TEvOpenDeviceRequest/TEvCloseDeviceRequest, or as a result of registration, we should pass device and Disk Registry tablet generations. A device generation needs to order open and close requests sent in one DR generation. In this way, we can order all open and closed requests and reject outdated ones.

### Agent start and registration

First of all, we need to introduce API for opening and closing devices in the Disk Agent. When the Disk Agent starts, it will scan all devices and open them as usual. Then the DA will try to register in the Disk Registry. The Disk Registry will provide information about which devices need to be opened or closed by the Disk Agent in response to registration. After that, the Disk Agent should close the corresponding devices. We open all devices at startup to ensure that data plane works normally if the Disk Registry is down or unavailable. In the future, we can also introduce a local cache to avoid opening unknown devices at start.

```mermaid
sequenceDiagram
  DiskAgent ->>+ DiskRegistry: Register
  DiskRegistry ->>- DiskAgent: Register response  (pass UnknownDevices and AllowedDevices)

  loop for each allowed device
    DiskAgent ->> DiskAgent: OpenDevice
  end

  loop for each unknown device
    DiskAgent ->> DiskAgent: CloseDevice
  end
```

This also guarantees that Disk Agent will eventually receive the right configuration and open all the correct devices.

### AddHost/AddDevice

First of all, we should introduce some new states for the device.
- OpeningDevice: Disk Agent should open this device. Disk Registry should try to send OpenDeviceRequest to the Disk Agent. Disk Registry can't allocate disks on this device.
- OpenedDevice: Disk Agent should open this device. Disk Registry can allocate disks on this device.
- ClosingDevice: Disk Agent should close this device. Disk Registry should try to send CloseDeviceRequest to the Disk Agent. Disk Registry can't allocate disks on this device.
- ClosedDevice: Disk Agent should close this device. Disk Registry can't allocate disks on this device.

So, Add actions will be executed in two stages:
- In the first stage, we execute a transaction, increase the device generation, and mark the device as OpeningDevice. After that, we can reply to the infrastructure that the device is in operation, but it is not allowed to allocate disks yet.
- In second stage, we asynchronously send an OpenDeviceRequest to the Disk Agent. Only after a successful response, we can execute another transaction to mark the device as OpenedDevice. After that we can start to allocate disks on the device.

These two steps are necessary to ensure that the Disk Agent will not close the devices, even if some disks have been allocated on them.

```mermaid
sequenceDiagram
  Infra ->> DiskRegistry: AddDevice/AddHost Request
  rect rgb(191, 223, 255)
      Note over DiskRegistry: AddDevice/AddHost Transaction
    DiskRegistry ->> DiskRegistry:  Increase Device generation
    DiskRegistry ->> DiskRegistry:  Switch unknown device to OpeningDevice state
  end
  DiskRegistry ->> Infra: AddDevice/AddHost Response

  loop Until successfull response
    DiskRegistry ->>+ DiskAgent: OpenDeviceRequest
    DiskAgent ->>- DiskRegistry: OpenDeviceResponse
  end

  rect rgb(191, 223, 255)
      Note over DiskRegistry: DeviceOpened Transaction
      DiskRegistry ->> DiskRegistry:  Switch to OpenedDevice
  end
```

### RemoveDevice/RemoveHost

In the same way, we execute RemoveDevice/RemoveHost in two stages:

- In the first stage, we perform a transaction to check that all devices have been migrated, and if so, we increase the DeviceGeneration and mark them as ClosingDevices. After that, no disks will be allocated on these devices.
- In the second stage, we try to send a CloseDeviceRequest to the disk agent. Only after receiving a successful response, do we mark the device as a ClosedDevice.
- After this, we can respond to subsequent RemoveDevice/RemovHost requests with S_OK.

Right now, we respond with E_TRY_AGAIN to RemoveDevice/RemovedHost requests until all devices are migrated. After adding Open Close devices feature, we should respond with E_TRY_AGAIN until all the devices are closed.

```mermaid
sequenceDiagram
  loop Until all devices are migrated
    Infra ->>+ DiskRegistry: RemoveDevice/RemoveHost Request
    DiskRegistry ->>- Infra: RemoveDevice/RemoveHost Response E_TRY_AGAIN
  end

  Infra ->> DiskRegistry: RemoveDevice/RemoveHost Request
  rect rgb(191, 223, 255)
      Note over DiskRegistry: Successful RemoveDevice/RemoveHost Transaction
    DiskRegistry ->> DiskRegistry:  Increase Device generation
    DiskRegistry ->> DiskRegistry:  Switch unknown device to ClosingDevice state
  end
  DiskRegistry ->> Infra: RemoveDevice/RemoveHost Response E_TRY_AGAIN

  loop Until successful device close
    Infra ->>+ DiskRegistry: RemoveDevice/RemoveHost Request
    DiskRegistry ->>- Infra: RemoveDevice/RemoveHost Response E_TRY_AGAIN

    DiskRegistry ->>+ DiskAgent: CloseDeviceRequest
    DiskAgent ->>- DiskRegistry: CloseDeviceResponse
  end

  rect rgb(191, 223, 255)
      Note over DiskRegistry: DeviceClosed Transaction
      DiskRegistry ->> DiskRegistry:  Switch to ClosedDevice
  end
    Infra ->>+ DiskRegistry: RemoveDevice/RemoveHost Request
    DiskRegistry ->>- Infra: RemoveDevice/RemoveHost Response S_OK

```

### Suspended or broken devices

As a first approximation, we will not introduce any specific logic for opening or closing when the device is broken or suspended. There's no need for that right now.
