# Fast shard device management

Filestore must allocate devices through Blockstore DR on creation and release them on deletion.
It must also replace devices after failures and during maintenance.

## Blockstore DR

### External ownership

**Problem:** store the external owner and send existing DR notifications to it.

```proto
TAllocateDiskRequest       { + uint64 ExternalVolumeTabletId; }
TMarkDiskForCleanupRequest { + uint64 ExternalVolumeTabletId; }
TDeallocateDiskRequest     { + uint64 ExternalVolumeTabletId; }
TDiskConfig                { + uint64 ExternalVolumeTabletId; }
TReallocateDiskRequest     { + uint64 ExternalVolumeTabletId; }
```

`ExternalVolumeTabletId` contains the tablet ID of the Filestore shard that uses the allocated devices.
If the value is zero, DR manages the disk as an NBS volume.

Pass this tablet ID through allocation transaction arguments and `TAllocateDiskParams`.
Store it in the disk's `TDiskState` and `TDiskConfig` records.
Cleanup and deallocation requests must contain the same tablet ID as the disk record.

### Notifications

For each notification, `TDiskRegistryActor` in addition supplies `TNotifyActor` with `ExternalVolumeTabletId`.
The worker sends requests through `TVolumeProxyActor`, which uses a nonzero `ExternalVolumeTabletId` to skip SS lookup.

The Filestore shard handles the `ReallocateDiskRequest/Response` pair through [wire-compatible events](#layout-notification-protocol).
See [Filestore tablet](#filestore-tablet) for layout changes and acknowledgement rules.

### Operations that assume an NBS volume

For external disks, change these operations:

- **Failed allocation:** keep the rollback that releases partially allocated devices
  and return the error to Filestore. Skip `AddToBrokenDisks`: it schedules deletion
  of an NBS volume, which does not exist for this allocation. Apply this check in
  every allocation failure path, including mirrored rollback, before the owner
  information is lost when the disk record is erased.
- **Volume config:** skip NBS SS config updates for external disks.
- **Unsupported operations:** reject checkpoint allocation and block-size changes
  when `ExternalVolumeTabletId` is set, before any state change.
- **DR restore:** keep external disks and their replica/allocation records from
  the backup. Skip NBS `ListVolumes`, `DescribeVolume` and `GetVolumeInfo` ownership
  checks for them.
- **Cleanup loop:** deallocate a marked external disk without an SS lookup.

### Journal devices

Optionally add `STORAGE_MEDIA_JOURNAL` to create journal devices by type instead of pool name.

## Filestore DR proxy

**Problem:** limit dependencies between Filestore and Blockstore.

Add a Filestore DR proxy with its own API to isolate Blockstore dependencies.

### Requests and responses

The service and tablets use the proxy, which maps Filestore events to Blockstore events and responses back to Filestore.

```cpp
// cloud/filestore/libs/storage/api/device_service.h, tablet/service <-> proxy <-> DR
TEvAllocateDevicesRequest   { FileSystemId, TabletId, CloudId, FolderId, DeviceCount, DeviceBlocksCount }
TEvDescribeDevicesRequest   { FileSystemId }
TEvMarkForCleanupRequest    { FileSystemId, TabletId }
TEvDeallocateDevicesRequest { FileSystemId, TabletId }
TEvFinishRepairRequest      { FileSystemId, DeviceUUID }
TEvFinishMigrationRequest   { FileSystemId, SourceUUID, TargetUUID }
TEvReplaceDeviceRequest     { FileSystemId, ReplicaIndex, DeviceUUID }
-> TEv*Response {
    Error;
    Replicas[][] { DeviceUUID, Host, Port };
    Migrations[] { SourceUUID, TStorageDevice Target };
    ReplacementDeviceUUIDs[];
    UnavailableDeviceUUIDs[];
}
```

### Layout notification protocol

The Filestore DR proxy handles all communication with Blockstore except for one event. DR sends layout
change notifications directly to the tablet named by `ExternalVolumeTabletId`. The Filestore DR proxy
exposes these events as direct wire duplicates.

NOTE: the wire duplicates share the event IDs of the Blockstore pair, so a receiver in the same process
reads the wrong message type. Keep DR and Filestore in separate processes, tests included.

```cpp
TEvLayoutChangedRequest     { Headers = 1, FileSystemId = 2 }
-> TEvLayoutChangedResponse { Error = 1 }
```

## Filestore service

**Problem:** support create, resize and delete fast shards. Idempotency and cleanup are separate work.

### Shard counts

Add these request fields:

```proto
TCreateFileStoreRequest { + optional uint32 FastShardCount; }
TResizeFileStoreRequest { + optional uint32 FastShardCount; }
```

`ShardCount` counts ordinary shards; `FastShardCount` counts additional fast shards. Neither includes the main tablet.
On resize, these values specify the resulting counts, not increments. Reject count decreases until shard removal is supported.

### Automatic fast-shard count

Add these fields to `NProto::TStorageConfig` in `cloud/filestore/config/storage.proto`:

```proto
optional bool AutomaticFastShardCreationEnabled;
optional uint64 FastShardAllocationUnit;
```

#### Configuration requests

When the resolved fast-shard count is nonzero, configure the main tablet and shards as follows:

- `ShardFileSystemIds`: all shard IDs; `FileShardFileSystemIds`: only fast-shard IDs.
- Main and ordinary tablets: `IsFastShard = false`, `DirectoryCreationInShardsEnabled = true` and both lists.
- Fast shards: `IsFastShard = true`, allocated `FastShardConfig` and `DirectoryCreationInShardsEnabled = false`.

- **Creation:** in `TCreateFileStoreActor`, add a prepare step to allocate devices.
- **Resize:** add the equivalent step to `TAlterFileStoreActor`.
- **Deletion:** use `GetFileSystemTopology -> PrepareDestroy -> Delete from SS` in `TDestroyFileStoreActor`.
  `PrepareDestroy` must release and mark devices for cleanup in DR.

## Filestore tablet

### Configuration

**Problem:** persist the DR layout and run device changes through the existing shard configuration path.

```proto
TStorageGroup {
    repeated TStorageDevice Devices;
    + TDeviceLayout TargetDeviceLayout;
}

TDeviceLayout {
    repeated TDeviceMigration Migrations;
    repeated TDeviceReplacement Replacements;
    repeated string UnavailableDeviceUUIDs;
}

TDeviceMigration {
    SourceUUID;
    TStorageDevice Target;
}

TDeviceReplacement {
    BrokenUUID;
    TStorageDevice Target;
}
```

The tablet matches DR replacement targets to persisted `Devices` by replica slot
to fill `BrokenUUID`. Keep existing pairs for unchanged copies.

Each DR `AllocateDeviceResponse`/`DescribeDeviceResponse` contains the complete current layout. One active reconfiguration
actor per tablet runs `AllocateDisk -> ConfigureAsShard -> apply device changes`. Reject overlapping DR notifications
with `E_REJECTED`; DR retries them.

### Reconfiguration

Add these methods to `IFileSystemShard` to account for device changes:

```cpp
TFuture<TError> MigrateDevice(
    TString sourceUUID,
    TStorageDevice target);

TFuture<TError> ReplaceDevice(
    TString brokenUUID,
    TStorageDevice target);

TFuture<TError> RevokeDevice(TString deviceUUID);
TFuture<TError> PromoteDevice(TString targetUUID);
```

Add corresponding private tablet events to notify tablet of completions:

```cpp
TEvMigrateDeviceRequest {
    OperationId, TabletGeneration, SourceUUID, TargetUUID
}
TEvReplaceDeviceRequest {
    OperationId, TabletGeneration, BrokenUUID, TargetUUID
}
TEvRevokeDeviceRequest {
    OperationId, TabletGeneration, DeviceUUID
}
TEvPromoteDeviceRequest {
    OperationId, TabletGeneration, TargetUUID
}

TEvMigrateDeviceResponse { OperationId, TabletGeneration, Error }
TEvReplaceDeviceResponse { OperationId, TabletGeneration, Error }
TEvRevokeDeviceResponse  { OperationId, TabletGeneration, Error }
TEvPromoteDeviceResponse { OperationId, TabletGeneration, Error }
```

## Storage Group

### Startup

**Problem:** initialize usable replicas without contacting devices already known to be broken.

At startup, SG uses both `Devices` and `TargetDeviceLayout` to exclude known broken
devices and unfinished targets from recovery sources. The tablet handles device configuration changes in
`StateAdapterBroken`, also named recovery mode, so that it recovers without a restart.

### SG replication proxy

**Problem:** copy a device online while keeping normal SG read and write routing.

SG uses `TDeviceProxy` for device operations. Add a replication proxy with:

- Copy progress in reserved pages 1–7; page 0 contains the SG header.
  Advance progress only after copied pages are durable.
- Coordination of page copies and concurrent writes by page range, for example through `TDisjointIntervalMap`.

Add a direct page-write method for device copies:

```proto
// device.proto
TWritePagesRequest {
    string DeviceUUID;
    repeated TDevicePageGroup PageGroups;
}
TWritePagesResponse { Error; }
```

### Configuration management

`N` is the original replica count; `Q` is the write quorum, with `2Q > N`.
`MigrateDevice` adds a replication proxy and increases the write quorum by one while source and target coexist.
`Q+1` of `N+1` intersects every `Q` of `N` quorum in both the original and final configurations.

`ReplaceDevice` disables the broken proxy and adds a replication proxy without changing the write quorum.
The target starts voting only after promotion.

`PromoteDevice` requires durable copy completion and journal replay through `QuorumLsn`.
It then replaces the replication proxy with a normal device proxy. Quorum stays unchanged.

`RevokeDevice` removes the device from the SG configuration.
Removing a migration source after promotion, or its target after cancellation, lowers quorum by one exactly once.
Replacement does not change quorum. Recover quorum from stored migration state, not device count alone.
