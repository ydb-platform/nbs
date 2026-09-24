# Fast shard device management

Problem: Filestore must manage the full device lifecycle through blockstore DR: allocation on filesystem creation,
deallocation on deletion, and replacement after device failures and during scheduled maintenance.

## Blockstore DR

### External ownership and notifications

**Problem:** persist the external owner and deliver existing DR notifications to it.

```proto
TAllocateDiskRequest       { + uint64 ExternalVolumeTabletId; }
TMarkDiskForCleanupRequest { + uint64 ExternalVolumeTabletId; }
TDeallocateDiskRequest     { + uint64 ExternalVolumeTabletId; }
TDiskConfig                { + uint64 ExternalVolumeTabletId; }
TReallocateDiskRequest     { + uint64 ExternalVolumeTabletId; }
```

Zero keeps native NBS behavior. Pass the owner through allocation transaction arguments,
`TAllocateDiskParams`, `TDiskState`, load/save and backup. Persist it on the logical
disk and internal replicas. Reject allocation with a different owner, including
native/external changes, before mutating state or returning `S_ALREADY`.

`TNotifyActor` takes the owner from the logical disk and sets `ExternalVolumeTabletId`. Keep
the existing notification queue, sequence numbers and retries.

In `TVolumeProxyActor`, branch on `ExternalVolumeTabletId` **before native SS lookup**.

The Filestore tablet handles the `ReallocateDiskRequest/Response` pair as a wire
duplicate, see [Filestore DR proxy](#filestore-dr-proxy). Its reconciliation and
acknowledgement rules are in [Filestore Tablet](#filestore-tablet).

### Paths that assume an NBS volume

**Problem:** DR uses NBS services to check, configure and delete volumes. Filestore
owns its tablets, so these calls must skip disks with `ExternalVolumeTabletId` set.

- **Failed allocation:** keep the rollback that releases partially allocated devices
  and return the error to Filestore. Skip `AddToBrokenDisks`: it schedules deletion
  of an NBS volume, which does not exist for this allocation. Apply this check in
  every allocation failure path, including mirrored rollback, before the owner
  information is lost when the disk record is erased.
- **Volume config:** skip NBS SS config updates for external disks.
- **Unsupported operations:** reject checkpoint allocation and block-size changes
  when `ExternalVolumeTabletId` is set, before changing state.
- **DR restore:** retain external disks and their replica/allocation records from
  the backup. Skip NBS `ListVolumes`, `DescribeVolume` and `GetVolumeInfo` ownership
  checks for them.
- **Cleanup loop:** deallocates a marked external disk on the next cycle without an SS
  check. Acceptable: the mark is set right before `DeallocateDisk`.

### MEDIA_KIND_JOURNALDEVICE

Optionally support `STORAGE_MEDIA_JOURNAL`. To explicitly ensure proper device type instead of fragile pool name.

## Filestore DR proxy

**Problem:** isolate Filestore from Blockstore.

Registers under `MakeFileStoreDeviceProxyId()`. Provides DR event wrapping under public api. Only `impl/` part of the
library links blockstore counterpart.

Note that tablet directly has to handle notification from DR, i.e. `TEvVolume::TEvReallocateDiskRequest/Response`. For 
that purpose `TEvLayoutChangedRequest/Response` are wire duplicates using the same actor event IDs.

```cpp
// cloud/filestore/libs/storage/api/device_service.h, tablet/service -> proxy -> DR
TEvAllocateDevicesRequest   { FileSystemId, TabletId, CloudId, FolderId, DeviceCount, DeviceBlocksCount }
TEvDescribeDevicesRequest   { FileSystemId }                        // DescribeDisk, read-only
TEvMarkForCleanupRequest    { FileSystemId, TabletId }
TEvDeallocateDevicesRequest { FileSystemId, TabletId }              // Sync = false
TEvFinishRepairRequest      { FileSystemId, DeviceUUID }            // MarkReplacementDevice(false)
TEvFinishMigrationRequest   { FileSystemId, SourceUUID, TargetUUID }
TEvReplaceDeviceRequest     { FileSystemId, ReplicaIndex, DeviceUUID }  // DiskId = <fs>/<index>
-> TEv*Response {
    Error;
    // Allocate/describe only; preserve DR replica slots, main replica first.
    Replicas[][] { DeviceUUID, Host, Port };
    Migrations[] { SourceUUID, TStorageDevice Target };
    ReplacementDeviceUUIDs[];
    UnavailableDeviceUUIDs[];
}

TEvLayoutChangedRequest     { Headers = 1, DiskId = 2, ExternalVolumeTabletId = 3 }
-> TEvLayoutChangedResponse { Error = 1 }
```

Extend `TDescribeDiskResponse` with `UnavailableDeviceUUIDs`.

## FilestoreService

**Problem:** support create, resize and delete fast shards. Idempotency and cleanup are separate work.

### Creation/Resize/Deletion

```proto
TCreateFileStoreRequest { + FastShardCount; } // subset of ShardCount
TResizeFileStoreRequest { + FastShardCount; } // desired total; omission preserves it
```

Require `0 < FastShardCount < ShardCount` when fast shards are requested. Populate
`FileShardFileSystemIds` with fast shards and keep ordinary shards in the
directory-routing set.

- **Creation:** in `TCreateFileStoreActor`, add a prepare step to allocate devices.
- **Resize:** in `TAlterFileStoreActor`, same optional intermediate loop.
- **Deletion:** release device sessions before deallocating fast-shard disks. So
  change `TDestroyFileStoreActor` logic to `GetFileSystemTopology -> PrepareDestroy -> Delete from SS`.

```cpp
// Private tablet API.
TPrepareDestroyRequest { FilesystemId }
-> TPrepareDestroyResponse { Error }
```

`PrepareDestroy` checks sessions, finishes teardown and sends `MarkForCleanup -> DeallocateDevices` through the DR proxy.

## Filestore Tablet

### Configuration

**Scope:** persist the DR layout and run device changes through the existing shard configuration path.

```proto
TStorageGroup {
    repeated TStorageDevice Devices;       // current replica slots
    + TDeviceLayout TargetDeviceLayout;
}

TDeviceLayout {
    repeated TDeviceMigration Migrations;       // { SourceUUID, TStorageDevice Target }
    repeated TDeviceReplacement Replacements;   // { BrokenUUID, TStorageDevice Target }
    repeated string UnavailableDeviceUUIDs;
}
```

The tablet matches DR replacement targets to persisted `Devices` by replica slot
to fill `BrokenUUID`. Keep existing pairs for unchanged copies.

Each DR `AllocateDeviceResponse`/`DescribeDeviceResponse` contains the complete current layout. One active reconfiguration 
actor per tablet runs `AllocateDisk -> ConfigureAsShard -> apply device changes`. Reject overlapping DR notifications 
with `E_REJECTED`; DR retries them.

Extend the fast-shard API:

```cpp
IFileSystemShard {
    + TFuture<TError> MigrateDevice(TString sourceUUID, TStorageDevice target);
    + TFuture<TError> ReplaceDevice(TString brokenUUID, TStorageDevice target);
    + TFuture<TError> RevokeDevice(TString deviceUUID);
    + TFuture<TError> PromoteDevice(TString targetUUID);
};
```

`MigrateDevice`/`ReplaceDevice` futures complete after copying and journal catch-up.
Layout updates start copies without waiting for completion; unchanged copies keep running.

Add corresponding private tablet events:

```cpp
TEvMigrateDeviceRequest/Response { OperationId, SourceUUID, TargetUUID } / { Error };
TEvReplaceDeviceRequest/Response { OperationId, BrokenUUID, TargetUUID } / { Error };
TEvRevokeDeviceRequest/Response { OperationId, DeviceUUID } / { Error };
TEvPromoteDeviceRequest/Response { OperationId, TargetUUID } / { Error };
```

`OperationId` identifies a local copy attempt; ignore completions that no longer
match the active operation. Queue copy completions while reconfiguration is busy.

Run completion handling through the same reconfiguration actor. If DR cancels a
copy, call `RevokeDevice(target)`. ACK a DR notification after removed-device I/O
has drained; DR may release those devices on ACK.

```text
Copy completes -> FinishMigration(source, target) or FinishRepair(target)
               -> refresh and persist layout -> PromoteDevice(target)
```

## Storage Group

### Startup

**Scope:** initialize usable replicas without contacting devices already known to be broken.

At startup, SG uses both `Devices` and `TargetDeviceLayout` to exclude known broken
devices and unfinished targets from recovery sources. Device initialization failure
does not restart the tablet: with quorum the shard serves; without it the tablet should boot into RecoveryMode.

### SG replication proxy

**Scope:** copy a device online while keeping normal SG read/write routing.

Internally SG uses `DeviceProxy` for device bookkeeping. `ReplicationProxy` is proposed to tackle concurrent writes/replication.
Main difference is in
- keeping track of replication cursor in SG service data range.
- interlocking between replicating and record ranges, e.g. via `TDisjointIntervalMap`.

Also for purpose of replicating direct page write method should be added:

```proto
// device.proto
TWritePagesRequest {
    string DeviceUUID;
    repeated TDevicePageGroup PageGroups;
}
TWritePagesResponse { Error; }
```

### Configuration management

`MigrateDevice` adds a replication proxy and increases the write quorum by one while the
source and target coexist: `Q+1` of `N+1` intersects every `Q` of `N` quorum of both the
old and the promoted configuration, so no committed record is lost across the switch.

`ReplaceDevice` disables the broken proxy and adds a replication proxy without
changing the write quorum. The target starts voting only after promotion.

`PromoteDevice` verifies copy completion and journal catch-up, then replaces the
replication proxy with a normal device proxy. Quorum stays unchanged.

`RevokeDevice` removes the device from the SG configuration. Removing a migration
source after target promotion, or its target after cancellation, lowers quorum by
one exactly once. Replacement does not change quorum.
