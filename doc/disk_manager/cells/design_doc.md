# Disk Manager cells mechanism

## Problem
We need to support scaling (division into cells) of the NBS service transparently for users and compute.
Disk Manager should be able to choose which cell is most advantageous to create a disk in and be able to support all disk operations in cells.

## Detailed Design

### Contracts

`cellSelector.SelectCell` idempotently returns nbsClient for most suitable Cell ID by given zone. If the zone is not divided into cells, or cells are not allowed for the folder, or cells config is not set, returns the original zone nbsClient.

`cellSelector.SelectCellForLocalDisk` finds the only correct cell, where requested `Agent` is located. If the zone is not divided into cells, or cells are not allowed for the folder, or cells config is not set, returns the original zone nbsClient.

`cellSelector.CellSelected` unbinds disk from cell.

### How to get cluster capacity information

To get data about Cell capacity, we will create a `cells.CollectZoneCapacity` task that will be schedulled by default once per hour. Period is configurable.
It will iterate through each of the Cells and get data from the `getClusterCapacity` handler.
After each step, we add the processed cellID to the task state, because on retry we want to avoid repeated calls to the `getClusterCapacity` handler.
If we get an error from the `getClusterCapacity` handler, we ignore it and move to the next Cell. In metrics, we increment the error counter to then set up an alert on this counter.
Repeated calls to `getClusterCapacity` can lead to a sharp increase in load on BSC.

```mermaid
sequenceDiagram
    participant GetCapactiyTask as cells.CollectZoneCapacity
    participant NBS as NBS Cell

    GetCapactiyTask ->>+ NBS: [private API] getClusterCapacity

    create participant BSC as Blob Storage Contoller
    NBS ->> BSC: TEvControllerConfigRequest
    destroy BSC
    BSC ->> NBS: OK, []storagePools, []groups
    NBS ->>- NBS: Aggregate Data From YDB

    create participant DR as Disk Registry
    NBS ->> DR: GetClusterCapacity
    destroy DR
    DR ->> NBS: TEvGetClusterCapacityResponse
    Note over DR: getCapacityResponse<br/>+ kind: StorageMediaKind<br/>+ free: uint64<br/>+ total: uint64
    NBS ->> GetCapactiyTask: TEvGetClusterCapacityResponse

    activate GetCapactiyTask
    GetCapactiyTask ->> GetCapactiyTask: SaveStateWithPreparation (CellID)
    create participant CS as Cells Storage
    GetCapactiyTask ->> CS: UpdateClusterCapacities()
    destroy CS
    CS ->> GetCapactiyTask: OK
    deactivate GetCapactiyTask
```

### How to select shard

We add a new component to Disk Manager: cellSelector. Through configuration, it receives information about which cells belong to which zone, for example:

```
Cells: {
    key: "zone-a"
    value: {
        Cells: [
            "zone-a-cell1",
            "zone-a"
        ]
    }
}
Cells: {
    key: "zone-b"
    value: {
        Cells: [
            "zone-b"
        ]
    }
}
```

Each zone is one of its own Cells.

#### SelectCell:

When selecting a Cell for creating a non-local disk, we first rely on the configuration. If the Folder from the request is allowed, we select the least occupied cell from the requested zone and bind it in CellStorage.
If cells config is not set, we return nbsClient for the requested zone.

```mermaid
sequenceDiagram
    participant CreateDiskTask
    participant CellSelector

    CreateDiskTask->>CellSelector:SelectCell(zone)
    CellSelector->>+CellSelector:Get Most Suitable Cell

    create participant CellStorage
    CellSelector->>CellStorage:BindDiskToCell(diskID, cellID)

    CellStorage->>CellSelector:[idempotent] cellID

    create participant F as nbsFactory
    CellSelector->>F:GetClient(selectedZone)
    destroy F
    F->>CellSelector:nbsClient

    CellSelector->>-CreateDiskTask:nbsClient

    Note right of CreateDiskTask: regular execution <br> of the task

    CreateDiskTask->>CellSelector:CellSelected()
    CellSelector->>CellStorage:UnbindDisk(diskID)
    destroy CellStorage
    CellStorage->>CellSelector:OK
    CellSelector->>CreateDiskTask:OK
```

For any task, that called from Disk Manager's Disks API we should get correct `zoneID` from `diskMeta`.

**Tasks list**

- alter_disk_task
- delete_disk_task (Unnecessary, due to getting correct zoneID from `storage.DeleteDisk`)
- migrate_disk_task
- resize_disk_task
- crete_image_from_disk_task
- create_snapshot_from_disk_task
- stat_disk_task (Should be created. There is no task for `DiskService.Stat` request currently)
- describe_disk_task (Should be created. There is no task for `DiskService.Describe` request currently)

For example, Migrate Disk Task:

```mermaid
sequenceDiagram
    participant migrateTask as Migrate Disk Task
    participant CellSelector

    migrateTask->>CellSelector:SelectCell()
    CellSelector->>migrateTask:Dst NBS Client

    create participant storage as Resources Storage
    migrateTask->>storage:GetDiskMeta()
    destroy storage
    storage->>migrateTask:Src ZoneID

    migrateTask->>migrateTask: SaveState (src ZoneID)
    Note right of migrateTask: regular execution <br> of the task

    migrateTask->>CellSelector:CellSelected()
    CellSelector->>migrateTask:OK
```

### SelectCellForLocalDisk

```mermaid
sequenceDiagram
    participant CreateDiskTask
    participant CellSelector

    CreateDiskTask ->> CellSelector: SelectCellForLocalDisk()

    create participant F as nbsFactory
    CellSelector->>F:GetClient(cell_0)
    F->>CellSelector:nbsClient_0

    create participant NBS0 as nbsClient0
    CellSelector-->>NBS0:QueryAvailableStorage(agentID)
    destroy NBS0
    NBS0-->>CellSelector:empty agent

    CellSelector->>F:GetClient(cell_1)
    F->>CellSelector:nbsClient_1

    create participant NBS1 as nbsClient1
    CellSelector-->>NBS1:QueryAvailableStorage(agentID)
    destroy NBS1
    NBS1-->>CellSelector:Agent

    CellSelector->>CreateDiskTask:nbsClient_1
```

If there are no available agents in any zone, we should return an `errors.NewInterruptExecutionError()`.
