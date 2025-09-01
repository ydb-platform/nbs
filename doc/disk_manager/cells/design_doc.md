# Disk Manager cells mechanism

## Problem
We need to support scaling (division into cells) of the NBS service transparently for users and compute.
Disk Manager should be able to choose which cell is most advantageous to create a disk in and be able to support all disk operations in cells.

## Detailed Design

### Contracts

`cellSelector.SelectCell` idempotently returns nbsClient for most suitable Cell ID by given zone. If the zone is not divided into cells, or cells are not allowed for the folder, or cells config is not set, returns the original zone nbsClient.

`cellSelector.SelectCellForLocalDisk` finds the only correct cell, where requested `Agent` is located. If the zone is not divided into cells, or cells are not allowed for the folder, or cells config is not set, returns the original zone nbsClient.

### How to get cluster capacity information

... TBD

### How to select shard for non local disks

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
```

Each zone is one of its own Cells.

#### SelectCell:

```mermaid
sequenceDiagram
    participant CreateDiskTask
    participant CellSelector

    CreateDiskTask->>CellSelector:SelectCell(zone)
    CellSelector->>+CellSelector:Verify

    create participant CellStorage
    CellSelector->>CellStorage:PickCell(diskID, cellID)

    CellStorage->>CellSelector:[idempotent]cellID

    create participant F as nbsFactory
    CellSelector->>F:GetClient(selectedZone)
    destroy F
    F->>CellSelector:nbsClient

    CellSelector->>-CreateDiskTask:nbsClient

    Note right of CreateDiskTask: regular execution <br> of the task

    CreateDiskTask->>CellSelector:CellSelected()
    CellSelector->>CellStorage:ClearCellInfo(diskID)
    destroy CellStorage
    CellStorage->>CellSelector:Success
```

For any task, that called from Disk Manager API we should get correct `zoneID` from `diskMeta`.

For example, Migrate Disk Task:

```mermaid
sequenceDiagram
    participant migrateTask as Migrate Disk Task
    participant CellSelector

    migrateTask->>CellSelector:SelectCell()
    destroy CellSelector
    CellSelector->>migrateTask:Dst NBS Client

    create participant storage as Resources Storage
    migrateTask->>storage:GetDiskMeta()
    destroy storage
    storage->>migrateTask:Src ZoneID

    migrateTask->>migrateTask: SaveState (src ZoneID)
    Note right of migrateTask: regular execution <br> of the task
```

### SelectCellForLocalDisk

```mermaid
sequenceDiagram
    participant CreateDiskTask as CreateDiskTask
    participant CellSelector as CellSelector

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

If there is no available agents in any zone, we should return an `errors.NewInterruptExecutionError()`.
