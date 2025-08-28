# Disk Manager cells mechanism

## Problem
Нужно поддержать масштабирование (разделение на cells) сервиса NBS прозрачно для пользователей и для compute.
Disk Manager должен уметь выбирать, в каком cell выгоднее всего создать диск и мочь поддерживать все операции с дисками в cells.

## Detailed Design

### Contracts

`cellSelector.SelectCell` idempotently returns nbsClient for most suitable Cell ID by given zone. If the zone is not divided into cells, or cells are not allowed for the folder, or cells config is not set, returns the original zone nbsClient.

`cellSelector.SelectCellForLocalDisk` finds the only correct cell, where requested `Agent` is located. If the zone is not divided into cells, or cells are not allowed for the folder, or cells config is not set, returns the original zone nbsClient.

### How to get cluster capacity information

... TBD

### How to select shard for non local disks

Добавляем в Disk Manager новый компонент: cellSelector. Через конфиг он получает информацию о том, какие cells относятся к какой зоне, например:

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

Каждая зона является одним из своих Cell.

#### SelectCellForLocalDisk:

```mermaid
sequenceDiagram
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

    Note right of CreateDiskTask: normal execution <br> of the task

    CreateDiskTask->>CellSelector:CellSelected()
    CellSelector->>CellStorage:ClearCellInfo(diskID)
    destroy CellStorage
    CellStorage->>CellSelector:Success
```

For any task, that called from Disk Manager API we should get correct `zoneID` from `diskMeta`.

### SelectCellForLocalDisk

```
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
