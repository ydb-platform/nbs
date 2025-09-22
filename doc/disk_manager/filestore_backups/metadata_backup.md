# Disk Manager Filestore metadata backup

This document describes the algorithm for backup and restore of the Filestore metadata and the storage schema for backups. Backups are expected to be performed as a Disk Manager dataplane persistent task, which can be interrupted and retried idempotently. Filestore backups are expected to use point-in-time checkpoints.

## Proposed solution

### Backup:
The filestore backup creation should consist of the following steps:
* API request handling, controlplane task creation
* Controlplane task, which creates a record in the controlplane database, creates a checkpoint and schedules dataplane metadata backup task.
* Metadata backup task, which reads all the metadata from the checkpoint and writes it to the backup storage.
* Controlplane task MUST wait for the dataplane task to finish and mark the backup as ready.

### Restoration:
* API request handling, controlplane task creation
* Controlplane task, which creates a filestore and the corresponding record in the controlplane database, and schedules dataplane restoration task.
* Dataplane task, which reads all the metadata from the backup storage and writes it to the filestore.
* Controlplane task MUST wait for the dataplane task to finish and mark the filestore as ready.

## Filestore Metadata structure

Filestore metadata consists of various inode (file) attributes and relations between inodes (which inode is a parent of which), it also contains information about which shard stores the required file system. Filestore metadata is served by a single tablet (replicated state machine), whilst data is served by multiple tablets. Tablet stores its state in several tables, which allows indexing by primary key only (local db).
Filestore does not have production-ready checkpoint support, but it is expected to be implemented soon. Until then, backups are expected to be inconsistent with future critical alerts on errors caused by inconsistencies.

### Node refs
Information about the directory structure is stored in the `NodeRefs` [table](https://github.com/ydb-platform/nbs/blob/40d2878cd3c878c53f8a4946ede2ee47ff43e8f6/cloud/filestore/libs/storage/tablet/tablet_schema.h#L218)
```cpp
struct NodeRefs: TTableSchema<9>
{
    struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
    struct CommitId     : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
    struct Name         : Column<3, NKikimr::NScheme::NTypeIds::String> {};
    struct ChildId      : Column<4, NKikimr::NScheme::NTypeIds::Uint64> {};
    struct ShardId      : Column<5, NKikimr::NScheme::NTypeIds::String> {};
    struct ShardName    : Column<6, NKikimr::NScheme::NTypeIds::String> {};

    using TKey = TableKey<NodeId, Name>;

    using TColumns = TableColumns<
        NodeId,
        CommitId,
        Name,
        ChildId,
        ShardId,
        ShardName
    >;

    using StoragePolicy = TStoragePolicy<IndexChannel>;
};
```
Note that `NodeId` here is the id of a parent node, whereas `ChildId` is the id of a child node,
in the Disk Manager terminology, we would prefer the terminology parent id and child id.
For sharded filesystems, `ShardId` is the id of the shard, which is a separate filesystem that stores files in plain structure (files are named by uids).`ShardName` is the name of the file in the shard fs.

**IMPORTANT** IDs of inodes do not provide any information about which file was created earlier. The index of the shard is encoded into the ID of the inode, the id of the shard id of the filesystem + "_" + index of the shard (0..N).


**IMPORTANT** YDB tablet API does not support querying a table simply by a limit and offset.
The table allows querying only by the id of the parent and by the name of the file.


Information about inode (atime, ctime, mtime, mod, uid, gid, etc. ) is stored in the `Nodes` [table](https://github.com/ydb-platform/nbs/blob/40d2878cd3c878c53f8a4946ede2ee47ff43e8f6/cloud/filestore/libs/storage/tablet/tablet_schema.h#L137)
```cpp
struct Nodes: TTableSchema<5>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CommitId     : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto        : ProtoColumn<3, NProto::TNode> {};

        using TKey = TableKey<NodeId>;

        using TColumns = TableColumns<
            NodeId,
            CommitId,
            Proto
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };
```

To support extended attributes backup, we will also need to back up the `NodeAttrs` [table](https://github.com/ydb-platform/nbs/blob/40d2878cd3c878c53f8a4946ede2ee47ff43e8f6/cloud/filestore/libs/storage/tablet/tablet_schema.h#L174)
```cpp
struct NodeAttrs: TTableSchema<7>
{
    struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
    struct CommitId     : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
    struct Name         : Column<3, NKikimr::NScheme::NTypeIds::String> {};
    struct Value        : Column<4, NKikimr::NScheme::NTypeIds::String> {};
    struct Version      : Column<5, NKikimr::NScheme::NTypeIds::Uint64> {};

    using TKey = TableKey<NodeId, Name>;

    using TColumns = TableColumns<
        NodeId,
        CommitId,
        Name,
        Value,
        Version
    >;

    using StoragePolicy = TStoragePolicy<IndexChannel>;
};
```

## Controlplane record creation

Controlplane database entry should have the following schema:
```
folder_id: Utf8
zone_id: Utf8
filesystem_id: Utf8
filesystem_backup_id: Utf8
creating_at: Timestamp
created_at: Timestamp
deleting_at: Timestamp
deleted_at: Timestamp
incremental: Bool
size: Uint64
storage_size: Uint64
status: Int64
```

After the controlplane record is created,an almost identical database entry for dataplane is created.

## Dataplane metadata backup


Since the `NodeRefs` table is indexed by the pair of (`NodeId`, `Name`), we can't efficiently process the `NodeRefs` backup in parallel (we can't split the data by some value, since names can vary greatly, and to know the names distribution, we still will need to scan the whole table). The sharded directories mechanism does not allow us to implement a linear NodeRefs table traversal, so we need to use the standard ListNodes API for directory listing. This way, we do not depend on the internal structure of the filestore.
The proposed approach for the `dataplane.BackupNodeReferences` is to perform a BFS-like traversal of the filesystem tree in several parallel workers, whilst utilizing a ydb table as a persistent queue. (Note that this is not an actual BFS, since we process records in parallel, we can process nodes of different depths at the same time).
For that, we will need the following `filesystems/node_references` table:
```
filesystem_backup_id: Utf8
depth: Uint64
parent_node_id: Uint64
name: Utf8
node_id: Uint64
node_type: Uint32
```

The primary key should be (`filesystem_backup_id`, `depth`, `parent_node_id`, `name`).
Node attributes should be stored in the `filesystems/node_attributes` table:
```
filesystem_backup_id: Utf8
node_id: Uint64
mode: Uint32
uid: Uint32
gid: Uint32
atime: Uint64
mtime: Uint64
ctime: Uint64
size: Uint64
symlink_target: Utf8
```
with primary key (`filesystem_backup_id`, `node_id`).

For the queue, we will use the following table `filesystems/directory_listing_queue`:
```
filesystem_backup_id: Utf8
status: Uint32 // e.g. pending, listing, finished
node_id: Uint64
cookie: Utf8
depth: Uint64
```
The primary key should be (`filesystem_backup_id`, `status`, `node_id`, `cookie`).

### Hard links:
Hard links are retrieved from the filestore as regular files with refcnt > 1. However, hardlinks are restored as files with `node_type`, Link and we do not wish to have a race condition whilst restoring the same inode multiple times (we need to restore the inode only once and restore the other pair of (parent_id, name) as a link).
To avoid conflicts, we will store hardlink information in a separate table (`filesystems/hardlinks`):
```
filesystem_backup_id: Utf8
node_id: Uint64
depth: Uint64
parent_node_id: Uint64
name: Utf8
```
With primary key (`filesystem_backup_id`, `node_id`, `depth`, `parent_node_id`, `name`).
After backing up the metadata, we will traverse this table sorted by the primary key and update statuses for each entry in the `node_references` table to `Link` (the first entry for each inode will be kept as a regular file).

### Algorithm:
First, we emplace the root node into the queue table if it does not exist with pending status, and then we start several `DirectoryLister`'s and a single `DirectoryListingScheduler`.
`DirectoryListingScheduler` does the following:
1. At the start of the task (it can be restarted multiple times),
2. Reads all the `listing` records (from task's  previous run) and puts them into a channel (`fetchListingDirectories()`).
3. In a loop, reads pending directory records, mark all the pending records as listing in the same transaction. (`lockDirectoriesToList()`). If there are currently `listing` records, remove `pending` duplicates with the same node id.
4. Puts locked records into the channel for processing.
5. Finishes if there are no unfinished records in the queue table.

`DirectoryLister` does the following:
1. Reads a record from the channel.
2. Performs ListNodes API call for the `node_id` from the record, using the `cookie` from the record if it is not empty.
3. Performs upsert of all the nodes to `filesystems/node_references` table. (By incrementing the depth of the parent from the queue by one). For this, we can use the `BulkUpsert` API call. (`saveNodeReferences()`)
4. Puts all the directories into the queue table with `status = pending` and cookie as empty. (`enqueueDirectoriesToList()`)
5. For all the symlinks, performs `ReadLink` API call to populate the node data.
6. For all the inode attributes saves them to the `filesystems/node_attributes` table. (`saveNodeAttributes()`)
7. For all the files with `refcnt > 1`, saves them to the `filesystems/hardlinks` (`saveHardlinks()`). Refcnt is returned in `ListNodes` API call.
8. If hardlinks are present in the listing, update the `node_type` field in node references table to `Link` for all but the first by the primary key of the hardlinks. (`processHardlinks()`)
9. On success, updates the cookie in the queue table and sets `status == finished` if listing did not return anything. Otherwise continue listing in the next iteration.

After all the metadata is backed up, delete all the records from the queue table by the given `filesystem_backup_id`.

### Metadata restore

For the metadata restoration, we will restore the filesystem layer-by-layer, and within each layer we will restore nodes in parallel using multiple workers and the channel-with-inflight-queue approach, incrementing the depth after each layer is restored.

For the mapping of source node ids to destination node ids we will need the following `filesystem/restore_mapping` table:
```
source_filesystem_id: Utf8
destination_filesystem_id: Utf8
source_node_id: Uint64
destination_node_id: Uint64
```
with primary key (`source_filesystem_id`, `destination_filesystem_id`, `source_node_id`).
We will initially prepend the table with the root node mapping (1 -> 1). (Root node id is always 1).

In the task state we will need to store the current depth and the index of the chunk of fixed size, all the chunks before which are already processed.
To split each layer into chunks, we need to implement a `CountNodesByDepth()` method.
Large number of hard links is unexpected. To simplify the implementation hardlinks are to be restored separately after all the layers are restored.

