# Disk Manager Filestore metadata backup

This document describes the algorithm for backup and restore of the Filestore metadata and the storage schema for backups. Backups are expected to be performed as a Disk Manager dataplane persistent task, which can be interrupted and retried idempotently. Filestore backups are expected to use point-in-time checkpoints. Filestore does not have production-ready checkpoint support yet (see https://github.com/ydb-platform/nbs/issues/1923). Until then, the backups are expected to be inconsistent.

## Proposed solution

### Backup:
The filestore backup creation should consist of the following steps:
* API request handling, controlplane task creation
* Controlplane task, which creates a record in the controlplane database, creates a checkpoint, and schedules the dataplane metadata backup task.
* Metadata backup task, which reads all the metadata from the checkpoint and writes it to the backup storage.

### Restoration:
* API request handling, controlplane task creation
* Controlplane task, which creates a filestore and the corresponding record in the controlplane database, and schedules a dataplane restoration task.
* Dataplane task, which reads all the metadata from the backup storage and writes it to the filestore.

## Filestore Metadata structure

Filestore metadata consists of inode attributes and the directory structure of the filesystem. A tablet stores its state in several tables, which allows indexing by primary key only.


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
Note that `NodeId` here is the ID of a parent node. In the Disk Manager terminology, we will stick to the terminology _parent id_ and _child id_.
For sharded filesystems, `ShardId` is the ID of the shard, which is a separate filesystem that stores files in a flat structure. `ShardName` is the name of the file in this shard filesystem.


**IMPORTANT** The YDB tablet API does not support querying a table simply by a limit and an offset. The table allows querying only by the ID of the parent and by the name of the file.


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
size: Uint64
storage_size: Uint64
status: Int64
```

After the controlplane record is created, an almost identical database entry for dataplane is created.

## Dataplane metadata backup

The proposed approach for the `dataplane.BackupMetadata` is to perform a BFS-like traversal of the filesystem tree via several parallel workers, whilst using a YDB table as a persistent queue.

### NodeRefs

The `NodeRefs` table is indexed by the pair of `(NodeId, Name)`. We will use the standard ListNodes API for directory listing. This way, we do not depend on the internal structure of the filestore.
For that, we will need the following `filesystems/node_refs` table:
```
filesystem_backup_id: Utf8
depth: Uint64
parent_node_id: Uint64
name: Utf8
child_node_id: Uint64
node_type: Uint32
```

The primary key should be `(filesystem_backup_id, depth, parent_node_id, name)`.

### Nodes

Node attributes should be stored in the `filesystems/nodes` table:
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
with primary key `(filesystem_backup_id, node_id)`.

### Directory listing queue

For the queue, we will use the following table `filesystems/directory_listing_queue`:
```
filesystem_backup_id: Utf8
node_id: Uint64
cookie: Bytes
depth: Uint64
```
The primary key should be `(filesystem_backup_id, node_id, cookie)`.

### Hard links:
Hard links are visible as files with `Links > 1`. They turn the filesystem tree into an acyclic graph and add some complexity. To tackle this issue, we will store such references in a separate table and restore these references at the very end of the metadata backup restore. `filesystems/hardlinks` will have the following schema:
```
filesystem_backup_id: Utf8
node_id: Uint64
parent_node_id: Uint64
name: Utf8
```
With primary key `(filesystem_backup_id, node_id, parent_node_id, name)`.

## `dataplane.BackupMetadata` task
*  First, it emplaces the root node into the queue table if one does not exist yet. Several `DirectoryLister` goroutines and a single `DirectoryListingScheduler` goroutine are spawned.
* `DirectoryListingScheduler` does the following:
    1. Check if root record was scheduled, if not, schedule root node as pending and update the task state in the same transaction (`SaveStateWithPreparation()`).
    2. Fetch all inodes to list with the `listing` state and put them into the channel.

* `DirectoryLister` does the following:
    1. Reads a record from the channel.
    2. Performs ListNodes API call for the `node_id` from the record, using the `cookie` from the record if it is not empty.
    3. Performs upsert of all the nodes to `filesystems/node_refs` table. (By incrementing the depth of the parent from the queue by one). For this, we can use the `BulkUpsert` API call. (`saveNodeRefs()`)
    4. For all the symlinks, performs `ReadLink` API call to populate the node data.
    5. For all the inode attributes saves them to the `filesystems/nodes` table. (`saveNodes()`)
    6. For all the files with `Links > 1`, saves them to the `filesystems/hardlinks` (`saveHardlinks()`). Links is returned in `ListNodes` API call. All the files with `Links > 1` are to be saved to the `filesystems/node_refs` table with type Link.
    7. Puts all the directories into the queue table with empty cookie. (`enqueueDirectoriesToList()`)
    8. In the same transaction as 7, updates the cookie and removes the record from the queue if the inode listing was finished.
    9. Continues 2-8 with the new cookie until there are no more entries to process. 

### Metadata restore

For the metadata restoration, we will restore the filesystem layer-by-layer, and within each layer we will restore nodes in parallel using multiple workers and the channel-with-inflight-queue approach, incrementing the depth after each layer is restored.

For the mapping of source node ids to destination node ids we will need the following `filesystem/restore_mapping` table:
```
source_filesystem_id: Utf8
destination_filesystem_id: Utf8
source_node_id: Uint64
destination_node_id: Uint64
```
with primary key `(source_filesystem_id, destination_filesystem_id, source_node_id)`.
We will initially prepend the table with the root node mapping (1 -> 1). (Root node id is always 1).

In the task state we will need to store the current depth and the index of the chunk of fixed size, all the chunks before which are already processed.
To split each layer into chunks, we need to implement a `CountNodesByDepth()` method.
Large number of hard links is unexpected. To simplify the implementation hardlinks are to be restored separately after all the layers are restored.
We will fetch data from the hardlinks table create the first entry with the same inode as a file and others as a link.
