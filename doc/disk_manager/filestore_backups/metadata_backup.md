# Disk Manager Filestore metadata backup

This document describes algorithm of backup and restore of Filestore metadata and the storage schema for backups. Backups are expected to be performed as a Disk Manager dataplane persistent task, which can be interrupted and retried idempotently. Filestore backups are expected to use point-in-time checkpoints.

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

Filestore metadata consists of various inode (file) attributes and relations between inodes (which inode is a parent of which), it also contains information about which shard stores required file system. Filestore metadata is served by a single tablet (replicated state machine), whilst data is served by multiple tablets. Tablet stores it's state in several tables which allows indexing by primary key only (local db).
Filestore has code to support checkpoint implementation, but for now, checkpoint creation is not supported.

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
Note, that `NodeId` here is the id of a parent node, whereas `ChildId` is id of a child node,
in the Disk Manager terminology we would prefer the terminology parent id and child id.
For sharded filesystems, `ShardId` is the id of the shard, which is a separate filesystem, which stores files in plain structure (files are named by uids).`ShardName` is a name of the file in the shard fs.

**IMPORTANT** IDs of inodes do not provide any information about which file was created earlier. The index of the shard is encoded into the ID of the inode, the id of the shard id of the filesystem + "_" + index of the shard (0..N).


**IMPORTANT** YDB tablet API does not support querying a table simply by a limit and offset.
The table allows querying only by the id of the parent and by the name of the file.


Information about inode (atime, ctime, mtime, mod, uid, gid, etc. ) are stored in the `Nodes` [table](https://github.com/ydb-platform/nbs/blob/40d2878cd3c878c53f8a4946ede2ee47ff43e8f6/cloud/filestore/libs/storage/tablet/tablet_schema.h#L137)
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

To support extended attributes backup we will also need to back up the `NodeAttrs` [table](https://github.com/ydb-platform/nbs/blob/40d2878cd3c878c53f8a4946ede2ee47ff43e8f6/cloud/filestore/libs/storage/tablet/tablet_schema.h#L174)
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
id: Utf8
folder_id: Utf8
zone_id: Utf8
filesystem_id: Utf8
backup_id: Utf8
creating_at: Timestamp
created_at: Timestamp
deleting_at: Timestamp
deleted_at: Timestamp
incremental: Bool
size: Uint64
storage_size: Uint64
status: Int64
```

After the controlplane record is created, almost identical database entry for dataplane is created.

## Dataplane metadata backup


Since `NodeRefs` table is indexed by the pair of (`NodeId`, `Name`), we can't efficiently process  `NodeRefs` backup in parallel (we can't split data by some value, since names can vary greatly and to know names distribution we still will need to scan the whole table). Sharded directories mechanism does not allow us to implement a linear NodeRefs table traversal, so we need to use standard ListNodes API for directories listing. This way we do not depend on the internal structure of the filestore.
The proposed approach for the `dataplane.BackupNodeReferences` is to perform a BFS-like traversal of the filesystem tree in several parallel workers, whilst utilizing a ydb table as a persistent queue. (Note that this is not an actual BFS, since parallel )
For that we will need the following `node_references` table:
```
filesystem_backup_id: Utf8
depth: Uint64
parent_node_id: Uint64
name: Utf8
node_id: Uint64
node_type: Uint32
```

The primary key should be (`filesystem_backup_id`, `depth`, `parent_node_id`, `name`).
Node attributes should be stored in the `node_attributes` table:
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
refcnt: Uint32
```
with primary key (`node_id`).

For the queue we will use the following table:
```
filesystem_backup_id: Utf8
status: Uint32 // e.g. pending, listing, finished
node_id: Uint64
cookie: Utf8
depth: Uint64
```
The primary key should be (`filesystem_backup_id`, `status`, `node_id`, `cookie`).

For data chunks there must be a separate table `chunk_maps`, which will store the mapping of chunks to files:
```
shard_id: Uint64
backup_id: Utf8
node_id: Uint64
chunk_index: Uint32
chunk_id: Utf8
stored_in_s3: Bool
```
primary key is (`shard_id`, `backup_id`, `node_id`, `chunk_index`).

### Hard links:
Hard links are retrieved from the filestore as a regular files with refcnt > 1.
