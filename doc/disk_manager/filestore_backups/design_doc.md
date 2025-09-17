# Filestore backup technical design

## Requirements

### Functional

* Disk Manager MUST provide an API for:
    - Filesystem backup creation (for an existing filesystem it should create a backup)
    - Filesystem backup restoration (it should create a new filesystem from a backup)
    - Filesystem backup retrieval
    - Filesystem backups listing
    - Filesystem backups deletion with data deletion in the background.
* Disk Manager MUST restore the filesystem backup only to a new (non-existent from the disk-manager point of view) filesystem.
* Backups storage should be data storage type agnostic (Disk Manager should be able to store data either in YDB or in S3).
* There should be an alert on filesystem backup creation estimate exceeded.
* Filesystem backups should support incrementality (if two backups were created from the same filesystem at different points in time, they must not contain duplicate data). // this is the last priority
* Filesystem backups must support cancellation (if the backup is in progress and the task is cancelled, the backup should be stopped and the data should eventually be deleted).
* Filesystem backup can't be restored to the filesystem which is being restored from another backup.

### Non functional

* ⏳ Disk Manager should be able to back up, or restore file systems up to 1Pib size within a reasonable timeframe (24h).
* ⚙️ Backup creation and restoration MUST be resilient to node downtime, failure and network errors.
* 📈 Loss of some progress is acceptable as long as filestore backup is finished within a reasonable amount of time.
* ⏱️ Latency < .1 sec for 99% for all api methods (filesystem backup creation is asynchronous, but we do not slow request for task state or operation).
* 🔢 The system should handle up to 100000 file system backups per datacenter (assuming worst case scenario where clients create small file systems sized several tens of Gib's, the number is derived from order of magnitude of snapshots count in some installations).
* 🔄Idempotency: Filesystem backup creation and deletion must be idempotent.
* 🪨 Consistency (filesystem backup should capture a consistent state of the filesystem at some point in time).

### Disk Manager
Disk Manager provides the following functionality out-of-the box:
* 📝Persistent tasks with dependency handling, resiliency to worker failures or long unavailability, consistent task state, idempotency and retries.
* 🔧 GRPC API with admin command line tools.
* 📚 Common code for transferring contiguous (or at least ordered) sets of data using multiple goroutines and saving the progress.

So, within the Disk Manager app, requirements can be rewritten as a creation of a task (or tasks),
which would adhere to the following requirements:
* The size of the saved task's state does not depend on the size of the filesystem.
* The task implementation allows the task to be re-run multiple times (idempotency).

## Filestore Metadata structure

Filestore metadata consists of various inode (file) attributes and relations between inodes (which inode is a parent of which), it also contains information about which shard stores required file system. Filestore metadata is served by a single tablet (replicated state machine). Tablet stores it's state in several tables which allows indexing by primary key only (local db).
Filestore has code to support checkpoint implementation, but for now, checkpoint creation is not supported.

### Node refs
Information about the directory structure is stored in the `NodeRefs` [table](link_to_table)
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
For sharded filesystems, `ShardId` is the name of the shard, which is a separate filesystem, which stores files in plain structure (files are named by uids).

**IMPORTANT** ID of inodes do not provide any information about which file was created earlier. Id of the shard is encoded into the id of inode.


**IMPORTANT** YDB tablet API does not support querying a table simply by a limit and offset.
The table allows querying only by the id ot the parent and by the name of the file.


Information about inode (atime, ctime, mtime, mod, uid, gid, etc. ) are stored in the `Nodes` [table](link_to_table)
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

To support extended attributes backup we will also need to back up the `NodeAttrs` [table](link_to_table)
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


## Possible solutions:

Filesystem backup consists of backing up the metadata, and file data. Those can be executed either in parallel or consequently.
Both approaches have advantages and disadvantages:
* Parallel metadata and data backup
  * Pros:
    * Better bandwidth utilisation in some cases.
    * Better error & node outages handling, if we can extract ordered metadata.
  * Cons:
    * More complicated state storage
    * Definitely required Filestore code modification and additional API handles
* Sequential processing of metadata and data
  * Pros:
    * Simple separation on two tasks
    * Data backup does not depend on metadata backup
    * Allows us to operate without Filestore modifications
  * Cons:
    * Need to wait for metadata backups before start of backing up the filestore, thus, potentially lower bandwidth.

## Proposed solution
We propose creation of two separate tasks for controlplane and dataplane respectively.
The controlplane task will create a checkpoint and a record about pending filesystem in the database.
Then it will wait for the dataplane task and finalize snapshot creation afterwards.
The backup process should be separated into the following stages:
* Checkpoint creation
* Database record creation
* Node hierarсhy backup
* Data backup.


The restoration would consist of the following stages:
* Filesystem entry creation
* Filesystem creation
* Filesystem hierarchy restoration
* Attribues restoration and data restoration (can be done in parallel).


### 🏁 Checkpoint creation

For the data consistency, it is required to create a checkpoint and to read from the filesystem checkpoint afterwards. Currently this functionality is not implemented and the filesystem backups feature requires checkpoint implementation, but the checkpoint implementation goes beyond the scope of this document.

### 💾 Controlplane record creation

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

After the controlplane record is created, almost identical database entry for controlplane is created.

### 🌳 Node hierarсhy backup


Since `NodeRefs` table is indexed by the pair of (`NodeId`, `Name`), we can't efficiently process  `NodeRefs` backup in parallel (we can't split data by some value, since names can vary greatly and to know names distribution we still will need to scan the whole table). Sharded directories mechanism does not allow us to implement a linear NodeRefs table traversal, so we need to use standard ListNodes API for directories listing. This way we do not depend on the internal structure of the filestore.
The proposed approach for the `dataplane.BackupNodeReferences` is to perform a BFS traversal of the filesystem tree in several parallel workers, whilst utilizing a ydb table as a persistent queue.
For that we will need the folowing `node_references` table:
```
filesystem_backup_id: Utf8
depth: Uint64
parent_node_id: Uint64
name: Utf8
node_id: Uint64
node_type: Uint32
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

The primary key should be (`filesystem_backup_id`, `depth`, `parent_node_id`, `name`).

For the queue we will use the following table:
```
filesystem_backup_id: Utf8
finished: Bool
node_id: Uint64
cookie: Utf8
depth: Uint64
```
The primary key should be (`filesystem_backup_id`, `finished`, `node_id`, `cookie`).

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

###### Hard links:
Hard links are retrieved from the filestore as a regular files with refcnt > 1.

#### Algorithm:
First we emplace the root node into the queue table and then we start several `DirectoryLister`'s and a single `DirectoryListingScheduler`.
`DirectoryListingScheduler` does the following:
1. Reads several records from the queue table with `finished == false` sorted by primary key.
2. For each record checks if there are more records in the queue with the same `node_id` and `finished == true`, or a greater cookie, in that case, it removes records from the table.
3. Puts records in a channel for processing while there are records to process.

`DirectoryLister` does the following:
1. Reads a record from the channel.
2. Performs ListNodes API call for the `node_id` from the record, using the `cookie` from the record if it is not empty.
3. Performs upsert of all the nodes to `node_references` table. (By incrementing depth by one).
4. Puts all the directories into the queue table with `finished == false` and cookie as empty.
5. For all the symlinks, performs `ReadLink` API call and updates the `symlink_target` field in the `node_references` table.
6. For all regular files, generate chunk entries and put them into the `chunk_maps` table.
7. On success, updates the cookie in the queue table and sets `finished == true` if listing did not return anything.

Steps 3,4,5 CAN be performed in parallel.

After all the metadata is backed up, delete all the records from the queue table.


### 💽 Data backup
Data backup will use channel with inflight queue and milestone to store the progress, but there be a slight modification to the inflight queue algorithm, since we will maintain not the number of goroutines, but rather the amount of data being processed. Data is saved to the `chunk_blobs` table.
```
shard_id: Uint64
chunk_id: Utf8
referer_backup_id: Utf8
data: String
compression: Utf8
checksum: Uint32
refcnt: Uint32
```
primary key is (`shard_id`, `chunk_id`, `referer`).


### 🌳↩️ Filesystem hierarchy restoration

We will use channel with inflight queue and milestone to store the process of the restoration of the filesystem hierarchy. We will process the `node_references` table ordered by the primary key.
For the restoration we will use channel with inflight queue and a milestone to store the progress.
We will read the `node_references` table ordered by the primary key.


For the mapping of source node ids to destination node ids we will need the following `filesystem_restore_links_mapping` table:
```
source_filesystem_id: Utf8
destination_filesystem_id: Utf8
source_node_id: Uint64
destination_node_id: Uint64
```
with primary key (`source_filesystem_id`, `destination_filesystem_id`, `source_node_id`).
We will initially prepend the table with the root node mapping (1 -> 1).

##### Algorithm:
1. Read a record from the database
2. Put it into the channel with inflight queue.
3. If the node parent does not exist in the mapping table, put the record back into the channel.
4. Create A file. If node is a link (reference count > 2), check if node itself was created, if not, create it as a regular, otherwise create a link.
5. Put the mapping of source node id to destination node id into the mapping table.
6. On success, notify the channel that the record is processed.
7. update milestone.

### ↩️ Data restoration
For file the  data restoration, the same approach as in the attributes and data backup can be used, but for each node, we will read the destination node id from the `filesystem_backup_restore_mapping` table and restore the data to the destination node.

#### Small files chunking
If the performance would be insuffficient, the smart thing here would be to:
1) Read several node refs records from the database as a single chunk, each chunk would be a limit & offset within the table.
2) Implement a method to create several files in a single API call whilst specifying attributes like atime, mtime, mode, uid, gid, etc.
3) Write small files in a single API call.
4) Add additional shard id to the `node_references` and launch different workers for different shards, whilst implementing a shard locking mechanism.

