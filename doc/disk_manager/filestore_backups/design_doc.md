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

* ⏳ Disk Manager should be able to back up, or restore up file systems up to 1Pib size within a reasonable timeframe (24h).
* ⚙️ Backup creation and restoration MUST be resilient to node downtime, failure and network errors.
* 📈 Loss of some progress is acceptable as long as filestore backup is finished within a reasonable amount of time.
* ⏱️ Lattency < .1 sec for 99% for all api methods (filesystem backup creation is asynchronous, but we do not slow request for task state or operation).
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
* Node hierarchy topological sort
* File attributes backup
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


Since `NodeRefs` table is indexed by the pair of (`NodeId`, `Name`), we can't efficiently parallel `NodeRefs` backup (we can't split data by some value, since names can vary greatly and to know names distribution we still will need to scan the whole table).
The proposed approach for the `dataplane.BackupNodeReferences` task would be to implement `readnoderefs` action, which would have the following protobuf spec:
```proto

message TReadNodeRefsRequest
{
    // FileSystem identifier.
    string FileSystemId = 1;
    // Parent node ID, we select nodes with parent node id greater
    // to the one in the request.
    uint64 NodeId = 2;
    // File name, we obtain nodes with file name greater alphabetically.
    string Cookie = 3;
    // Limit on the number of entries within the response.
    uint32 Limit = 4;
}


message TReadNodeRefsResponse
{
    NCloud.NProto.TError Error = 1;
    // List of node refs.
    repeated TNodeRef NodeRefs = 2;
    // Parent node ID to start the next request from.
    uint64 NextNodeId = 3;
    // File name to start the next request from.
    string NextCookie = 4;
}

message TNodeRef
{
    // Parent node ID.
    uint64 NodeId = 1;
    // Commit ID (id of the checkpoint the file is present in).
    uint64 CommitId = 2;
    // File name.
    string Name = 3;
    // File node ID.
    uint64 ChildId = 4;
    // ID of a shard.
    string ShardId = 5;
    // Node ID within the shard.
    string ShardNodeName = 6;
}
```
The task will sequentially read `NodeRefsPaginationLimit` entries each time and save them to the database.
After each read, the `NextNodeId` and `NextCookie` are stored to the task state.
To store node metadata, `node_references` table will be used:

```
backup_id: Utf8
parent_node_id: Uint64
node_id: Uint64
name: Utf8
tree_depth: Uint64
shard_id: Utf8
shard_node_id: Utf8
```
primarey key is (`backup_id`, `parent_node_id`, `name`).
additional index is (`tree_depth`, `parent_node_id`, `name`),
additional index is `node_id`.


### 📶Node hierarchy topological sort
To restore the filesystem it is required to sort nodes by their height from the tree root, in order to avoid moving files after creation. Since the backup is restored to a newly created filesystem, there is a requirement to create file tree using standart mechanisms.

#### Algorithm:
* Select all records with `parent=root` with limit.
* For each record, set `tree_depth=0`, save it to the database, save the tuple of (`tree_depth`, `last_seen_parent_node_id`, `last_seen_node_name`) to the task state.
* When iteration is finished
  * Select all records with `parent=previous_parent` with limit.
  * For each record, set `tree_depth=previous_tree_depth+1`, save it to the database, save the tuple of (`last_tree_depth`, `last_parent_node_id`, `last_node_name`) to the task state.
* Repeat until all records are processed.

### 🗂️ File attributes backup
File attributes backup will consist of reading `Nodes` and `NodeAttrs` tables and saving them to the database. For node attributes, `TGetNodeAttrBatchRequest` can be used, for node attributes additional API handle need to be implemented, which seems optional for now.
Attributes backup can be performed in parallel with the node hierarchy topological sort.
Attributes backup can be performed in parallel using the approach used in transfer.go, where channel with inflight queue is used.
Attributes backup will follow this algorithm:
* A separate goroutine reads node refs from the database (either directories or files, files should be read in separate tasks per shard).
* Node refs are read in batches.
* For each batch, reader will read attributes using `TGetNodeAttrBatchRequest` and save them to the database, for each node attributes record, chunk_map entries are created (file size is split into 4MB chunks, so if file size is 10MB, there will be 3 chunks). Chunk indices are extracted from batch index and the index of the node within the batch + index of the chunk within the file. This way there will be no contention.
* Milestone is updated to be equal the biggest batch number, all batches before that are processed.
File attributes are saved to the `node_attributes` table:
```
backup_id: Utf8
parent_node_id: Uint64
name: Utf8
size: Uint64
mtime: Timestamp
atime: Timestamp
ctime: Timestamp
uid: Uint64
gid: Uint64
mode: Uint64
```
primary key is (`backup_id`, `node_id`).

For chunks there must be a separate table `chunk_maps`, which will store the mapping of chunks to files:
```
shard_id: Uint64
backup_id: Utf8
node_id: Uint64
chunk_index: Uint32
chunk_id: Utf8
stored_in_s3: Bool
```
primary key is (`shard_id`, `backup_id`, `node_id`, `chunk_index`).

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

For filesystem hierarchy restoration, we are going to use the separate restoration table (`filesystem_backup_restore_mapping`), which will store from source ids to destination ids:
```
destination_filesystem_id: Utf8
node_id: Uint64
destination_node_id: Uint64
```
primary key is (`destination_filesystem_id`, `node_id`).

To properly restore the filesystem hierarchy, we will read the `node_references` table and create the filesystem tree using the following steps:
1. Read all records from the `node_references` table ordered by `tree_depth`, `parent_node_id`, and `name`.
2. For each record, create a new node in the filesystem, take the parent id in `filesystem_backup_restore_mapping`, create `filesystem_backup_restore_mapping` entry after the node is created.
3. Create the next level of nodes the same way.
At each iteration, store the tuple of (`last_tree_depth`, `last_parent_node_id`, `last_node_name`) to the task state to allow resuming from the last processed node.

### ↩️ Attributes and data restoration
For file attributes and data restoration, the same approach as in the attributes and data backup can be used, but for each node, we will read the destination node id from the `filesystem_backup_restore_mapping` table and restore attributes and data to the destination node.
