# Filestore backup technical design

## Requirements

## Functional

* Create filesystem backup stored in YDB tables (task `dataplane.CreateSnapshotFromFilesystem` ).
* Restore filesystem backup to a newly created filesystem (task `dataplane.CreateFilesystemFromSnapshot`)
* API handles and respective controlplane tasks.
* At the first iteration we do not care for data consistency. This issue will be solved via Filestore checkpoint creation.
* We need to backup metadata and file contents.

## Non-functional
* Ability to back up large filesystems with size ~ 1 Pb in a reasonable amount of time. (e.g. 24 hours)
* Outage of any dataplane & controlplane worker would not affect the backup process. (Some of the metadata backup progress can be lost)
* Filesystem backup MUST be idempotent
* Filesystem backup MUST be consistent (filesystem write operations should not affect current backup's data integrity)
* Consistently high bandwidth. Bandwidth should not change independently of the file size. 
* Incremental filesystem backups shall be implemented. (After the required features in filestore are supported).

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

### Metadata backup
We propose two tasks approach, one for metadata and one for data.
The metadata backup would require creating a new GRPC API handle for the filestore server. In the first iteration, we would simply 
traverse all directories recursively in a single thread (BFS), but this approach would prevent us from utilising multiple threads and 
prevent the metadata backup task recovery (it would be really complicated to determine which nodes were traversed). 

Metadata backup filestore API messages should have the following format:
```protobuf
syntax = "proto3";

message NodeRequest{
  uint64 FilesystemId = 1;
  uint64 CommitId = 2;
  uint64 limit = 3;
  uint64 offset = 4;
}

message NodeResponse{
  uint64 NodeId = 1;
  uint64 CommitId = 2;
  uint32 Uid = 3;
  uint32 Gid = 4;

  uint64 ATime = 5;
  uint64 MTime = 6;
  uint64 CTime = 7;

  uint64 Size = 8;
  string SymLink = 9;
  string ShardId = 10;
  uint64 ParentId = 11;
  // Also should store key-value attributes
}
```
Backup process would consist of:
1. Filesystem database entry creation.
2. Checkpoint creation.
3. Collect the total number of files.  
4. Place the list of file in the queue and spawn workers, which would save all the inodes to the inodes table.
5. Update the milestone to the inode batch number, all batches after which are already processed.

In case of single threaded filesystem traversal, we would prepend file tree depth to the inode table for quicker restoration, and implement
 a simple BF traversal. We would need to store tree structure in-memory to avoid cyclic directories listing. 

Metadata restore process would consist of:
1. Filesystem creation
2. Stream inodes extraction from the database and upload them to the Filestore API handle using multiple workers and a queue.

Metadata restore process without filestore API handles implementation would require us to sort inodes based on their deepth in the file tree, to create diectories first, and files later.

## Data backup
In case of sequential metadata and data backup (and restoration, respectively), data backup can be significantly simplified.
Data backup would consist of the following steps:
1. Create a routine which would extract (inodes, file size, shard ID) tuples from the database and place pairs of (inode id, chunk number, shard ID) to the inflight requests queue.
2. Create workers which would extract files from database and write them to the filesystem. For each file, before data write requiest the size allocation should be performed.

## Potential issues & improvements:
* Without checkpoint implementation, we MUST handle file's resize and deletion by the filesystem client while we are performing the backup. In that case, the error should not be retried, but should be reported to logs & monitoring system.
* Question about open files, how many can we open, can we perform parallel write to a single handle, can we perform writes to a file without allocation. Can we perform parallel writes to the same handle.
* We need to re-scale the worker pool in accordance to the next K files (e.g. we are to process 10000 files with size 10Bytes each), given we do not have a capability for batch processing, we need to increase the number of workers.
* We need to increase and decrease the number of workers according to the lattency. 
