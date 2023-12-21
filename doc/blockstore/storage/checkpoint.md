# Checkpoints

## General information

NBS can create checkpoints on disks. Checkpoints are used for the following purposes:
- read data from the disk as it was when the checkpoint was created.
- get the bitmask of the blocks that changed between two checkpoints.

Blockstore API provides the following requests for working with checkpoints:
- `CreateCheckpoint(disk_id, checkpoint_id, checkpoint_type)`: creates a checkpoint with id `checkpoint_id` of type `checkpoint_type` on disk `disk_id`.
- `DeleteCheckpoint(disk_id, checkpoint_id)`: deletes the checkpoint with id `checkpoint_id` on disk `disk_id`.
- `GetChangedBlocks(disk_id, start_index, blocks_count, low_checkpoint_id, high_checkpoint_id)`: gets the bitmask of the changed blocks between `low_checkpoint_id` and `high_checkpoint_id` on disk `disk_id` for the block range of length `blocks_count` starting with `start_index`. I.e. the result is the bitmask of size `blocks_count` where bit '1' stands for the blocks that changed between low_checkpoint and high_checkpoint.
- `ReadBlocks`: takes `checkpoint_id` as a parameter, reads data as it was when the checkpoint was created.
- (private api) `DeleteCheckpointData(disk_id, checkpoint_id)`: deletes data for checkpoint `checkpoint_id` on disk `disk_id`. (See below for more details on checkpoint data.)

## Creation and deletion of checkpoints.

The history of create checkpoint and delete checkpoint requests is stored persistently in the volume tablet's local database. If the volume tablet reboots, information about all alive checkpoints will be restored from the history, so the volume will not forget alive checkpoints. (The checkpoint is called alive if it was created and was not deleted.) 

Checkpoint creation is idempotent, i.e. checkpoint creation does nothing if there exists an alive checkpoint with the same id. Checkpoint deletion is also idempotent, i.e. checkpoint deletion does nothing if a checkpoint with such id does not exist (was deleted or never existed).

If a checkpoint was deleted earlier, one should not create a checkpoint on the same disk with the same id. If one tries to do that, the CreateCheckpoint request will probably return an error. But the request might succeed because information about the checkpoint deletion could have been erased from the history.

One can not create a checkpoint with empty id.

## Checkpoints with data

The checkpoint either *has data* or *has no data* (we also say *checkpoint with data* and *checkpoint without data*). Checkpoints with data make sense for blob storage-based disks only. For blob storage-based disks, checkpoints with data are checkpoints we can correctly read from.

A checkpoint can be created either with or without data. One can delete data for an existing checkpoint using a `DeleteCheckpointData` request. So the "life cycle" of the checkpoint includes the following stages: "Checkpoint does not exist" -> "Checkpoint exists and has data" -> "Checkpoint exist and has no data" -> "Checkpoint was deleted". One of the stages "Checkpoint exists and has data" and "Checkpoint exists and has no data" can be skipped, but if the checkpoint has no data, it will never have data again.

If the checkpoint has data, it forbids to delete the blobs needed for reading from this checkpoint. Namely, some blob commit id corresponds to each checkpoint with data, and a garbage collection barrier for this commit id is set up. If we don't read from the checkpoint anymore, we should delete its data -- otherwise garbage collection will be stuck.

Some usecases of checkpoints with/without data:
- Incremental snapshots: When we create a snapshot, we transfer the diff between two checkpoints. For this purpose we need the data of the high checkpoint but we don't need the data of the low checkpoint. When we create the next snapshot, the high checkpoint becomes the lower one, so we don't need its data anymore. So checkpoints for incremental snapshots are created with data, but then their data is deleted.
- Relocation: We don't need checkpoint data for relocation, so checkpoints for relocation are created without data.

## Checkpoint types

Three types of checkpoint can be specified in a CreateCheckpoint request:
- Normal
- Light
- WithoutData
Normal checkpoints and checkpoints WithoutData are essentially the same, the only difference is that Normal checkpoints are created with data and checkpoints WithoutData are created without data. For this reason, we will distinguish only Light and Normal types of checkpoints.

Light checkpoints are used for relocation of disk registry-based disks. Though Light checkpoints can be created for all types of disks, there are no usecases of Light checkpoints for blob storage-based disks. Light checkpoints never have data. Reading from a Light checkpoint is the same as reading without a checkpoint: the last version of each block is read.

Normal checkpoints can be created for all types of disks, but **for disk registry-based disks, write requests are not allowed if the disk has at least one Normal checkpoint**. Normal checkpoints are used for blob storage-based disks (the usecases include creating snapshots, base disk checkpoint and relocation). Normal checkpoints for disk registry-based disks are used for creating snapshots with blocking write requests during the snapshot creation. The feature of creating snapshots of disk registry-based disks without blocking write requests is currently in development.

Light and Normal checkpoints have some more differences, we talk about them below.

## Used blocks tracking and GetChangedBlocks

For every write request, consider the blocks used by this request. Information about these blocks can be saved and then used for finding the changed blocks for GetChangedBlocks requests. Normal checkpoints use information saved persistently. TODO describe in more details, what kind of information is saved, where it is saved and in which cases it is saved. If a disk has at least one Light checkpoint, some information about the changed blocks is also kept in memory.

The guarantees provided by the GetChangedBlocks request for Light checkpoints are weaker than the ones for Normal checkpoints. In case of Normal checkpoints, GetChangedBlocks returns the bitmask of the blocks that changed after the creation of low_checkpoint but before the creation of high_checkpoint (the block is considered changed if it was touched by at least one write request, even if its content hasn't actually changed). In case of Light checkpoints everything is the same, but GetChangedBlocks is allowed to return the bitmask that includes all blocks of the requested range (i.e. bitmask of all '1's) instead of the correct bitmask.

## More details on GetChangedBlocks for Light checkpoints

Note that GetChangedBlocks has no parameter explicitly specifying the checkpoint type. But each valid GetChangedBlocks request is treated either as Normal or Light according to the types of the checkpoints in this request. Normal and Light GetChangedBlocks requests are handled in different ways. Normal requests are forwarded to the partition tablet(s), while Light requests are handled in the volume tablet. Normal GetChangedBlocks requests for disk registry-based disks return an error.

The type of the GetChangedBlocks request (Normal or Light) is deduced by the following rules:
- Both checkpoints are Normal, or one of them is empty and another one is Normal. Then the request is Normal.
- Both checkpoints are Light, or one of them is empty and another one is Light. Then the request is Light.
- Both checkpoints are empty and the disk is blob storage-based. Then the request is Normal.
- Both checkpoints are empty and the disk is disk registry-based. Then the request is Light.
- One of the checkpoints is Light and another one is Normal. Then the request is invalid, an error will be returned.
- One of the checkpoints is nonempty and a checkpoint with such an id does not exist. Then the request is invalid, an error will be returned.

Information about the changed blocks for all Light checkpoints on the disk is represented as two bitmasks. Namely, let `current_ch` be the last Light checkpoint on the disk and `previous_ch` be the second to last Light checkpoint. Then we remember the bitmask of the blocks that changed between `previous_ch` and `current_ch` and the bitmask of the blocks that changed after `current_ch`. All the information about the changed blocks for older Light checkpoints is forgotten.

These two bitmasks of changed blocks are a part of the volume tablet's in-memory state, they are not saved persistently. It means that if the volume tablet reboots, all the information about the changed blocks for the Light checkpoints on the disk will be forgotten.

The idea of the GetChangedBlocks request for Light checkpoints is the following: if the volume 'knows' the correct bitmask of the changed blocks, it returns the correct bitmask. Otherwise, it returns the bitmask of all '1's.

Note that such behavior of GetChangedBlocks might affect efficiency but it never affects correctness. Indeed, the bitmask of all '1's might force the client to replicate the entire disk instead of replicating the changed blocks only. But this never causes data corruption (one can think about this as if there was some write request that touched every block on the disk but didn't actually change any data).

The best pattern for using Light checkpoints is the following. The client creates checkpoint 1, gets changed blocks between empty checkpoint and checkpoint 1, creates checkpoint 2, get changed blocks between checkpoint 1 and checkpoint 2, and so on. Such a pattern is used for disk relocation. The design of Light checkpoints makes this pattern efficient for disk registry-based disks: we don't pay the overhead cost for saving bitmasks to the database. On the other hand, if the client doesn't use this pattern or the volume tablet reboots at some moment, the client will lose in performance because of the bitmasks of all '1's.
