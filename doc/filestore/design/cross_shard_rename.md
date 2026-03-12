# RenameNode with directories in shards

The feature itself is implemented here: https://github.com/ydb-platform/nbs/issues/2674

With this feature RenameNode operation becomes a lot more complex because it might require a cross-shard transaction which might be impossible to execute either in the shard in charge of the source directory or in the shard in charge of the parent directory.

Because of this we need to do something similar to 2PC. A simplified outline of what happens:
1. RenameNodeRequest is initially sent to the shard in charge of the source parent dir
2. Source shard checks whether it can perform the op on its side, takes all the required locks and sends the request to the destination shard
3. The destination shard checks whether it can perform the op on its side, performs the op and saves the response persistently to its localdb to make sure that potential RenameNodeInDestinationRequest retries are idempotent
4. Source shard performs the op on its side, releases the locks and tells the destination shard that it can delete the saved response

But there're a lot of details which make the actual logic more complex, e.g. if `NewName` in the destination dir points to a directory then we should atomically check whether it's empty and, if it is, lock it to prevent new node-ref creation. The logic is described in detail in the following 2 sequence diagrams.

## Some terminology
* In this doc we call the directory pointed to by `ParentId` (or `NodeId`) as "source directory" and the directory pointed to by `NewParentId` as "destination directory".
* Source/destination nodes are the nodes pointed to by the `ParentId/Name` and `NewParentId/NewName` node-refs respectively.
* Source/destination shards are the shards in charge of the source/destination directories.

## Crash recovery
* We save `RenameNodeInDestinationRequest` to tablet `OpLog` to make sure to complete the whole transaction upon source shard crash and reboot.
* We save `RenameNodeInDestinationResponse` to tablet `ResponseLog` to make sure that the destination shard doesn't repeat an already completed `RenameNodeInDestination` operation (that's what was called "idempotency" earlier in this doc).
* We save `AbortUnlinkDirectoryNodeInShardRequest` to tablet `OpLog` to make sure that in any case when we could've sent a `PrepareUnlinkDirectoryNodeInShardRequest` to the shard in charge of the destination node, we will abort it in case of crash and reboot. Here we rely on the fact that directory hardlinks cannot exist so the only shard that can be sending Prepare/AbortUnlinkDirectoryNodeInShardRequest for this node is the destination shard which performs this operation under the `NewParentId/NewName` node-ref lock.

## RenameNode: ParentNode managed by one shard, NewParentNode managed by another shard
```mermaid
sequenceDiagram
    participant ServiceActor_RenameNode
    participant IndexTabletActor_SourceDir
    participant IndexTabletActor_DestinationDir
    participant IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor
    participant IndexTabletActor_SourceNode
    participant IndexTabletActor_DestinationNode

    ServiceActor_RenameNode->>IndexTabletActor_SourceDir: RenameNode(ParentId aka NodeId, Name, NewParentId, NewName)

    IndexTabletActor_SourceDir->>IndexTabletActor_SourceDir: lock NodeRef

    IndexTabletActor_SourceDir->>IndexTabletActor_SourceDir: Tx_PrepareRenameNodeInSource_Prepare

    alt can perform RenameNodeInSource
        IndexTabletActor_SourceDir->>IndexTabletActor_SourceDir: Tx_PrepareRenameNodeInSource_Execute (update parent attributes and write RenameNodeInDestinationRequest to OpLog)
        IndexTabletActor_SourceDir->>IndexTabletActor_DestinationDir: RenameNodeInDestinationRequest
        IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir: lock NewNodeRef
        IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir: Tx_RenameNodeInDestination_Prepare (1st run)

        alt NewName exists
            IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor: Register
            IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_SourceNode: GetNodeAttrRequest
            IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_DestinationNode: GetNodeAttrRequest
            IndexTabletActor_SourceNode->>IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor: GetNodeAttrResponse
            IndexTabletActor_DestinationNode->>IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor: GetNodeAttrResponse

            alt DestinationNode is dir
                IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_DestinationDir: WriteOpLogEntryRequest (write AbortUnlinkDirectoryNodeInShardRequest to OpLog)
                IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir: Tx_WriteOpLogEntryRequest
                IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor: WriteOpLogEntryResponse
                IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_DestinationNode: PrepareUnlinkDirectoryNodeInShardRequest
                IndexTabletActor_DestinationNode->>IndexTabletActor_DestinationNode: Tx_PrepareUnlinkDirectoryNode
                IndexTabletActor_DestinationNode->>IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor: PrepareUnlinkDirectoryNodeInShardResponse
                IndexTabletActor_DestinationDir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_DestinationDir: DoRenameNodeInDestination
            end

            IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir: Tx_RenameNodeInDestination_Prepare (2nd run)

            alt can perform RenameNodeInDestination
                IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir: Tx_RenameNodeInDestination_Execute (perform the op, delete AbortUnlinkDirectoryNodeInShardRequest from OpLog and save the response to ResponseLog)
            else cannot perform RenameNodeInDestination
                IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationNode: AbortUnlinkDirectoryNodeInShardRequest
                IndexTabletActor_DestinationNode->>IndexTabletActor_DestinationNode: Tx_AbortUnlinkDirectoryNode
                IndexTabletActor_DestinationNode->>IndexTabletActor_DestinationDir: AbortUnlinkDirectoryNodeInShardResponse
                IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir: Tx_DeleteOpLogEntry (AbortUnlinkDirectoryNodeInShardRequest)
            end
        else NewName doesn't exist
            IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir: Tx_RenameNodeInDestination_Execute (perform the op and save the response to ResponseLog)
        end

        IndexTabletActor_DestinationDir->>IndexTabletActor_DestinationDir: unlock NewNodeRef
        IndexTabletActor_DestinationDir->>IndexTabletActor_SourceDir: RenameNodeInDestinationResponse
    end

    IndexTabletActor_SourceDir->>IndexTabletActor_SourceDir: Tx_CommitRenameNodeInSource_Prepare
    alt RenameNodeInDestination completed successfully
        IndexTabletActor_SourceDir->>IndexTabletActor_SourceDir: Tx_CommitRenameNodeInSource_Execute (remove source NodeRef)
    end
    IndexTabletActor_SourceDir->>IndexTabletActor_SourceDir: Tx_CommitRenameNodeInSource_Execute (unlock source NodeRef, delete RenameNodeInDestinationRequest from OpLog)
    IndexTabletActor_SourceDir->>ServiceActor_RenameNode: RenameNodeResponse
    IndexTabletActor_SourceDir->>IndexTabletActor_DestinationDir: DeleteResponseLogEntryRequest (best effort)
```

## RenameNode: ParentNode and NewParentNode managed by the same shard
```mermaid
sequenceDiagram
    participant ServiceActor_RenameNode
    participant IndexTabletActor_Dir
    participant IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor
    participant IndexTabletActor_SourceNode
    participant IndexTabletActor_DestinationNode
    participant IndexTabletActor_Dir_AbortUnlinkNodeInDestinationActor

    ServiceActor_RenameNode->>IndexTabletActor_Dir: RenameNode(ParentId aka NodeId, Name, NewParentId, NewName)

    IndexTabletActor_Dir->>IndexTabletActor_Dir: lock NodeRef
    IndexTabletActor_Dir->>IndexTabletActor_Dir: lock NewNodeRef
    IndexTabletActor_Dir->>IndexTabletActor_Dir: Tx_PrepareRenameNode

    alt NewName exists
        IndexTabletActor_Dir->>IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor: Register
        IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_SourceNode: GetNodeAttrRequest
        IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_DestinationNode: GetNodeAttrRequest
        IndexTabletActor_SourceNode->>IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor: GetNodeAttrResponse
        IndexTabletActor_DestinationNode->>IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor: GetNodeAttrResponse

        alt DestinationNode is dir
            IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_Dir: WriteOpLogEntryRequest (write AbortUnlinkDirectoryNodeInShardRequest to OpLog)
            IndexTabletActor_Dir->>IndexTabletActor_Dir: Tx_WriteOpLogEntryRequest
            IndexTabletActor_Dir->>IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor: WriteOpLogEntryResponse
            IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_DestinationNode: PrepareUnlinkDirectoryNodeInShardRequest
            IndexTabletActor_DestinationNode->>IndexTabletActor_DestinationNode: Tx_PrepareUnlinkDirectoryNode
            IndexTabletActor_DestinationNode->>IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor: PrepareUnlinkDirectoryNodeInShardResponse
            IndexTabletActor_Dir_GetNodeInfoAndPrepareUnlinkActor->>IndexTabletActor_Dir: DoRenameNode
        end

        IndexTabletActor_Dir->>IndexTabletActor_Dir: Tx_RenameNode_Prepare (2nd run)

        alt can perform RenameNode
            IndexTabletActor_Dir->>IndexTabletActor_Dir: Tx_RenameNode_Execute (perform the operation and delete AbortUnlinkDirectoryNodeInShardRequest from OpLog)
        else cannot perform RenameNode and DestinationNode is dir
            IndexTabletActor_Dir->>IndexTabletActor_Dir_AbortUnlinkNodeInDestinationActor: Register
            IndexTabletActor_Dir_AbortUnlinkNodeInDestinationActor->>IndexTabletActor_DestinationNode: AbortUnlinkDirectoryNodeInShardRequest
            IndexTabletActor_DestinationNode->>IndexTabletActor_DestinationNode: Tx_AbortUnlinkDirectoryNode
            IndexTabletActor_DestinationNode->>IndexTabletActor_Dir_AbortUnlinkNodeInDestinationActor: AbortUnlinkDirectoryNodeInShardResponse
            IndexTabletActor_Dir_AbortUnlinkNodeInDestinationActor->>IndexTabletActor_Dir: UnlinkDirectoryNodeAbortedInShard
            IndexTabletActor_Dir->>IndexTabletActor_Dir: Tx_DeleteOpLogEntry (AbortUnlinkDirectoryNodeInShardRequest)
        end
    end

    IndexTabletActor_Dir->>IndexTabletActor_Dir: unlock NewNodeRef

    IndexTabletActor_Dir->>IndexTabletActor_Dir: unlock NodeRef
    IndexTabletActor_Dir->>ServiceActor_RenameNode: RenameNodeResponse
```
