#pragma once
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_FILESYSTEM_STATS(xxx, ...)                                   \
    xxx(LastNodeId,             __VA_ARGS__)                                   \
    xxx(LastLockId,             __VA_ARGS__)                                   \
    xxx(LastCollectCommitId,    __VA_ARGS__)                                   \
    xxx(LastXAttr,              __VA_ARGS__)                                   \
                                                                               \
    xxx(UsedNodesCount,         __VA_ARGS__)                                   \
    xxx(UsedSessionsCount,      __VA_ARGS__)                                   \
    xxx(UsedHandlesCount,       __VA_ARGS__)                                   \
    xxx(UsedLocksCount,         __VA_ARGS__)                                   \
    xxx(UsedBlocksCount,        __VA_ARGS__)                                   \
                                                                               \
    xxx(FreshBlocksCount,       __VA_ARGS__)                                   \
    xxx(MixedBlocksCount,       __VA_ARGS__)                                   \
    xxx(MixedBlobsCount,        __VA_ARGS__)                                   \
    xxx(DeletionMarkersCount,   __VA_ARGS__)                                   \
    xxx(GarbageQueueSize,       __VA_ARGS__)                                   \
    xxx(GarbageBlocksCount,     __VA_ARGS__)                                   \
    xxx(CheckpointNodesCount,   __VA_ARGS__)                                   \
    xxx(CheckpointBlocksCount,  __VA_ARGS__)                                   \
    xxx(CheckpointBlobsCount,   __VA_ARGS__)                                   \
    xxx(FreshBytesCount,        __VA_ARGS__)                                   \
    xxx(AttrsUsedBytesCount,    __VA_ARGS__)                                   \
    xxx(DeletedFreshBytesCount, __VA_ARGS__)                                   \
// FILESTORE_FILESYSTEM_STATS

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief This interface contains a subset of the methods that can be performed over
 * the localDB tables. Those are all the operations, that are performed with
 * the following tables (a.k.a inode index):
 *  - Nodes
 *  - Nodes_Ver
 *  - NodeAttrs
 *  - NodeAttrs_Ver
 *  - NodeRefs
 *  - NodeRefs_Ver
 *  - CheckpointNodes
 */
class IIndexState
{
public:
    struct TNode
    {
        ui64 NodeId;
        NProto::TNode Attrs;
        ui64 MinCommitId;
        ui64 MaxCommitId;
    };

    struct TNodeRef
    {
        ui64 NodeId;
        TString Name;
        ui64 ChildNodeId;
        TString FollowerId;
        TString FollowerName;
        ui64 MinCommitId;
        ui64 MaxCommitId;
    };

    struct TNodeAttr
    {
        ui64 NodeId;
        TString Name;
        TString Value;
        ui64 MinCommitId;
        ui64 MaxCommitId;
        ui64 Version;
    };

    virtual ~IIndexState() = default;

    //
    // Nodes
    //

    virtual bool ReadNode(ui64 nodeId, ui64 commitId, TMaybe<TNode>& node) = 0;

    //
    // Nodes_Ver
    //

    virtual bool ReadNodeVer(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<TNode>& node) = 0;

    //
    // NodeAttrs
    //

    virtual bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) = 0;

    virtual bool ReadNodeAttrs(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) = 0;

    //
    // NodeAttrs_Ver
    //

    virtual bool ReadNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) = 0;

    virtual bool ReadNodeAttrVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) = 0;

    //
    // NodeRefs
    //

    virtual bool ReadNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) = 0;

    virtual bool ReadNodeRefs(
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<TNodeRef>& refs,
        ui32 maxBytes,
        TString* next) = 0;

    virtual bool PrechargeNodeRefs(
        ui64 nodeId,
        const TString& cookie,
        ui32 bytesToPrecharge) = 0;

    //
    // NodeRefs_Ver
    //

    virtual bool ReadNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) = 0;

    virtual bool ReadNodeRefVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeRef>& refs) = 0;

    //
    // CheckpointNodes
    //

    virtual bool ReadCheckpointNodes(
        ui64 checkpointId,
        TVector<ui64>& nodes,
        size_t maxCount = 100) = 0;
};

///////////////////////////////////////////////////////////////////////////////

/**
 * @brief This interface contains all the methods that are needed for RW
 * transactions that modify inode index-related tables.
 */
class IIndexTabletDatabase : public IIndexState
{
public:
    virtual ~IIndexTabletDatabase() = default;

    //
    // FileSystem stats
    //

#define FILESTORE_DECLARE_STATS(name, ...)                                     \
    virtual void Write##name(ui64 value) = 0;                                  \
// FILESTORE_DECLARE_STATS

FILESTORE_FILESYSTEM_STATS(FILESTORE_DECLARE_STATS);

#undef FILESTORE_DECLARE_STATS

    //
    // Nodes
    //

    virtual void WriteNode(
        ui64 nodeId,
        ui64 commitId,
        const NProto::TNode& attrs) = 0;

    virtual void DeleteNode(ui64 nodeId) = 0;

    //
    // Nodes_Ver
    //

    virtual void WriteNodeVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs) = 0;

    virtual void DeleteNodeVer(ui64 nodeId, ui64 commitId) = 0;

    //
    // NodeAttrs
    //

    virtual void WriteNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        const TString& value,
        ui64 version) = 0;

    virtual void DeleteNodeAttr(ui64 nodeId, const TString& name) = 0;

    //
    // NodeAttrs_Ver
    //

    virtual void WriteNodeAttrVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& name,
        const TString& value,
        ui64 version) = 0;

    virtual void DeleteNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name) = 0;

    //
    // NodeRefs
    //

    virtual void WriteNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        ui64 childNode,
        const TString& followerId,
        const TString& followerName) = 0;

    virtual void DeleteNodeRef(ui64 nodeId, const TString& name) = 0;

    //
    // NodeRefs_Ver
    //

    virtual void WriteNodeRefVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& name,
        ui64 childNode,
        const TString& followerId,
        const TString& followerName) = 0;

    virtual void DeleteNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name) = 0;

    //
    // CheckpointNodes
    //

    virtual void WriteCheckpointNode(ui64 checkpointId, ui64 nodeId) = 0;

    virtual void DeleteCheckpointNode(ui64 checkpointId, ui64 nodeId) = 0;

    // Following methods are needed because UnlinkNode calls Truncate, which
    // in turn modifies tables, which modifies tables that are not related to
    // the inode index.

    //
    // FreshBytes
    //

    virtual void WriteFreshBytes(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        TStringBuf data) = 0;

    virtual void WriteFreshBytesDeletionMarker(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        ui64 len) = 0;

    //
    // FreshBlocks
    //

    virtual void MarkFreshBlockDeleted(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        ui32 blockIndex) = 0;

    //
    // DeletionMarkers
    //

    virtual void WriteDeletionMarkers(
        ui32 rangeId,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) = 0;

    //
    // CompactionMap
    //

    virtual void WriteCompactionMap(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount) = 0;

};

}   // namespace NCloud::NFileStore::NStorage
