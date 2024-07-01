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
class IIndexTabletDatabase
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

    virtual ~IIndexTabletDatabase() = default;

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

}   // namespace NCloud::NFileStore::NStorage
