#pragma once

#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief This interface contains a subset of the methods that can be performed
 * over the localDB tables. Those are all the operations, that are performed
 * with the following tables (a.k.a inode index):
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
        TString ShardId;
        TString ShardNodeName;
        ui64 MinCommitId;
        ui64 MaxCommitId;

        // There are two types of node refs: those that point to nodes in the
        // same filesystem as the parent and those that point to nodes in
        // another filesystem. The latter ones have ShardId and ShardNodeName
        // specified instead of ChildNodeId
        bool IsExternal() const
        {
            return !ShardId.empty();
        }
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

    virtual bool ReadNodes(
        ui64 startNodeId,
        ui64 maxNodes,
        ui64& nextNodeId,
        TVector<TNode>& nodes) = 0;

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

    /**
     * @brief read at most maxCount node refs starting from key
     * (startNodeId, startCookie). Populates refs with the nodeRefs that have
     * been read. If there are more nodeRefs to read, nextNodeId and nextCookie
     * will be populated with the key to continue reading from
     */
    virtual bool ReadNodeRefs(
        ui64 startNodeId,
        const TString& startCookie,
        ui64 maxCount,
        TVector<IIndexTabletDatabase::TNodeRef>& refs,
        ui64& nextNodeId,
        TString& nextCookie) = 0;

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
        size_t maxCount) = 0;
};

}   // namespace NCloud::NFileStore::NStorage
