#pragma once

#include "tablet_state_iface.h"

#include <cloud/filestore/libs/storage/tablet/tablet_schema.h>

#include <library/cpp/cache/cache.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief Stores the state of the index tables in memory. Can be used to perform
 * read-only operations.
 */
class TInMemoryIndexState : public IIndexTabletDatabase
{
public:
    explicit TInMemoryIndexState(IAllocator* allocator);

    void Reset(
        ui64 nodesCapacity,
        ui64 nodesVerCapacity,
        ui64 nodeAttrsCapacity,
        ui64 nodeAttrsVerCapacity,
        ui64 nodeRefsCapacity,
        ui64 nodeRefsVerCapacity);

    //
    // Nodes
    //

    bool ReadNode(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<IIndexTabletDatabase::TNode>& node) override;

    //
    // Nodes_Ver
    //

    bool ReadNodeVer(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<IIndexTabletDatabase::TNode>& node) override;

    //
    // NodeAttrs
    //

    bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeAttr>& attr) override;

    bool ReadNodeAttrs(
        ui64 nodeId,
        ui64 commitId,
        TVector<IIndexTabletDatabase::TNodeAttr>& attrs) override;

    //
    // NodeAttrs_Ver
    //

    bool ReadNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeAttr>& attr) override;

    bool ReadNodeAttrVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<IIndexTabletDatabase::TNodeAttr>& attrs) override;

    //
    // NodeRefs
    //

    bool ReadNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeRef>& ref) override;

    bool ReadNodeRefs(
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<IIndexTabletDatabase::TNodeRef>& refs,
        ui32 maxBytes,
        TString* next) override;

    bool PrechargeNodeRefs(
        ui64 nodeId,
        const TString& cookie,
        ui32 bytesToPrecharge) override;

    //
    // NodeRefs_Ver
    //

    bool ReadNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeRef>& ref) override;

    bool ReadNodeRefVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<IIndexTabletDatabase::TNodeRef>& refs) override;

    //
    // CheckpointNodes
    //

    bool ReadCheckpointNodes(
        ui64 checkpointId,
        TVector<ui64>& nodes,
        size_t maxCount) override;

private:
    // TODO(#1146): use LRU cache / something with better eviction policy
    ui64 NodesCapacity = 0;
    ui64 NodesVerCapacity = 0;
    ui64 NodeAttrsCapacity = 0;
    ui64 NodeAttrsVerCapacity = 0;
    ui64 NodeRefsCapacity = 0;
    ui64 NodeRefsVerCapacity = 0;

    //
    // Nodes
    //

    struct TNodeRow
    {
        ui64 CommitId = 0;
        NProto::TNode Node;
    };

    THashMap<ui64, TNodeRow> Nodes;

    //
    // Nodes_Ver
    //

    struct TNodesVerKey
    {
        TNodesVerKey(ui64 nodeId, ui64 minCommitId)
            : NodeId(nodeId)
            , MinCommitId(minCommitId)
        {}

        ui64 NodeId = 0;
        ui64 MinCommitId = 0;

        bool operator<(const TNodesVerKey& rhs) const
        {
            return std::tie(NodeId, MinCommitId) <
                   std::tie(rhs.NodeId, rhs.MinCommitId);
        }
    };

    struct TNodesVerRow
    {
        ui64 MaxCommitId = 0;
        NProto::TNode Node;
    };

    TMap<TNodesVerKey, TNodesVerRow> NodesVer;

    //
    // NodeAttrs
    //

    struct TNodeAttrsKey
    {
        TNodeAttrsKey(ui64 nodeId, const TString& name)
            : NodeId(nodeId)
            , Name(name)
        {}

        ui64 NodeId = 0;
        TString Name;

        bool operator==(const TNodeAttrsKey& rhs) const
        {
            return std::tie(NodeId, Name) == std::tie(rhs.NodeId, rhs.Name);
        }
    };

    struct TNodeAttrsKeyHash
    {
        size_t operator()(const TNodeAttrsKey& key) const
        {
            return MultiHash(key.NodeId, key.Name);
        }
    };

    struct TNodeAttrsRow
    {
        ui64 CommitId = 0;
        TString Value;
        ui64 Version = 0;
    };

    THashMap<TNodeAttrsKey, TNodeAttrsRow, TNodeAttrsKeyHash> NodeAttrs;

    //
    // NodeAttrs_Ver
    //

    struct TNodeAttrsVerKey
    {
        TNodeAttrsVerKey(ui64 nodeId, const TString& name, ui64 minCommitId)
            : NodeId(nodeId)
            , Name(name)
            , MinCommitId(minCommitId)
        {}

        ui64 NodeId = 0;
        TString Name;
        ui64 MinCommitId = 0;

        bool operator<(const TNodeAttrsVerKey& rhs) const
        {
            return std::tie(NodeId, Name, MinCommitId) <
                   std::tie(rhs.NodeId, rhs.Name, rhs.MinCommitId);
        }
    };

    struct TNodeAttrsVerRow
    {
        ui64 MaxCommitId = 0;
        TString Value;
        ui64 Version = 0;
    };

    TMap<TNodeAttrsVerKey, TNodeAttrsVerRow> NodeAttrsVer;

    //
    // NodeRefs
    //

    struct TNodeRefsKey
    {
        TNodeRefsKey(ui64 nodeId, const TString& name)
            : NodeId(nodeId)
            , Name(name)
        {}

        ui64 NodeId = 0;
        TString Name;

        bool operator==(const TNodeRefsKey& rhs) const
        {
            return std::tie(NodeId, Name) == std::tie(rhs.NodeId, rhs.Name);
        }
    };

    struct TNodeRefsKeyHash
    {
        size_t operator()(const TNodeRefsKey& key) const
        {
            return MultiHash(key.NodeId, key.Name);
        }
    };

    struct TNodeRefsRow
    {
        ui64 CommitId = 0;
        ui64 ChildId = 0;
        TString FollowerId;
        TString FollowerName;
    };

    THashMap<TNodeRefsKey, TNodeRefsRow, TNodeRefsKeyHash> NodeRefs;

    //
    // NodeRefs_Ver
    //

    struct TNodeRefsVerKey
    {
        TNodeRefsVerKey(ui64 nodeId, const TString& name, ui64 minCommitId)
            : NodeId(nodeId)
            , Name(name)
            , MinCommitId(minCommitId)
        {}

        ui64 NodeId = 0;
        TString Name;
        ui64 MinCommitId = 0;

        bool operator<(const TNodeRefsVerKey& rhs) const
        {
            return std::tie(NodeId, Name, MinCommitId) <
                   std::tie(rhs.NodeId, Name, rhs.MinCommitId);
        }
    };

    struct TNodeRefsVerRow
    {
        ui64 MaxCommitId = 0;
        ui64 ChildId = 0;
        TString FollowerId;
        TString FollowerName;
    };

    TMap<TNodeRefsVerKey, TNodeRefsVerRow> NodeRefsVer;

public:
    struct TWriteNodeRequest
    {
        ui64 NodeId = 0;
        TNodeRow Row;
    };

    struct TDeleteNodeRequest
    {
        ui64 NodeId = 0;
    };

    struct TWriteNodeVerRequest
    {
        TNodesVerKey NodesVerKey;
        TNodesVerRow NodesVerRow;
    };

    struct TDeleteNodeVerRequest
    {
        TNodesVerKey NodesVerKey;
    };

    struct TWriteNodeAttrsRequest
    {
        TNodeAttrsKey NodeAttrsKey;
        TNodeAttrsRow NodeAttrsRow;
    };

    struct TDeleteNodeAttrsRequest
    {
        TNodeAttrsKey NodeAttrsKey;
    };

    struct TWriteNodeAttrsVerRequest
    {
        TNodeAttrsVerKey NodeAttrsVerKey;
        TNodeAttrsVerRow NodeAttrsVerRow;
    };

    struct TDeleteNodeAttrsVerRequest
    {
        TNodeAttrsVerKey NodeAttrsVerKey;
    };

    struct TWriteNodeRefsRequest
    {
        TNodeRefsKey NodeRefsKey;
        TNodeRefsRow NodeRefsRow;
    };

    struct TDeleteNodeRefsRequest
    {
        TNodeRefsKey NodeRefsKey;
    };

    struct TWriteNodeRefsVerRequest
    {
        TNodeRefsVerKey NodeRefsVerKey;
        TNodeRefsVerRow NodeRefsVerRow;
    };

    struct TDeleteNodeRefsVerRequest
    {
        TNodeRefsVerKey NodeRefsVerKey;
    };
    using TIndexStateRequest = std::variant<
        TWriteNodeRequest,
        TDeleteNodeRequest,
        TWriteNodeVerRequest,
        TDeleteNodeVerRequest,
        TWriteNodeAttrsRequest,
        TDeleteNodeAttrsRequest,
        TWriteNodeAttrsVerRequest,
        TDeleteNodeAttrsVerRequest,
        TWriteNodeRefsRequest,
        TDeleteNodeRefsRequest,
        TWriteNodeRefsVerRequest,
        TDeleteNodeRefsVerRequest>;
};

}   // namespace NCloud::NFileStore::NStorage
