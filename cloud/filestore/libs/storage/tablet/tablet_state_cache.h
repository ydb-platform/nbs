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
    public:
        ui64 CommitId;
        NProto::TNode Node;
    };

    THashMap<ui64, TNodeRow> Nodes;

    //
    // Nodes_Ver
    //

    struct TNodesVerKey
    {
    public:
        TNodesVerKey(ui64 nodeId, ui64 minCommitId)
            : NodeId(nodeId)
            , MinCommitId(minCommitId)
        {}

        ui64 NodeId;
        ui64 MinCommitId;

        bool operator<(const TNodesVerKey& rhs) const
        {
            return std::tie(NodeId, MinCommitId) <
                   std::tie(rhs.NodeId, rhs.MinCommitId);
        }
    };

    struct TNodesVerRow
    {
    public:
        ui64 MaxCommitId;
        NProto::TNode Node;
    };

    TMap<TNodesVerKey, TNodesVerRow> NodesVer;

    //
    // NodeAttrs
    //

    struct TNodeAttrsKey
    {
    public:
        TNodeAttrsKey(ui64 nodeId, const TString& name)
            : NodeId(nodeId)
            , Name(name)
        {}

        ui64 NodeId;
        TString Name;

        bool operator==(const TNodeAttrsKey& rhs) const
        {
            return std::tie(NodeId, Name) == std::tie(rhs.NodeId, rhs.Name);
        }
    };

    struct TNodeAttrsKeyHash
    {
    public:
        size_t operator()(const TNodeAttrsKey& key) const
        {
            return MultiHash(key.NodeId, key.Name);
        }
    };   // namespace NCloud::NFileStore::NStorage

    struct TNodeAttrsRow
    {
    public:
        ui64 CommitId;
        TString Value;
        ui64 Version;
    };

    THashMap<TNodeAttrsKey, TNodeAttrsRow, TNodeAttrsKeyHash> NodeAttrs;

    //
    // NodeAttrs_Ver
    //

    struct TNodeAttrsVerKey
    {
    public:
        TNodeAttrsVerKey(ui64 nodeId, const TString& name, ui64 minCommitId)
            : NodeId(nodeId)
            , Name(name)
            , MinCommitId(minCommitId)
        {}

        ui64 NodeId;
        TString Name;
        ui64 MinCommitId;

        bool operator<(const TNodeAttrsVerKey& rhs) const
        {
            return std::tie(NodeId, Name, MinCommitId) <
                   std::tie(rhs.NodeId, rhs.Name, rhs.MinCommitId);
        }
    };

    struct TNodeAttrsVerRow
    {
    public:
        ui64 MaxCommitId;
        TString Value;
        ui64 Version;
    };

    TMap<TNodeAttrsVerKey, TNodeAttrsVerRow> NodeAttrsVer;

    //
    // NodeRefs
    //

    struct TNodeRefsKey
    {
    public:
        TNodeRefsKey(ui64 nodeId, const TString& name)
            : NodeId(nodeId)
            , Name(name)
        {}

        ui64 NodeId;
        TString Name;

        bool operator==(const TNodeRefsKey& rhs) const
        {
            return std::tie(NodeId, Name) == std::tie(rhs.NodeId, rhs.Name);
        }
    };

    struct TNodeRefsKeyHash
    {
    public:
        size_t operator()(const TNodeRefsKey& key) const
        {
            return MultiHash(key.NodeId, key.Name);
        }
    };

    struct TNodeRefsRow
    {
    public:
        ui64 CommitId;
        ui64 ChildId;
        TString FollowerId;
        TString FollowerName;
    };

    THashMap<TNodeRefsKey, TNodeRefsRow, TNodeRefsKeyHash> NodeRefs;

    //
    // NodeRefs_Ver
    //

    struct TNodeRefsVerKey
    {
    public:
        TNodeRefsVerKey(ui64 nodeId, const TString& name, ui64 minCommitId)
            : NodeId(nodeId)
            , Name(name)
            , MinCommitId(minCommitId)
        {}

        ui64 NodeId;
        TString Name;
        ui64 MinCommitId;

        bool operator<(const TNodeRefsVerKey& rhs) const
        {
            return std::tie(NodeId, Name, MinCommitId) <
                   std::tie(rhs.NodeId, Name, rhs.MinCommitId);
        }
    };

    struct TNodeRefsVerRow
    {
    public:
        ui64 MaxCommitId;
        ui64 ChildId;
        TString FollowerId;
        TString FollowerName;
    };

    TMap<TNodeRefsVerKey, TNodeRefsVerRow> NodeRefsVer;
};

}   // namespace NCloud::NFileStore::NStorage
