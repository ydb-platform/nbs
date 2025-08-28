#pragma once

#include "tablet_state_iface.h"

#include <cloud/filestore/libs/storage/tablet/tablet_schema.h>

#include <library/cpp/cache/cache.h>

#include <cloud/storage/core/libs/common/lru_cache.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TInMemoryIndexStateStats
{
    ui64 NodesCount;
    ui64 NodesCapacity;
    ui64 NodeRefsCount;
    ui64 NodeRefsCapacity;
    ui64 NodeAttrsCount;
    ui64 NodeAttrsCapacity;
    bool IsNodeRefsExhaustive;
};

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
        ui64 nodeAttrsCapacity,
        ui64 nodeRefsCapacity);

    void LoadNodeRefs(const TVector<TNodeRef>& nodeRefs);

    void MarkNodeRefsLoadComplete();

    [[nodiscard]] TInMemoryIndexStateStats GetStats() const;

    //
    // Nodes
    //

    bool ReadNode(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<IIndexTabletDatabase::TNode>& node) override;

    bool ReadNodes(
        ui64 startNodeId,
        ui64 maxNodes,
        ui64& nextNodeId,
        TVector<IIndexTabletDatabase::TNode>& nodes) override;

private:
    void WriteNode(
        ui64 nodeId,
        ui64 commitId,
        const NProto::TNode& attrs);

    void DeleteNode(ui64 nodeId);

    //
    // Nodes_Ver
    //

public:
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

private:
    void WriteNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        const TString& value,
        ui64 version);

    void DeleteNodeAttr(ui64 nodeId, const TString& name);

    //
    // NodeAttrs_Ver
    //

public:
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

    bool ReadNodeRefs(
        ui64 startNodeId,
        const TString& startCookie,
        ui64 maxCount,
        TVector<IIndexTabletDatabase::TNodeRef>& refs,
        ui64& nextNodeId,
        TString& nextCookie) override;

    bool PrechargeNodeRefs(
        ui64 nodeId,
        const TString& cookie,
        ui64 rowsToPrecharge,
        ui64 bytesToPrecharge) override;

private:
    void WriteNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        ui64 childNode,
        const TString& shardId,
        const TString& shardNodeName);

    void DeleteNodeRef(ui64 nodeId, const TString& name);

    //
    // NodeRefs_Ver
    //

public:
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

    //
    // MixedIndex
    //

    bool ReadMixedBlocks(
        ui32 rangeId,
        TVector<IIndexTabletDatabase::TMixedBlob>& blobs,
        IAllocator* alloc) override;

    bool ReadDeletionMarkers(
        ui32 rangeId,
        TVector<TDeletionMarker>& deletionMarkers) override;

private:

    //
    // Nodes
    //

    struct TNodeRow
    {
        ui64 CommitId = 0;
        NProto::TNode Node;
    };

    ::TLRUCache<ui64, TNodeRow> Nodes;

    //
    // NodeAttrs
    //

public:
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

private:
    struct TNodeAttrsRow
    {
        ui64 CommitId = 0;
        TString Value;
        ui64 Version = 0;
    };

    ::TLRUCache<TNodeAttrsKey, TNodeAttrsRow> NodeAttrs;

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

        bool operator<(const TNodeRefsKey& rhs) const
        {
            return std::tie(NodeId, Name) < std::tie(rhs.NodeId, rhs.Name);
        }

        bool operator==(const TNodeRefsKey& rhs) const
        {
            return std::tie(NodeId, Name) == std::tie(rhs.NodeId, rhs.Name);
        }
    };

    struct TNodeRefsKeyHash
    {
        size_t operator()(
            const NCloud::NFileStore::NStorage::TInMemoryIndexState::TNodeRefsKey&
                key) const
        {
            return MultiHash(key.NodeId, key.Name);
        }
    };

    struct TNodeRefsRow
    {
        ui64 CommitId = 0;
        ui64 ChildId = 0;
        TString ShardId;
        TString ShardNodeName;
    };

    NCloud::TLRUCache<
        TNodeRefsKey,
        TNodeRefsRow,
        TNodeRefsKeyHash,
        TMap<TNodeRefsKey, TNodeRefsRow, TLess<TNodeRefsKey>, TStlAllocator>>
        NodeRefs;

    bool IsNodeRefsEvictionObserved = false;
    bool IsNodeRefsExhaustive = false;

    void NodeRefsEvictionObserved()
    {
        IsNodeRefsEvictionObserved = true;
        IsNodeRefsExhaustive = false;
    }

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

    struct TWriteNodeAttrsRequest
    {
        TNodeAttrsKey NodeAttrsKey;
        TNodeAttrsRow NodeAttrsRow;
    };

    using TDeleteNodeAttrsRequest = TNodeAttrsKey;

    struct TWriteNodeRefsRequest
    {
        TNodeRefsKey NodeRefsKey;
        TNodeRefsRow NodeRefsRow;
    };

    using TDeleteNodeRefsRequest = TNodeRefsKey;

    using TIndexStateRequest = std::variant<
        TWriteNodeRequest,
        TDeleteNodeRequest,
        TWriteNodeAttrsRequest,
        TDeleteNodeAttrsRequest,
        TWriteNodeRefsRequest,
        TDeleteNodeRefsRequest>;

    void UpdateState(const TVector<TIndexStateRequest>& nodeUpdates);
};

}   // namespace NCloud::NFileStore::NStorage

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NCloud::NFileStore::NStorage::TInMemoryIndexState::TNodeAttrsKey>
{
    inline size_t operator()(
        const NCloud::NFileStore::NStorage::TInMemoryIndexState::TNodeAttrsKey&
            key) const
    {
        return MultiHash(key.NodeId, key.Name);
    }
};
