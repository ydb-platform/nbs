#pragma once

#include "tablet_state_iface.h"

#include <cloud/filestore/libs/storage/tablet/model/metadata_cache.h>
#include <cloud/filestore/libs/storage/tablet/tablet_schema.h>

#include <cloud/storage/core/libs/common/lru_cache.h>

#include <library/cpp/cache/cache.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>

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
    ui64 NodeRefsExhaustivenessCapacity;
    ui64 NodeRefsExhaustivenessCount;
    bool IsNodeRefsExhaustive;
};

////////////////////////////////////////////////////////////////////////////////

class IInMemoryIndexState : public IIndexTabletDatabase
{
public:
    virtual void Reset(
        ui64 nodesCapacity,
        ui64 nodeAttrsCapacity,
        ui64 nodeRefsCapacity,
        ui64 nodeRefsExhaustivenessCapacity) = 0;

    virtual void LoadNodeRefs(const TVector<TNodeRef>& nodeRefs) = 0;

    virtual void MarkNodeRefsLoadComplete() = 0;

    virtual void MarkNodeRefsExhaustive(ui64 nodeId) = 0;

    [[nodiscard]] virtual TInMemoryIndexStateStats GetStats() const = 0;

    virtual void UpdateLogTag(TString logTag) = 0;

    //
    // Cache bypass
    //

    virtual void ActivateInMemoryIndexStateBypass(
        ui64 nodeId,
        ui64 commitId) = 0;

    virtual void DeactivateInMemoryIndexStateBypass(
        ui64 nodeId,
        ui64 commitId) = 0;

    virtual void SetUnconfirmedRecoveryReady(bool unconfirmedRecoveryReady) = 0;

    //
    // Nodes
    //

    struct TNodeRow
    {
        ui64 CommitId = 0;
        NProto::TNode Node;
    };

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

    // This request can be interpreted as follow: "last RefsSize added refs were
    // children of the NodeId and present the entirety of its children", thus if
    // we see such request, we can mark the NodeRefs cache as exhaustive for
    // this particular NodeId
    struct TMarkNodeRefsAsCachedRequest
    {
        ui64 NodeId;
        ui64 RefsSize;
    };

    using TIndexStateRequest = std::variant<
        TWriteNodeRequest,
        TDeleteNodeRequest,
        TWriteNodeAttrsRequest,
        TDeleteNodeAttrsRequest,
        TWriteNodeRefsRequest,
        TDeleteNodeRefsRequest,
        TMarkNodeRefsAsCachedRequest>;

    virtual void UpdateState(
        const TVector<TIndexStateRequest>& nodeUpdates) = 0;
};

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief Stores the state of the index tables in memory. Can be used to perform
 * read-only operations.
 */
template <typename TNodeRefsImpl>
class TInMemoryIndexState : public IInMemoryIndexState
{
public:
    explicit TInMemoryIndexState(IAllocator* allocator);

    void Reset(
        ui64 nodesCapacity,
        ui64 nodeAttrsCapacity,
        ui64 nodeRefsCapacity,
        ui64 nodeRefsExhaustivenessCapacity) override;

    void LoadNodeRefs(const TVector<TNodeRef>& nodeRefs) override;

    void MarkNodeRefsLoadComplete() override;

    void MarkNodeRefsExhaustive(ui64 nodeId) override;

    [[nodiscard]] TInMemoryIndexStateStats GetStats() const override;

    void UpdateLogTag(TString logTag) override;

private:
    TString LogTag;

    //
    // Cache bypass
    //

public:
    void ActivateInMemoryIndexStateBypass(ui64 nodeId, ui64 commitId) override;

    void DeactivateInMemoryIndexStateBypass(
        ui64 nodeId,
        ui64 commitId) override;

    void SetUnconfirmedRecoveryReady(bool unconfirmedRecoveryReady) override;

private:
    bool ShouldBypassCacheRead(ui64 nodeId, ui64 commitId) const;

    bool UnconfirmedRecoveryReady = false;
    THashMap<ui64, TDeque<ui64>> CacheBypassCommitIdsByNodeId;

    //
    // Nodes
    //

public:
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
        TString* next,
        ui32* skippedRefs,
        bool noAutoPrecharge,
        NProto::EListNodesSizeMode sizeMode = NProto::LNSM_NAME_ONLY) override;

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

    ::TLRUCache<ui64, TNodeRow> Nodes;

    //
    // NodeAttrs
    //

private:
    ::TLRUCache<TNodeAttrsKey, TNodeAttrsRow> NodeAttrs;

    //
    // NodeRefs
    //

    TNodeRefsImpl NodeRefs;

    struct TNodeRefsExhaustivenessInfo
    {
    private:
        // Indicates whether at least one eviction was observed
        bool IsNodeRefsEvictionObserved = false;
        // Can only be set explicitly upone all node refs load completion and in
        // case zero evictions were observed
        bool IsNodeRefsExhaustive = false;
        // Per-nodeId info about several selected nodes
        ::TLRUCache<ui64, bool> IsExhaustivePerNode;

    public:
        TNodeRefsExhaustivenessInfo()
            : IsExhaustivePerNode(0)
        {}

        void SetMaxSize(size_t size)
        {
            IsExhaustivePerNode.SetMaxSize(size);
        }

        [[nodiscard]] bool IsExhaustiveForNode(ui64 nodeId)
        {
            if (Y_UNLIKELY(IsNodeRefsExhaustive)) {
                return true;
            }
            // TInMemoryIndexState is a preemptive cache, thus it is not always
            // possible to determine, whether the set of stored references is
            // complete.
            auto it = IsExhaustivePerNode.Find(nodeId);
            return it != IsExhaustivePerNode.End() && it.Value();
        }

        [[nodiscard]] bool IsExhaustive() const
        {
            return IsNodeRefsExhaustive;
        }

        void NodeRefsEvictionObserved(ui64 nodeId)
        {
            IsNodeRefsEvictionObserved = true;
            IsNodeRefsExhaustive = false;
            auto it = IsExhaustivePerNode.Find(nodeId);
            if (it != IsExhaustivePerNode.End()) {
                IsExhaustivePerNode.Erase(it);
            }
        }

        void MarkNodeRefsExhaustive(ui64 nodeId)
        {
            if (IsExhaustivePerNode.GetMaxSize()) {
                IsExhaustivePerNode.Insert(nodeId, true);
            }
        }

        void MarkNodeRefsLoadComplete()
        {
            // If during the startup there were no evictions, then the cache
            // should be complete upon the load completion.
            IsNodeRefsExhaustive = !IsNodeRefsEvictionObserved;
        }

        [[nodiscard]] ui64 GetSize() const
        {
            return IsExhaustivePerNode.Size();
        }

        [[nodiscard]] ui64 GetMaxSize() const
        {
            return IsExhaustivePerNode.GetMaxSize();
        }

    } NodeRefsExhaustivenessInfo;

public:
    void UpdateState(const TVector<TIndexStateRequest>& nodeUpdates) override;
};

////////////////////////////////////////////////////////////////////////////////

using TStandardInMemoryIndexState = TInMemoryIndexState<TStandardNodeRefsCache>;

}   // namespace NCloud::NFileStore::NStorage
