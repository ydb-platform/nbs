#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeIndexCacheStats
{
    ui64 NodeCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeIndexCache
{
private:
    struct TNodeIndexCacheKey
    {
        ui64 ParentNodeId;
        TString Name;

        TNodeIndexCacheKey(ui64 parentNodeId, const TString& name)
            : ParentNodeId(parentNodeId)
            , Name(name)
        {}

        size_t GetHash() const
        {
            return MultiHash(ParentNodeId, Name);
        }

        TNodeIndexCacheKey(const TNodeIndexCacheKey&) = default;
        TNodeIndexCacheKey& operator=(const TNodeIndexCacheKey&) = default;
        TNodeIndexCacheKey(TNodeIndexCacheKey&&) = default;
        TNodeIndexCacheKey& operator=(TNodeIndexCacheKey&&) = default;

        bool operator==(const TNodeIndexCacheKey& rhs) const = default;
    };

    struct TNodeIndexCacheKeyHash
    {
        size_t operator()(const TNodeIndexCacheKey& key) const noexcept
        {
            return key.GetHash();
        }
    };

    THashMap<ui64, TNodeIndexCacheKey> KeyByNodeId;
    THashMap<TNodeIndexCacheKey, NProto::TNodeAttr, TNodeIndexCacheKeyHash>
        AttrByParentNodeId;
    // Nodes that are not possible to update due to an ongoing RW access
    THashMultiSet<ui64> LockedNodes;

    ui32 MaxNodes = 0;

public:
    explicit TNodeIndexCache(IAllocator* allocator);

    [[nodiscard]] TNodeIndexCacheStats GetStats() const;

    void Reset(ui32 maxNodes);

    void InvalidateCache(ui64 parentNodeId, const TString& name);

    void InvalidateCache(ui64 nodeId);

    void LockNode(ui64 nodeId);

    void UnlockNode(ui64 nodeId);

    void RegisterGetNodeAttrResult(
        ui64 parentNodeId,
        const TString& name,
        const NProto::TNodeAttr& response);

    bool TryFillGetNodeAttrResult(
        ui64 parentNodeId,
        const TString& name,
        NProto::TNodeAttr* response);
};

}   // namespace NCloud::NFileStore::NStorage
