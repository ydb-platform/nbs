#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TKey
{
    ui64 ParentNodeId;
    TString Name;

    TKey(ui64 parentNodeId, const TString& name)
        : ParentNodeId(parentNodeId)
        , Name(name)
    {}

    size_t GetHash() const
    {
        return MultiHash(ParentNodeId, Name);
    }

    TKey(const TKey&) = default;
    TKey& operator=(const TKey&) = default;
    TKey(TKey&&) = default;
    TKey& operator=(TKey&&) = default;

    friend bool operator==(const TKey& lhv, const TKey& rhv) = default;
};

}   // namespace

}   // namespace NCloud::NFileStore::NStorage

template <>
struct THash<::NCloud::NFileStore::NStorage::TKey>
{
    size_t operator()(
        const ::NCloud::NFileStore::NStorage::TKey& key) const noexcept
    {
        return key.GetHash();
    }
};

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeIndexCacheStats
{
    ui64 NodeCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeIndexCache
{
    THashMap<ui64, TKey> KeyByNodeId;
    THashMap<TKey, NProto::TNodeAttr> AttrByParentNodeId;

    size_t MaxNodes;

public:
    explicit TNodeIndexCache(IAllocator* allocator);

    [[nodiscard]] TNodeIndexCacheStats GetStats() const;

    void Reset(size_t maxNodes);

    void InvalidateCache(ui64 parentNodeId, const TString& name);

    void InvalidateCache(ui64 nodeId);
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
