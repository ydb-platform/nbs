#pragma once

#include <cloud/storage/core/libs/common/lru_cache.h>

#include <util/digest/multi.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <absl/container/btree_map.h>

#include <optional>

/*
 * The whole implementation is located in the header to allow inlining.
 * NodeRef lookup and iteration can be done in hot code paths.
 */

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeRefsKey
{
    TNodeRefsKey(ui64 nodeId, TString name)
        : NodeId(nodeId)
        , Name(std::move(name))
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

struct TNodeRefsRow
{
    ui64 CommitId = 0;
    ui64 ChildId = 0;
    TString ShardId;
    TString ShardNodeName;
};

struct TNodeRefsKeyHash
{
    size_t operator()(const TNodeRefsKey& key) const
    {
        return MultiHash(key.NodeId, key.Name);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TContainer>
class TMetadataCacheIterator
{
private:
    using TImpl = typename TContainer::iterator;
    TImpl Impl;
    TImpl End;

public:
    TMetadataCacheIterator(TImpl impl, TImpl end)
        : Impl(std::move(impl))
        , End(std::move(end))
    {}

public:
    bool Get(const TNodeRefsKey** key, const TNodeRefsRow** value)
    {
        if (Impl == End) {
            *key = nullptr;
            *value = nullptr;
            return false;
        }

        *key = &Impl->first;
        *value = &Impl->second;
        return true;
    }

    auto& operator++()
    {
        if (Impl != End) {
            ++Impl;
        }

        return *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStandardNodeRefsCache
{
private:
    using TNodeRefs = NCloud::TLRUCache<
        TNodeRefsKey,
        TNodeRefsRow,
        true /* UseIndexLookup */,
        TNodeRefsKeyHash,
        TMap<TNodeRefsKey, TNodeRefsRow, TLess<TNodeRefsKey>, TStlAllocator>>;
    TNodeRefs NodeRefs;

public:
    TStandardNodeRefsCache(IAllocator *allocator, size_t maxSize)
        : NodeRefs(allocator)
    {
        NodeRefs.SetMaxSize(maxSize);
    }

public:
    [[nodiscard]] size_t Size() const
    {
        return NodeRefs.size();
    }

    [[nodiscard]] size_t GetMaxSize() const
    {
        return NodeRefs.GetMaxSize();
    }

    TNodeRefsRow* Find(const TNodeRefsKey& key)
    {
        return NodeRefs.FindInIndex(key);
    }

    void TouchKey(const TNodeRefsKey& key)
    {
        NodeRefs.TouchKey(key);
    }

    void Erase(const TNodeRefsKey& key)
    {
        NodeRefs.erase(key);
    }

    std::optional<TNodeRefsKey> Put(
        const TNodeRefsKey& key,
        TNodeRefsRow value)
    {
        return std::get<2>(NodeRefs.emplace(key, std::move(value)));
    }

    TMetadataCacheIterator<TNodeRefs> LowerBound(const TNodeRefsKey& key)
    {
        return {NodeRefs.lower_bound(key), NodeRefs.end()};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnlimitedBTreeNodeRefsCache
{
private:
    using TNodeRefs = absl::btree_map<TNodeRefsKey, TNodeRefsRow>;
    TNodeRefs NodeRefs;

public:
    TUnlimitedBTreeNodeRefsCache(IAllocator *allocator, size_t maxSize)
    {
        Y_UNUSED(allocator);
        Y_UNUSED(maxSize);
    }

public:
    [[nodiscard]] size_t Size() const
    {
        return NodeRefs.size();
    }

    [[nodiscard]] size_t GetMaxSize() const
    {
        return 0;
    }

    TNodeRefsRow* Find(const TNodeRefsKey& key)
    {
        auto it = NodeRefs.find(key);
        return it != NodeRefs.end() ? &it->second : nullptr;
    }

    void TouchKey(const TNodeRefsKey& key)
    {
        Y_UNUSED(key);
    }

    void Erase(const TNodeRefsKey& key)
    {
        NodeRefs.erase(key);
    }

    std::optional<TNodeRefsKey> Put(
        const TNodeRefsKey& key,
        TNodeRefsRow value)
    {
        NodeRefs.emplace(key, std::move(value));
        return {};
    }

    TMetadataCacheIterator<TNodeRefs> LowerBound(const TNodeRefsKey& key)
    {
        return {NodeRefs.lower_bound(key), NodeRefs.end()};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeAttrsKey
{
    TNodeAttrsKey(ui64 nodeId, TString name)
        : NodeId(nodeId)
        , Name(std::move(name))
    {}

    ui64 NodeId = 0;
    TString Name;

    bool operator==(const TNodeAttrsKey& rhs) const
    {
        return std::tie(NodeId, Name) == std::tie(rhs.NodeId, rhs.Name);
    }
};

struct TNodeAttrsRow
{
    ui64 CommitId = 0;
    TString Value;
    ui64 Version = 0;
};

}   // namespace NCloud::NFileStore::NStorage

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NCloud::NFileStore::NStorage::TNodeAttrsKey>
{
    size_t operator()(const NCloud::NFileStore::NStorage::TNodeAttrsKey& key)
        const
    {
        return MultiHash(key.NodeId, key.Name);
    }
};
