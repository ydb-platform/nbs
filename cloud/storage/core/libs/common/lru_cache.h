#pragma once

#include "alloc.h"

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T>
concept HasReserve = requires(T t) {
    { t.reserve(42) } -> std::same_as<void>;
};

}   // namespace

// A simple wrapper around THashMap that also evicts the least recently used
// elements when the capacity is reached. It keeps track of the order in which
// keys are accessed
template <
    typename TKey,
    typename TValue,
    typename THashFunc = THash<TKey>,
    typename TBase =
        THashMap<TKey, TValue, THashFunc, TEqualTo<TKey>, TStlAllocator>>
class TLRUCache: public TMapOps<TLRUCache<TKey, TValue, THashFunc, TBase>>
{
    using TOrderList = TList<TKey, TStlAllocator>;
    using TOrderPositions = THashMap<
        TKey,
        typename TOrderList::iterator,
        THashFunc,
        TEqualTo<TKey>,
        TStlAllocator>;

    // Contains the actual mapping key -> value
    TBase Base;
    // Contains the keys in order of access, from most to least recently
    // accessed (the last element is the oldest one to be accessed)
    TOrderList OrderList;
    // Contains the position of each key in OrderList, needed to quickly find
    // and update the order when accessing a key
    TOrderPositions OrderPositions;

    IAllocator* Alloc;

    size_t MaxSize = 0;

    inline void RemoveFromOrder(const TKey& key)
    {
        auto it = OrderPositions.find(key);
        if (it != OrderPositions.end()) {
            OrderList.erase(it->second);
            OrderPositions.erase(it);
        }
    }

    /**
     * @brief Removes least recently used keys from the cache if its size
     * exceeds the maximum allowed.
     *
     * @return TVector<TKey> A vector containing the keys that were evicted from
     * the cache.
     */
    TVector<TKey> CleanupIfNeeded()
    {
        TVector<TKey> evictedKeys;
        if (Base.size() > MaxSize) {
            evictedKeys.reserve(Base.size() - MaxSize);
        }
        while (Base.size() > MaxSize) {
            auto& key = OrderList.back();
            Base.erase(key);
            OrderPositions.erase(key);
            evictedKeys.emplace_back(key);
            OrderList.pop_back();
        }
        return evictedKeys;
    }

public:
    using iterator = typename TBase::iterator;

    explicit TLRUCache(IAllocator* alloc)
        : Base(alloc)
        , OrderList(alloc)
        , OrderPositions(alloc)
        , Alloc(alloc)
    {}

    /**
     * @brief Sets the maximum size of the cache and performs cleanup if
     * necessary.
     *
     * @param maxSize The new maximum number of items allowed in the cache.
     * @return TVector<TKey> A vector containing the excess keys that were
     * evicted
     */
    TVector<TKey> SetMaxSize(size_t maxSize)
    {
        MaxSize = maxSize;
        return CleanupIfNeeded();
    }

    void Reserve(size_t hint)
    {
        if constexpr (HasReserve<TBase>) {
            Base.reserve(hint);
        }
        OrderPositions.reserve(hint);
    }

    // Movesd the key to the front of the order list; used upon any access
    void TouchKey(const TKey& key)
    {
        auto it = OrderPositions.find(key);
        if (it != OrderPositions.end()) {
            OrderList.splice(OrderList.begin(), OrderList, it->second);
        } else {
            OrderPositions.emplace(
                key,
                OrderList.insert(OrderList.begin(), key));
        }
    }

    iterator begin()
    {
        return Base.begin();
    }

    iterator end()
    {
        return Base.end();
    }

    iterator find(const TKey& key)
    {
        auto result = Base.find(key);
        if (result != Base.end()) {
            TouchKey(key);
        }
        return result;
    }

    void erase(iterator it)
    {
        RemoveFromOrder(it->first);
        Base.erase(it);
    }

    void erase(const TKey& key)
    {
        auto it = Base.find(key);
        if (it != Base.end()) {
            RemoveFromOrder(it->first);
            Base.erase(it);
        }
    }

    iterator lower_bound(const TKey& key)
    {
        return Base.lower_bound(key);
    }

    template <typename... Args>
    std::tuple<iterator, bool, std::optional<TKey>> emplace(Args&&... args)
    {
        if (MaxSize == 0) {
            return {Base.end(), false, std::nullopt};
        }
        auto result = Base.emplace(std::forward<Args>(args)...);
        TouchKey(result.first->first);
        auto excessKey = CleanupIfNeeded();
        // There should always be at most one excess key
        Y_DEBUG_ABORT_UNLESS(excessKey.size() <= 1);
        if (!excessKey.empty()) {
            return {result.first, result.second, excessKey[0]};
        }
        return {result.first, result.second, std::nullopt};
    }

    TValue& at(const TKey& key)
    {
        auto& result = Base.at(key);
        TouchKey(key);
        return result;
    }

    [[nodiscard]] size_t size() const
    {
        return Base.size();
    }

    [[nodiscard]] size_t GetMaxSize() const
    {
        return MaxSize;
    }
};

}   // namespace NCloud
