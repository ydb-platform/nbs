#pragma once

#include "alloc.h"

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

namespace {

template<typename T>
concept HasReserve = requires(T t) {
    { t.reserve(42) } -> std::same_as<void>;
};

}   // namespace

// A simple wrapper around THashMap that also evicts the least recently used
// elements when the capacity is reached. It keeps track of the order in which
// keys are accessed
template<typename TKey, typename TValue, typename THashFunc = THash<TKey>, 
    typename TBase = THashMap<TKey, TValue, THashFunc, TEqualTo<TKey>, TStlAllocator>>
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

    size_t Capacity = 0;

    inline void RemoveFromOrder(const TKey& key)
    {
        auto it = OrderPositions.find(key);
        if (it != OrderPositions.end()) {
            OrderList.erase(it->second);
            OrderPositions.erase(it);
        }
    }

    void CleanupIfNeeded()
    {
        while (Base.size() > Capacity) {
            auto& key = OrderList.back();
            Base.erase(key);
            OrderPositions.erase(key);
            OrderList.pop_back();
        }
    }

public:
    using iterator = typename TBase::iterator;

    explicit TLRUCache(IAllocator* alloc)
        : Base(alloc)
        , OrderList(alloc)
        , OrderPositions(alloc)
        , Alloc(alloc)
    {}

    void SetCapacity(size_t capacity)
    {
        Capacity = capacity;
        CleanupIfNeeded();
        if constexpr (HasReserve<TBase>) {
            Base.reserve(Capacity);
        }
        OrderPositions.reserve(Capacity);
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

    template<typename... Args>
    std::pair<iterator, bool> emplace(Args&&... args)
    {
        if (Capacity == 0) {
            return {Base.end(), false};
        }
        auto result = Base.emplace(std::forward<Args>(args)...);
        TouchKey(result.first->first);
        CleanupIfNeeded();
        return result;
    }

    TValue& at(const TKey& key) {
        auto& result = Base.at(key);
        TouchKey(key);
        return result;
    }

    [[nodiscard]] size_t size() const
    {
        return Base.size();
    }

    [[nodiscard]] size_t capacity() const
    {
        return Capacity;
    }
};

}   // namespace NCloud
