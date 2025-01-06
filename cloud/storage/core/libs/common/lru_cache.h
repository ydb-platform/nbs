#pragma once

#include "alloc.h"

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// A simple wrapper around THashMap that also evicts the least recently used
// elements when the capacity is reached. It keeps track of the order in which
// keys are accessed
template <typename TKey, typename TValue>
class TLRUCache: public TMapOps<TLRUCache<TKey, TValue>>
{
    using TBase =
        THashMap<TKey, TValue, THash<TKey>, TEqualTo<TKey>, TStlAllocator>;
    using TOrderList = TList<TKey, TStlAllocator>;
    using TOrderPositions = THashMap<
        TKey,
        typename TOrderList::iterator,
        THash<TKey>,
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

private:
    // Bumps the key to the front of the order list, used upon any access
    void UpdateOrder(const TKey& key)
    {
        auto it = OrderPositions.find(key);
        if (it != OrderPositions.end()) {
            OrderList.splice(OrderList.begin(), OrderList, it->second);
            it->second = OrderList.insert(OrderList.begin(), key);
        } else {
            OrderPositions.emplace(
                key,
                OrderList.insert(OrderList.begin(), key));
        }
    }

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
        Base.reserve(Capacity);
        OrderPositions.reserve(Capacity);
    }

    iterator end()
    {
        return Base.end();
    }

    iterator find(const TKey& key)
    {
        auto result = Base.find(key);
        if (result != Base.end()) {
            UpdateOrder(key);
        }
        return result;
    }

    void erase(iterator it)
    {
        RemoveFromOrder(it->first);
        Base.erase(it);
    }

    template <typename... Args>
    std::pair<iterator, bool> emplace(Args&&... args)
    {
        if (Capacity == 0) {
            return {Base.end(), false};
        }
        auto result = Base.emplace(std::forward<Args>(args)...);
        UpdateOrder(result.first->first);
        CleanupIfNeeded();
        return result;
    }

    template <class T>
    TValue& at(const T& key) {
        auto& result = Base.at(key);
        UpdateOrder(key);
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
