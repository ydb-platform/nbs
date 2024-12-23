#pragma once

#include "alloc.h"

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// A simple wrapper around THashMap that also evicts the least recently used
// elements when the capacity is reached. It keeps track of the order in which
// keys are accessed.
//
// Note: not all THashMap methods are implemented, only the ones that are
// actually used upon the usage of this class. Feel free to add more as needed.
template <typename TKey, typename TValue>
class TLRUCache: public TMapOps<TLRUCache<TKey, TValue>>
{
    using TBase =
        THashMap<TKey, TValue, THash<TKey>, TEqualTo<TKey>, TStlAllocator>;
    using TOrderList = TList<TKey, TStlAllocator>;
    using TOrderIterator = THashMap<
        TKey,
        typename TOrderList::iterator,
        THash<TKey>,
        TEqualTo<TKey>,
        TStlAllocator>;

    // Contains the actual mapping key -> value
    TBase Base;
    // Contains the keys in order of access, from most to least recently
    // accessed (the last element is the oldest one to be accessed) most
    // recently accessed
    TOrderList OrderList;
    // Contains the position of each key in OrderList, needed to quickly find
    // and update the order when accessing a key
    TOrderIterator OrderPosition;

    IAllocator* Alloc;

    size_t Capacity = 0;

private:
    // Bumps the key to the front of the order list, used upon any access
    void UpdateOrder(const TKey& key)
    {
        auto it = OrderPosition.find(key);
        if (it != OrderPosition.end()) {
            OrderList.erase(it->second);
        }
        OrderList.emplace_front(key);
        OrderPosition[key] = OrderList.begin();
    }

    void RemoveFromOrder(const TKey& key)
    {
        auto it = OrderPosition.find(key);
        if (it != OrderPosition.end()) {
            OrderList.erase(it->second);
            OrderPosition.erase(it);
        }
    }

    void CleanupIfNeeded()
    {
        while (Base.size() > Capacity) {
            auto& key = OrderList.back();
            Base.erase(key);
            OrderPosition.erase(key);
            OrderList.pop_back();
        }
    }

public:
    using iterator = typename TBase::iterator;

    explicit TLRUCache(IAllocator* alloc)
        : Base(alloc)
        , OrderList(alloc)
        , OrderPosition(alloc)
        , Alloc(alloc)
    {}

    void Reset(size_t capacity)
    {
        Capacity = capacity;
        CleanupIfNeeded();
        Base.reserve(Capacity);
        OrderPosition.reserve(Capacity);
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

};

}   // namespace NCloud
