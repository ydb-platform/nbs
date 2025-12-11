#pragma once

#include "public.h"

#include <util/generic/vector.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////
// Dummy RCU list implementation

template <typename T>
class TRCUList
{
    using TList = TVector<T>;
    using TListPtr = std::shared_ptr<TList>;

private:
    TListPtr List{new TList()};
    TAdaptiveLock Lock;

public:
    void Add(T value)
    {
        with_lock (Lock) {
            auto list = std::make_shared<TList>(*List);
            list->push_back(std::move(value));

            DoSwap(List, list);
        }
    }

    void Delete(std::function<bool(T)> predicate)
    {
        auto list = std::make_shared<TList>();

        with_lock (Lock) {
            for (const T& item: *List) {
                if (!predicate(item)) {
                    list->push_back(item);
                }
            }

            DoSwap(List, list);
        }
    }

    TListPtr Get()
    {
        with_lock (Lock) {
            return List;
        }
    }
};

}   // namespace NCloud::NBlockStore::NRdma
