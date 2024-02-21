#pragma once

#include "ring_buffer.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class THistory
{
private:
    TRingBuffer<T> Ring;
    THashSet<T> Items;

public:
    THistory(size_t capacity)
        : Ring(capacity)
    {}

    void Put(T item)
    {
        if (auto back = Ring.PushBack(item)) {
            Items.erase(*back);
        }
        Items.emplace(std::move(item));
    }

    bool Contains(const T& item) const
    {
        return Items.find(item) != Items.end();
    }
};

}   // namespace NCloud
