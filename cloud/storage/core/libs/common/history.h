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
        if (auto oldest = Ring.PushBack(item)) {
            Items.erase(*oldest);
        }
        Items.emplace(std::move(item));
    }

    bool Contains(const T& item) const
    {
        return Items.find(item) != Items.end();
    }
};

}   // namespace NCloud
