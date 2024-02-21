#pragma once

#include "ring_buffer.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class THistory
{
private:
    TRingBuffer<T> Ring;
    THashMap<T, size_t> References;

public:
    THistory(size_t capacity)
        : Ring(capacity)
    {}

    void Put(T item)
    {
        References[item]++;
        if (auto oldest = Ring.PushBack(std::move(item))) {
            auto it = References.find(*oldest);
            if (--it->second == 0) {
                References.erase(it);
            }
        }
    }

    bool Contains(const T& item) const
    {
        return References.find(item) != References.end();
    }
};

}   // namespace NCloud
