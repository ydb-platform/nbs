#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/ring_buffer.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TTsRingBuffer
{
public:
    struct TEntry
    {
        TInstant Ts;
        T Value;
    };

private:
    NCloud::TRingBuffer<TEntry> Entries;

public:
    explicit TTsRingBuffer(ui32 size)
        : Entries(size, TEntry{})
    {}

    bool Ready() const
    {
        return Capacity() != 0 && Entries.IsFull();
    }

    TInstant LastTs() const
    {
        if (Entries.IsEmpty()) {
            return {};
        }

        return Entries.Back().Ts;
    }

    TEntry Get(ui32 offset) const
    {
        return Entries.Back(offset);
    }

    ui32 Capacity() const
    {
        return Entries.Capacity();
    }

    ui32 Size() const
    {
        return Entries.Size();
    }

    void Register(TEntry entry)
    {
        if (Entries.Capacity() == 0) {
            return;
        }

        Y_VERIFY_DEBUG(Entries.IsEmpty() || LastTs() <= entry.Ts);
        Entries.PushBack(std::move(entry));
    }
};

}   // namespace NCloud::NBlockStore::NStorage
