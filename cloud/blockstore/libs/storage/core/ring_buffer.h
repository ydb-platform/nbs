#pragma once

#include "public.h"

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TRingBuffer
{
public:
    struct TEntry
    {
        TInstant Ts;
        T Value;
    };

private:
    TVector<TEntry> Entries;
    ui32 Count = 0;

public:
    TRingBuffer(ui32 size)
        : Entries(size)
    {
    }

public:
    bool Ready() const
    {
        return Entries.size() && Count >= Entries.size();
    }

    TInstant LastTs() const
    {
        if (Entries.empty()) {
            return {};
        }

        return Entries[Index(0)].Ts;
    }

    TEntry Get(ui32 i) const
    {
        if (i >= Count) {
            return {};
        }

        return Entries[Index(i)];
    }

    ui32 Size() const
    {
        return Entries.size();
    }

    void Register(TEntry entry)
    {
        if (Entries.empty()) {
            return;
        }

        Y_VERIFY_DEBUG(!Count || Entries[Index(0)].Ts <= entry.Ts);
        Entries[Index(-1)] = std::move(entry);
        ++Count;
    }

private:
    ui32 Index(ui32 offset) const
    {
        return (Count - offset - 1) % Entries.size();
    }
};

}    // namespace NCloud::NBlockStore::NStorage
