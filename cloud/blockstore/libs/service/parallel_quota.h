#pragma once

#include <util/generic/maybe.h>
#include <util/system/types.h>

#include <algorithm>

namespace NCloud::NBlockStore {

// For a fork/join, remove quota delay from each child's completion before
// choosing the last completion. No per-child records are retained.
struct TParallelQuota
{
    ui64 Start = 0;
    ui64 End = 0;
    ui64 QuotaFreeEnd = 0;
    bool Valid = true;

    void Add(ui64 end, ui64 quotaCycles)
    {
        if (end < Start || quotaCycles > end - Start) {
            Valid = false;
            return;
        }
        End = std::max(End, end);
        QuotaFreeEnd = std::max(QuotaFreeEnd, end - quotaCycles);
    }

    TMaybe<ui64> GetDelay() const
    {
        return Valid ? TMaybe<ui64>(End - QuotaFreeEnd) : Nothing();
    }
};

}   // namespace NCloud::NBlockStore
