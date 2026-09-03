#pragma once

#include <util/generic/map.h>
#include <util/system/spinlock.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

class TWatermarkTracker
{
private:
    mutable TAdaptiveLock Lock;
    ui64 Watermark = 0;
    mutable TMap<ui64, ui64> AcquiredWatermarks;

public:
    void AdvanceWatermark(ui64 watermark);

    ui64 Acquire() const;
    ui64 AcquireFrom(ui64 watermark) const;
    void Release(ui64 watermark) const;

    ui64 GetMinAcquired() const;
};

////////////////////////////////////////////////////////////////////////////////

class TLsnBarrier {
private:
    mutable TAdaptiveLock Lock;
    TMap<ui64, ui64> AcquiredLsns;

public:
    void Acquire(ui64 lsn);
    void Release(ui64 lsn);

    ui64 GetMinAcquired() const;
};

}   // namespace NCloud::NJournalled
