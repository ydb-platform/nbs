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
    TMap<ui64, ui64> PinCountByWatermark;

public:
    void Advance(ui64 watermark);

    [[nodiscard]] ui64 Pin();
    [[nodiscard]] ui64 PinAtLeast(ui64 watermark);

    void Unpin(ui64 watermark);

    ui64 GetPinnedWatermark() const;
};

}   // namespace NCloud::NJournalled
