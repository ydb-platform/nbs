#include "watermark_tracker.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

void TWatermarkTracker::Advance(ui64 watermark)
{
    with_lock (Lock) {
        if (Watermark < watermark) {
            Watermark = watermark;
        }
    }
}

ui64 TWatermarkTracker::Pin()
{
    with_lock (Lock) {
        auto it = PinCountByWatermark.emplace(Watermark, 0).first;
        ++it->second;
        return Watermark;
    }
}

ui64 TWatermarkTracker::PinAtLeast(ui64 watermark)
{
    with_lock (Lock) {
        watermark = Max(watermark, Watermark);
        auto it = PinCountByWatermark.emplace(watermark, 0).first;
        ++it->second;
        return watermark;
    }
}

void TWatermarkTracker::Unpin(ui64 watermark)
{
    with_lock (Lock) {
        auto it = PinCountByWatermark.find(watermark);

        Y_ABORT_UNLESS(
            it != PinCountByWatermark.end(),
            "unpinning a watermark that was never pinned: %lu",
            watermark);

        if (--it->second == 0) {
            PinCountByWatermark.erase(it);
        }
    }
}

ui64 TWatermarkTracker::GetPinnedWatermark() const
{
    with_lock (Lock) {
        if (PinCountByWatermark.empty()) {
            return Watermark;
        }
        return Min(Watermark, PinCountByWatermark.begin()->first);
    }
}

}   // namespace NCloud::NJournalled
