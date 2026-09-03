#include "lsn_barriers.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/utility.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

void TWatermarkTracker::AdvanceWatermark(ui64 watermark)
{
    with_lock (Lock) {
        if (Watermark < watermark) {
            Watermark = watermark;
        }
    }
}

ui64 TWatermarkTracker::Acquire() const
{
    with_lock (Lock) {
        auto it = AcquiredWatermarks.emplace(Watermark, 0).first;
        ++it->second;
        return Watermark;
    }
}

ui64 TWatermarkTracker::AcquireFrom(ui64 watermark) const
{
    with_lock (Lock) {
        watermark = Max(watermark, Watermark);
        auto it = AcquiredWatermarks.emplace(watermark, 0).first;
        ++it->second;
        return watermark;
    }
}

void TWatermarkTracker::Release(ui64 watermark) const
{
    with_lock (Lock) {
        auto it = AcquiredWatermarks.find(watermark);

        STORAGE_VERIFY(
            it != AcquiredWatermarks.end(),
            TWellKnownEntityTypes::DEVICE,
            watermark);

        if (--it->second == 0) {
            AcquiredWatermarks.erase(it);
        }
    }
}

ui64 TWatermarkTracker::GetMinAcquired() const
{
    with_lock (Lock) {
        if (AcquiredWatermarks.empty()) {
            return Watermark;
        }
        return Min(Watermark, AcquiredWatermarks.begin()->first);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLsnBarrier::Acquire(ui64 lsn) {
    with_lock (Lock) {
        auto it = AcquiredLsns.emplace(lsn, 0).first;
        ++it->second;
    }
}

void TLsnBarrier::Release(ui64 lsn) {
    with_lock (Lock) {
        auto it = AcquiredLsns.find(lsn);

        STORAGE_VERIFY(
            it != AcquiredLsns.end(),
            TWellKnownEntityTypes::DEVICE,
            lsn);

        if (--it->second == 0) {
            AcquiredLsns.erase(it);
        }
    }
}

ui64 TLsnBarrier::GetMinAcquired() const
{
    with_lock (Lock) {
        if (AcquiredLsns.empty()) {
            return 0;
        }
        return AcquiredLsns.begin()->first;
    }
}

}   // namespace NCloud::NJournalled
