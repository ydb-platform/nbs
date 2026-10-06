#include "io_depth_tracker.h"

#include <library/cpp/int128/int128.h>

#include <util/system/guard.h>
#include <util/system/spinlock.h>
#include <util/system/yassert.h>

#include <chrono>
#include <limits>

namespace NCloud {

namespace {

TIoDepthClock MakeIoDepthClock()
{
    const auto origin = std::chrono::steady_clock::now();

    return [origin] {
        return static_cast<ui64>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - origin).count());
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TIoDepthTracker::TImpl
{
    struct TLane
    {
        ui64 Current = 0;
        ui64 LastNs = 0;
        ui128 IntegralNs = 0;
    };

    TAdaptiveLock Lock;

    const TGUID Generation = TGUID::Create();
    const TIoDepthClock Clock;

    TVector<TLane> Lanes;

    ui64 LastObservedNs = 0;
    bool Continuous = true;

    TImpl(ui32 laneCount, TIoDepthClock clock)
        : Clock(clock ? std::move(clock) : MakeIoDepthClock())
        , Lanes(laneCount)
    {
        LastObservedNs = Clock();

        for (auto& lane : Lanes) {
            lane.LastNs = LastObservedNs;
        }
    }

    ui64 ReadNow()
    {
        const ui64 now = Clock();

        if (now < LastObservedNs) {
            Continuous = false;
            return LastObservedNs;
        }

        LastObservedNs = now;
        return now;
    }

    void Advance(TLane& lane, ui64 now)
    {
        lane.IntegralNs += ui128(lane.Current) * ui128(now - lane.LastNs);
        lane.LastNs = now;
    }
};

TIoDepthTracker::TIoDepthTracker(
    ui32 laneCount,
    TIoDepthClock clock)
    : Impl(std::make_unique<TImpl>(laneCount, std::move(clock)))
{}

TIoDepthTracker::~TIoDepthTracker() = default;

void TIoDepthTracker::Started(ui32 index)
{
    TGuard<TAdaptiveLock> guard(Impl->Lock);

    Y_ABORT_UNLESS(index < Impl->Lanes.size());

    auto& lane = Impl->Lanes[index];

    Impl->Advance(lane, Impl->ReadNow());

    if (lane.Current == std::numeric_limits<ui64>::max()) {
        Impl->Continuous = false;
        return;
    }

    ++lane.Current;
}

bool TIoDepthTracker::Completed(ui32 index)
{
    TGuard<TAdaptiveLock> guard(Impl->Lock);

    Y_ABORT_UNLESS(index < Impl->Lanes.size());

    auto& lane = Impl->Lanes[index];
    Impl->Advance(lane, Impl->ReadNow());

    if (!lane.Current) {
        Impl->Continuous = false;
        return false;
    }

    --lane.Current;
    return true;
}

TIoDepthSnapshot TIoDepthTracker::Snapshot()
{
    TGuard<TAdaptiveLock> guard(Impl->Lock);

    const ui64 now = Impl->ReadNow();

    TIoDepthSnapshot result;
    result.Generation = Impl->Generation;
    result.TimestampNs = now;
    result.Lanes.reserve(Impl->Lanes.size());

    for (auto& lane : Impl->Lanes) {
        Impl->Advance(lane, now);

        const ui128 integralUs = lane.IntegralNs / ui128(1000);

        if (integralUs > ui128(std::numeric_limits<ui64>::max())) {
            Impl->Continuous = false;
            result.Lanes.push_back({lane.Current, 0});
        } else {
            result.Lanes.push_back({lane.Current, static_cast<ui64>(integralUs)});
        }
    }

    result.Continuous = Impl->Continuous;
    return result;
}

void TIoDepthTracker::MarkDiscontinuity()
{
    TGuard<TAdaptiveLock> guard(Impl->Lock);

    const ui64 now = Impl->ReadNow();

    for (auto& lane : Impl->Lanes) {
        Impl->Advance(lane, now);
    }

    Impl->Continuous = false;
}

} // namespace NCloud
