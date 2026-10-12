#include "io_depth_tracker.h"

#include "busy_idle_calculator.h"

#include <library/cpp/int128/int128.h>

#include <util/system/guard.h>
#include <util/system/spinlock.h>
#include <util/system/yassert.h>

#include <chrono>
#include <limits>

namespace NCloud {

namespace {

struct TDepthStorage
{
    ui128* IntegralNs = nullptr;

    void Register(ui128* integralNs)
    {
        IntegralNs = integralNs;
    }

    void IncrementDepth(ui64 elapsedNs, ui32 depth)
    {
        *IntegralNs += ui128(elapsedNs) * ui128(depth);
    }
};

using TDepthCalculator =
    TBusyIdleTimeCalculator<TDepthStorage, true, TIoDepthClock>;

TIoDepthClock MakeIoDepthClock()
{
    const auto origin = std::chrono::steady_clock::now();

    return [origin]
    {
        return static_cast<ui64>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - origin)
                .count());
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TIoDepthTracker::TImpl
{
    struct TLane
    {
        ui128 IntegralNs = 0;
        TDepthCalculator Calculator;

        explicit TLane(TIoDepthClock clock)
            : Calculator(std::move(clock))
        {
            Calculator.Register(&IntegralNs);
        }
    };

    TAdaptiveLock Lock;

    const TGUID Generation = TGUID::Create();
    const TIoDepthClock Clock;

    TVector<std::unique_ptr<TLane>> Lanes;

    ui64 LastObservedNs = 0;
    bool Continuous = true;

    TImpl(ui32 laneCount, TIoDepthClock clock)
        : Clock(clock ? std::move(clock) : MakeIoDepthClock())
    {
        LastObservedNs = Clock();

        Lanes.reserve(laneCount);
        for (ui32 i = 0; i < laneCount; ++i) {
            // All calculators use the same observation time under Lock.
            Lanes.push_back(
                std::make_unique<TLane>([this] { return LastObservedNs; }));
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
};

TIoDepthTracker::TIoDepthTracker(ui32 laneCount, TIoDepthClock clock)
    : Impl(std::make_unique<TImpl>(laneCount, std::move(clock)))
{}

TIoDepthTracker::~TIoDepthTracker() = default;

void TIoDepthTracker::Started(ui32 index)
{
    TGuard<TAdaptiveLock> guard(Impl->Lock);

    Y_ABORT_UNLESS(index < Impl->Lanes.size());

    auto& calculator = Impl->Lanes[index]->Calculator;
    Impl->ReadNow();

    if (calculator.GetInflight() == std::numeric_limits<ui32>::max()) {
        calculator.OnUpdateStats();
        Impl->Continuous = false;
        return;
    }

    calculator.OnRequestStarted();
}

bool TIoDepthTracker::Completed(ui32 index)
{
    TGuard<TAdaptiveLock> guard(Impl->Lock);

    Y_ABORT_UNLESS(index < Impl->Lanes.size());

    auto& calculator = Impl->Lanes[index]->Calculator;
    Impl->ReadNow();

    if (!calculator.GetInflight()) {
        calculator.OnUpdateStats();
        Impl->Continuous = false;
        return false;
    }

    calculator.OnRequestCompleted();
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

    for (auto& lane: Impl->Lanes) {
        lane->Calculator.OnUpdateStats();

        const ui64 current = lane->Calculator.GetInflight();
        const ui128 integralUs = lane->IntegralNs / ui128(1000);

        if (integralUs > ui128(std::numeric_limits<ui64>::max())) {
            Impl->Continuous = false;
            result.Lanes.push_back({current, 0});
        } else {
            result.Lanes.push_back({current, static_cast<ui64>(integralUs)});
        }
    }

    result.Continuous = Impl->Continuous;
    return result;
}

}   // namespace NCloud
