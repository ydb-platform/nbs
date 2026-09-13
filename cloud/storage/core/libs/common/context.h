#pragma once

#include "public.h"

#include "request_timing_snapshot.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/lwtrace/shuttle.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <atomic>
#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class EProcessingStage
{
    Postponed,
    Backoff,
    Shaping,

    Last = Shaping,
};

struct TRequestTime
{
    TDuration TotalTime;
    TDuration ExecutionTime;

    explicit operator bool() const
    {
        return TotalTime || ExecutionTime;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestTimingCollector;

struct TCallContextBase: public TThrRefBase
{
private:
    TAtomic Stage2Time[static_cast<int>(EProcessingStage::Last) + 1] = {};
    TAtomic RequestStartedCycles = 0;
    TAtomic ResponseSentCycles = 0;
    TAtomic PossiblePostponeMicroSeconds = 0;
    TAtomic PostponeTsCycles = 0;

    // Used only in tablet throttler.
    TInstant PostponeTs = TInstant::Zero();

    TCallContextBasePtr LegacyParent;

public:
    ui64 RequestId;

private:
    NLWTrace::TOrbit OwnLWOrbit;
    enum class ETimingPhase: ui8
    {
        Disabled,
        Enabled,
        Locked,
        CollectorReady,
        Frozen,
    };
    class TTimingGuard;

    struct TFrozenTiming
    {
        ui64 RequestId = 0;
        ui64 TotalMicros = 0;
        ui32 ErrorCode = 0;
    };

    // Phase publishes immutable origin/collector ownership and frozen value.
    std::atomic<ETimingPhase> TimingPhase{ETimingPhase::Disabled};
    std::shared_ptr<TRequestTimingCollector> RequestTiming;
    TFrozenTiming FrozenTiming;
    ui64 TimingOrigin = 0;
    ui32 TimingPart = 0;

    ETimingPhase LoadTimingPhase() const;
    TRequestTimingSnapshot GetFrozenRequestTiming() const;
    TRequestTimingCollector* GetOrCreateRequestTiming();
    ui64 TimingOffset(ui64 cycles) const;
    void AddLegacyTime(EProcessingStage stage, TDuration d);
    void SetLegacyPostponeCycles(ui64 cycles);

public:
    // Child diagnostic contexts retain the original shared trace stream.
    NLWTrace::TOrbit& LWOrbit;

    TCallContextBase(ui64 requestId, TCallContextBasePtr parent = {});

    TDuration GetPossiblePostponeDuration() const;
    void SetPossiblePostponeDuration(TDuration d);

    ui64 GetRequestStartedCycles() const;
    void SetRequestStartedCycles(ui64 cycles);

    TInstant GetPostponeTs() const;
    void SetPostponeTs(TInstant ts);

    ui64 GetResponseSentCycles() const;
    void SetResponseSentCycles(ui64 cycles);

    void Postpone(ui64 nowCycles);
    TDuration Advance(ui64 nowCycles);

    TDuration Time(EProcessingStage stage) const;
    void AddTime(EProcessingStage stage, TDuration d);

    TRequestTime CalcRequestTime(ui64 nowCycles) const;

    // Optional diagnostic recording. Legacy counters retain their meaning.
    void EnableRequestTiming();
    bool IsRequestTimingEnabled() const;
    static ui64 GetThreadTimingEventSequence();
    ui32 ForkRequestTiming(ui64 nowCycles);
    void InitChildRequestTiming(
        TCallContextBasePtr parent, ui32 fork, ui64 nowCycles);
    void FinishRequestTiming(ui64 nowCycles);
    void JoinRequestTiming(
        const TVector<TCallContextBasePtr>& children, ui64 nowCycles);
    void CancelRequestTiming(ui64 nowCycles);
    void MarkRequestTimingIncomplete(TString reason);
    void AddTimedWait(EProcessingStage stage, ui64 beginCycles, ui64 endCycles);
    TRequestTimingSnapshot FreezeRequestTiming(
        TDuration total, ui32 errorCode = 0);
    TString CompleteRequestTiming(TDuration total, ui32 errorCode = 0);
};

}   // namespace NCloud
