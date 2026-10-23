#pragma once

#include "public.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/lwtrace/shuttle.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

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

struct TCallContextBase
    : public TThrRefBase
{
private:
    TAtomic Stage2Time[static_cast<int>(EProcessingStage::Last) + 1] = {};
    TAtomic RequestStartedCycles = 0;
    TAtomic ResponseSentCycles = 0;
    TAtomic PossiblePostponeMicroSeconds = 0;
    TAtomic PostponeTsCycles = 0;

    // Used only in tablet throttler.
    TInstant PostponeTs = TInstant::Zero();

    // Set by the tablet throttler. Negative means not measured.
    TAtomic ThrottlerQuotaDelayMicroSeconds = -1;
    TAtomic ThrottlerQuotaRejected = false;

public:
    ui64 RequestId;
    NLWTrace::TOrbit LWOrbit;

    TCallContextBase(ui64 requestId);

    TDuration GetPossiblePostponeDuration() const;
    void SetPossiblePostponeDuration(TDuration d);

    ui64 GetRequestStartedCycles() const;
    void SetRequestStartedCycles(ui64 cycles);

    TInstant GetPostponeTs() const;
    void SetPostponeTs(TInstant ts);

    ui64 GetResponseSentCycles() const;
    void SetResponseSentCycles(ui64 cycles);

    // The part of the tablet throttler delay attributed to the original
    // performance profile. Nothing() means it was not measured.
    TMaybe<TDuration> GetThrottlerQuotaDelay() const;
    void SetThrottlerQuotaDelay(TMaybe<TDuration> d);

    bool GetThrottlerQuotaRejected() const;
    void SetThrottlerQuotaRejected(bool rejected);

    void Postpone(ui64 nowCycles);
    TDuration Advance(ui64 nowCycles);

    TDuration Time(EProcessingStage stage) const;
    void AddTime(EProcessingStage stage, TDuration d);

    TRequestTime CalcRequestTime(ui64 nowCycles) const;
};

}   // namespace NCloud
