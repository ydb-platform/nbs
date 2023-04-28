#pragma once

#include "public.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/lwtrace/shuttle.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class EProcessingStage
{
    Postponed,
    Backoff,
    Last
};

////////////////////////////////////////////////////////////////////////////////

struct TCallContextBase
    : public TThrRefBase
{
private:
    TAtomic Stage2Time[static_cast<int>(EProcessingStage::Last)] = {};
    TAtomic RequestStartedCycles = 0;

    // Used only in tablet throttler.
    TAtomic PostponeTs = 0;

protected:
    TAtomic PostponeTsCycles = 0;

public:
    ui64 RequestId;
    NLWTrace::TOrbit LWOrbit;

    TCallContextBase(ui64 requestId);

    ui64 GetRequestStartedCycles() const;
    void SetRequestStartedCycles(ui64 cycles);

    ui64 GetPostponeCycles() const;
    void SetPostponeCycles(ui64 cycles);

    void Postpone(ui64 nowCycles);
    TDuration Advance(ui64 nowCycles);

    TDuration Time(EProcessingStage stage) const;
    void AddTime(EProcessingStage stage, TDuration d);
};

}   // namespace NCloud
