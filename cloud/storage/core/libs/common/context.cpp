#include "context.h"

#include <util/datetime/cputimer.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TCallContextBase::TCallContextBase(ui64 requestId)
    : RequestId(requestId)
{}

ui64 TCallContextBase::GetRequestStartedCycles() const
{
    return AtomicGet(RequestStartedCycles);
}

void TCallContextBase::SetRequestStartedCycles(ui64 cycles)
{
    AtomicSet(RequestStartedCycles, cycles);
}

ui64 TCallContextBase::GetPostponeCycles() const
{
    return AtomicGet(PostponeTs);
}

void TCallContextBase::SetPostponeCycles(ui64 cycles)
{
    AtomicSet(PostponeTs, cycles);
}

ui64 TCallContextBase::GetResponseSentCycles() const {
    return AtomicGet(ResponseSentCycles);
}

void TCallContextBase::SetResponseSentCycles(ui64 cycles) {
    AtomicSet(ResponseSentCycles, cycles);
}

void TCallContextBase::Postpone(ui64 nowCycles)
{
    Y_VERIFY_DEBUG(
        AtomicGet(PostponeTsCycles) == 0,
        "Request was not advanced.");
    AtomicSet(PostponeTsCycles, nowCycles);
}

TDuration TCallContextBase::Advance(ui64 nowCycles)
{
    const auto start = AtomicGet(PostponeTsCycles);
    Y_VERIFY_DEBUG(start != 0, "Request was not postponed.");

    const auto delay = CyclesToDurationSafe(nowCycles - start);
    AddTime(EProcessingStage::Postponed, delay);
    AtomicSet(PostponeTsCycles, 0);

    return delay;
}

TDuration TCallContextBase::Time(EProcessingStage stage) const
{
    return TDuration::MicroSeconds(AtomicGet(Stage2Time[static_cast<int>(stage)]));
}

void TCallContextBase::AddTime(EProcessingStage stage, TDuration d)
{
    AtomicAdd(Stage2Time[static_cast<int>(stage)], d.MicroSeconds());
}

}   // namespace NCloud
