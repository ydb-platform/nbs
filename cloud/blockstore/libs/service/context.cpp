#include "context.h"

#include <cloud/blockstore/public/api/protos/headers.pb.h>

#include <util/datetime/cputimer.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TCallContext::TCallContext(ui64 requestId)
    : TCallContextBase(requestId)
{}

void TCallContext::AddLatencyThrottleDelay(TDuration delay)
{
    if (!delay || !LatencyTimingRequested.load(std::memory_order_relaxed)) {
        return;
    }
    i64 us = LatencyThrottleUs.load(std::memory_order_relaxed);
    while (us >= 0) {
        const i64 next =
            delay.MicroSeconds() > static_cast<ui64>(Max<i64>() - us)
                ? LatencyTimingIncomplete
                : us + static_cast<i64>(delay.MicroSeconds());
        if (LatencyThrottleUs.compare_exchange_weak(us, next,
                                                    std::memory_order_relaxed))
        {
            return;
        }
    }
}

void TCallContext::MarkLatencyTimingIncomplete()
{
    if (LatencyTimingRequested.load(std::memory_order_relaxed)) {
        LatencyThrottleUs.store(LatencyTimingIncomplete,
                                std::memory_order_relaxed);
    }
}

void TCallContext::ConsumeLatencyTiming(NProto::TResponseHeaders& headers)
{
    if (headers.HasThrottleDelayUs()) {
        AddLatencyThrottleDelay(
            TDuration::MicroSeconds(headers.GetThrottleDelayUs()));
        headers.ClearThrottleDelayUs();
    } else {
        MarkLatencyTimingIncomplete();
    }
}

bool TCallContext::GetSilenceRetriableErrors() const
{
    return SilenceRetriableErrors.load(std::memory_order_acquire);
}

void TCallContext::SetSilenceRetriableErrors(bool silence)
{
    SilenceRetriableErrors.store(silence, std::memory_order_release);
}

bool TCallContext::GetHasUncountableRejects() const
{
    return HasUncountableRejects.load(std::memory_order_acquire);
}

void TCallContext::SetHasUncountableRejects()
{
    HasUncountableRejects.store(true, std::memory_order_release);
}

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext)
{
    if (!callContext) {
        return {};
    }

    auto* concrete = dynamic_cast<TCallContext*>(callContext.Get());
    Y_ABORT_UNLESS(concrete);
    return TCallContextPtr(concrete);
}

}   // namespace NCloud::NBlockStore
