#include "context.h"

#include <cloud/blockstore/public/api/protos/headers.pb.h>

#include <util/datetime/cputimer.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TCallContext::TCallContext(ui64 requestId)
    : TCallContextBase(requestId)
{}

void TCallContext::EnableLatencyTiming()
{
    i64 disabled = LatencyTimingDisabled;
    LatencyThrottleUs.compare_exchange_strong(disabled, 0,
                                              std::memory_order_relaxed);
}

std::optional<TDuration> TCallContext::GetLatencyThrottleDelay() const
{
    const i64 us = LatencyThrottleUs.load(std::memory_order_relaxed);
    if (us < 0) {
        return std::nullopt;
    }
    return TDuration::MicroSeconds(us);
}

void TCallContext::AddLatencyThrottleDelay(TDuration delay)
{
    if (!delay) {
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
    if (IsLatencyTimingEnabled()) {
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
    return AtomicGet(SilenceRetriableErrors);
}

void TCallContext::SetSilenceRetriableErrors(bool silence)
{
    AtomicSet(SilenceRetriableErrors, silence);
}

bool TCallContext::GetHasUncountableRejects() const
{
    return AtomicGet(HasUncountableRejects);
}

void TCallContext::SetHasUncountableRejects()
{
    AtomicSet(HasUncountableRejects, true);
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
