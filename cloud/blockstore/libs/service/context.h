#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/context.h>

#include <atomic>
#include <optional>

namespace NCloud::NBlockStore::NProto {
class TResponseHeaders;
}

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final: public TCallContextBase
{
private:
    static constexpr i64 LatencyTimingDisabled = -1;
    static constexpr i64 LatencyTimingIncomplete = -2;
    std::atomic<i64> LatencyThrottleUs{LatencyTimingDisabled};

    TAtomic SilenceRetriableErrors = false;
    TAtomic HasUncountableRejects = false;

public:
    TCallContext(ui64 requestId = 0);

    void EnableLatencyTiming();

    bool IsLatencyTimingEnabled() const
    {
        return LatencyThrottleUs.load(std::memory_order_relaxed) >= 0;
    }

    std::optional<TDuration> GetLatencyThrottleDelay() const;
    void AddLatencyThrottleDelay(TDuration delay);
    void MarkLatencyTimingIncomplete();
    void ConsumeLatencyTiming(NProto::TResponseHeaders& headers);

    bool GetSilenceRetriableErrors() const;
    void SetSilenceRetriableErrors(bool silence);

    bool GetHasUncountableRejects() const;
    void SetHasUncountableRejects();
};

////////////////////////////////////////////////////////////////////////////////

inline TCallContextPtr CreateCallContext(ui64 requestId = 0)
{
    return MakeIntrusive<TCallContext>(requestId);
}

////////////////////////////////////////////////////////////////////////////////

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext);

}   // namespace NCloud::NBlockStore
