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
    static constexpr i64 LatencyTimingIncomplete = -1;
    std::atomic<i64> LatencyThrottleUs{0};
    std::atomic<bool> LatencyTimingRequested{false};
    std::atomic<bool> SilenceRetriableErrors{false};
    std::atomic<bool> HasUncountableRejects{false};

public:
    TCallContext(ui64 requestId = 0);

    void EnableLatencyTiming()
    {
        // Re-enabling cannot reset accumulated waits or incomplete timing.
        LatencyTimingRequested.store(true, std::memory_order_relaxed);
    }

    bool IsLatencyTimingEnabled() const
    {
        return LatencyTimingRequested.load(std::memory_order_relaxed) &&
               LatencyThrottleUs.load(std::memory_order_relaxed) >= 0;
    }

    std::optional<TDuration> GetLatencyThrottleDelay() const
    {
        if (LatencyTimingRequested.load(std::memory_order_relaxed)) {
            const i64 us = LatencyThrottleUs.load(std::memory_order_relaxed);
            if (us >= 0) {
                return TDuration::MicroSeconds(us);
            }
        }
        return std::nullopt;
    }

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
