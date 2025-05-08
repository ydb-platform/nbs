#pragma once

#include "util/datetime/base.h"

#include <library/cpp/containers/ring_buffer/ring_buffer.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceStat
{
    enum class EDeviceStatus
    {
        Ok,
        Unavailable,
        SilentBroken,
        Broken,
    };

    // The start time of the first timed out request.
    TInstant FirstTimedOutRequestStartTs;

    // The start time of the last successful request.
    TInstant LastSuccessfulRequestStartTs;

    // Execution times of the last 10 requests.
    TSimpleRingBuffer<TDuration> ResponseTimes{10};

    // The current status of the device.
    EDeviceStatus DeviceStatus = EDeviceStatus::Ok;

    // When the device was considered broken.
    TInstant BrokenTransitionTs;

    // Returns the maximum request execution time among the latest.
    [[nodiscard]] TDuration WorstRequestTime() const;

    [[nodiscard]] TDuration GetTimedOutStateDuration(TInstant now) const;

    [[nodiscard]] bool CooldownPassed(
        TInstant now,
        TDuration cooldownTimeout) const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
