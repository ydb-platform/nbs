#pragma once

#include "util/datetime/base.h"

#include <library/cpp/containers/ring_buffer/ring_buffer.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceStat
{
public:
    enum class EDeviceStatus
    {
        Ok,
        Unavailable,
        SilentBroken,
        Broken,
    };

    [[nodiscard]] EDeviceStatus GetDeviceStatus() const;
    [[nodiscard]] TInstant GetBrokenTransitionTs() const;
    [[nodiscard]] TInstant GetFirstTimedOutRequestStartTs() const;
    [[nodiscard]] TInstant GetLastSuccessfulRequestStartTs() const;

    void SetDeviceStatus(EDeviceStatus status);
    void SetBrokenTransitionTs(TInstant ts);
    void SetFirstTimedOutRequestStartTs(TInstant ts);
    void SetLastSuccessfulRequestStartTs(TInstant ts);
    void AddResponseTime(TDuration duration);

    // Returns the maximum request execution time among the latest.
    [[nodiscard]] TDuration WorstRequestTime() const;

    [[nodiscard]] TDuration GetTimedOutStateDuration(TInstant now) const;

    [[nodiscard]] bool CooldownPassed(
        TInstant now,
        TDuration cooldownTimeout) const;

private:
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
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
