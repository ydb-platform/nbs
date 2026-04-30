#pragma once

#include "util/datetime/base.h"

#include <library/cpp/containers/ring_buffer/ring_buffer.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class IDeviceStatObserver
{
public:
    virtual void OnDeviceBroken(
        const TString& deviceUUID,
        TInstant brokenTs) = 0;
    virtual void OnDeviceRecovered(const TString& deviceUUID) = 0;
    virtual ~IDeviceStatObserver() = default;
};

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

    TString DeviceUUID;
    IDeviceStatObserver* Observer = nullptr;

public:
    TDeviceStat(TString deviceUUID, IDeviceStatObserver* observer);

    [[nodiscard]] EDeviceStatus GetDeviceStatus() const;

    void MarkOk(TInstant requestStartTs, TDuration executionTime);
    void MarkBroken(TInstant now);
    void MarkBrokenAndNotify(TInstant now);
    void MarkUnavailable();
    void MarkBackOnline();

    void HandleTimeout(
        TInstant requestStartTs,
        TInstant now,
        TDuration maxTimedOutStateDuration,
        TDuration agentMaxTimeout);

    // Returns the maximum request execution time among the latest.
    [[nodiscard]] TDuration WorstRequestTime() const;

    [[nodiscard]] TDuration GetTimedOutStateDuration(TInstant now) const;

    [[nodiscard]] bool CooldownPassed(
        TInstant now,
        TDuration cooldownTimeout) const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
