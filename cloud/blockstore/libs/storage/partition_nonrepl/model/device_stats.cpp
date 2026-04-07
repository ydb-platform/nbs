#include "device_stats.h"

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TDeviceStat::TDeviceStat(TString deviceUUID, IDeviceStatObserver* observer)
    : DeviceUUID(std::move(deviceUUID))
    , Observer(observer)
{}

TDeviceStat::EDeviceStatus TDeviceStat::GetDeviceStatus() const
{
    return DeviceStatus;
}

void TDeviceStat::MarkOk(TInstant requestStartTs, TDuration executionTime)
{
    const bool wasBroken = DeviceStatus == EDeviceStatus::Broken ||
                           DeviceStatus == EDeviceStatus::SilentBroken;

    FirstTimedOutRequestStartTs = {};
    LastSuccessfulRequestStartTs = requestStartTs;
    ResponseTimes.PushBack(executionTime);
    DeviceStatus = EDeviceStatus::Ok;
    BrokenTransitionTs = {};

    if (wasBroken && Observer) {
        Observer->OnDeviceRecovered(DeviceUUID);
    }
}

void TDeviceStat::MarkBroken(TInstant now, bool notifyObserver)
{
    if (DeviceStatus == EDeviceStatus::Broken) {
        return;
    }
    BrokenTransitionTs = now;
    DeviceStatus = EDeviceStatus::Broken;
    if (notifyObserver && Observer) {
        Observer->OnDeviceBroken(DeviceUUID, now);
    }
}

void TDeviceStat::MarkUnavailable()
{
    DeviceStatus = EDeviceStatus::Unavailable;
}

void TDeviceStat::MarkBackOnline()
{
    if (DeviceStatus <= EDeviceStatus::Unavailable) {
        DeviceStatus = EDeviceStatus::Ok;
        FirstTimedOutRequestStartTs = {};
    }
}

void TDeviceStat::HandleTimeout(
    TInstant requestStartTs,
    TInstant now,
    TDuration maxTimedOutStateDuration,
    TDuration agentMaxTimeout)
{
    if (requestStartTs < LastSuccessfulRequestStartTs) {
        return;
    }

    if (!FirstTimedOutRequestStartTs) {
        FirstTimedOutRequestStartTs = requestStartTs;
    }

    switch (DeviceStatus) {
        case EDeviceStatus::Ok:
        case EDeviceStatus::Unavailable: {
            if (GetTimedOutStateDuration(now) > maxTimedOutStateDuration) {
                BrokenTransitionTs = now;
                DeviceStatus = EDeviceStatus::SilentBroken;
                if (Observer) {
                    Observer->OnDeviceBroken(DeviceUUID, now);
                }
            }
            break;
        }
        case EDeviceStatus::SilentBroken: {
            if (CooldownPassed(now, agentMaxTimeout)) {
                DeviceStatus = EDeviceStatus::Broken;
                if (Observer) {
                    Observer->OnDeviceBroken(DeviceUUID, now);
                }
            }
            break;
        }
        case EDeviceStatus::Broken: {
            break;
        }
    }
}

TDuration TDeviceStat::WorstRequestTime() const
{
    TDuration result;
    for (size_t i = ResponseTimes.FirstIndex(); i < ResponseTimes.TotalSize();
         ++i)
    {
        result = Max(result, ResponseTimes[i]);
    }
    return result;
}

TDuration TDeviceStat::GetTimedOutStateDuration(TInstant now) const
{
    return FirstTimedOutRequestStartTs ? (now - FirstTimedOutRequestStartTs)
                                       : TDuration();
}

bool TDeviceStat::CooldownPassed(TInstant now, TDuration cooldownTimeout) const
{
    switch (DeviceStatus) {
        case EDeviceStatus::Ok:
        case EDeviceStatus::Unavailable:
            return false;
        case EDeviceStatus::SilentBroken:
            return BrokenTransitionTs + cooldownTimeout < now;
        case EDeviceStatus::Broken:
            return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
