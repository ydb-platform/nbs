#include "device_stats.h"

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TDeviceStat::EDeviceStatus TDeviceStat::GetDeviceStatus() const
{
    return DeviceStatus;
}

void TDeviceStat::SetDeviceStatus(EDeviceStatus status)
{
    DeviceStatus = status;
}

void TDeviceStat::SetBrokenTransitionTs(TInstant ts)
{
    BrokenTransitionTs = ts;
}

void TDeviceStat::SetFirstTimedOutRequestStartTs(TInstant ts)
{
    FirstTimedOutRequestStartTs = ts;
}

void TDeviceStat::MarkOk(TInstant requestStartTs, TDuration executionTime)
{
    FirstTimedOutRequestStartTs = {};
    LastSuccessfulRequestStartTs = requestStartTs;
    ResponseTimes.PushBack(executionTime);
    DeviceStatus = EDeviceStatus::Ok;
    BrokenTransitionTs = {};
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
            }
            break;
        }
        case EDeviceStatus::SilentBroken: {
            if (CooldownPassed(now, agentMaxTimeout)) {
                DeviceStatus = EDeviceStatus::Broken;
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
