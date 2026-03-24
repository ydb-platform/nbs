#include "device_stats.h"

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TDeviceStat::EDeviceStatus TDeviceStat::GetDeviceStatus() const
{
    return DeviceStatus;
}

TInstant TDeviceStat::GetBrokenTransitionTs() const
{
    return BrokenTransitionTs;
}

TInstant TDeviceStat::GetFirstTimedOutRequestStartTs() const
{
    return FirstTimedOutRequestStartTs;
}

TInstant TDeviceStat::GetLastSuccessfulRequestStartTs() const
{
    return LastSuccessfulRequestStartTs;
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

void TDeviceStat::SetLastSuccessfulRequestStartTs(TInstant ts)
{
    LastSuccessfulRequestStartTs = ts;
}

void TDeviceStat::AddResponseTime(TDuration duration)
{
    ResponseTimes.PushBack(duration);
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
