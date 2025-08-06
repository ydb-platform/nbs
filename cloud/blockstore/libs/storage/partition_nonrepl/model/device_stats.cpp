#include "device_stats.h"

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

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
