#include "device_replacement_tracker.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

bool TDeviceReplacementTracker::AddDeviceReplacement(
    const TString& id)
{
    return DeviceReplacementIds.insert(id).second;
}

bool TDeviceReplacementTracker::RemoveDeviceReplacement(
    const TString& id)
{
    return DeviceReplacementIds.erase(id) != 0;
}

bool TDeviceReplacementTracker::HasDeviceReplacement(
    const TString& id) const
{
    return DeviceReplacementIds.contains(id);
}

const THashSet<TString>& TDeviceReplacementTracker::GetDevicesReplacements() const
{
    return DeviceReplacementIds;
}

}   // namespace NCloud::NBlockStore::NStorage
