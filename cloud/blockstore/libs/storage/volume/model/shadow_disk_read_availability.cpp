#include "shadow_disk_read_availability.h"

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString TUnacquiredShadowDiskDevices::GetErrorMessage() const
{
    return TStringBuilder()
           << "Devices [" << DeviceUUIDs << "] are unavailable";
}

TUnacquiredShadowDiskDevices FindUnacquiredShadowDiskDevices(
    const TDevices& devices,
    const TDevices& acquired)
{
    THashSet<TString> acquiredUUIDs;
    for (const auto& device: acquired) {
        acquiredUUIDs.insert(device.GetDeviceUUID());
    }

    TUnacquiredShadowDiskDevices result;
    ui64 blocksEnd = 0;
    for (const auto& device: devices) {
        // Disk Registry describes devices in logical blocks.
        blocksEnd += device.GetBlocksCount();
        if (!acquiredUUIDs.contains(device.GetDeviceUUID())) {
            result.BlocksEnd = blocksEnd;
            if (!result.DeviceUUIDs.empty()) {
                result.DeviceUUIDs += ", ";
            }
            result.DeviceUUIDs += device.GetDeviceUUID();
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TShadowDiskReadAvailability::UpdateDevices(
    const TDevices& devices,
    const TDevices& acquired)
{
    UnacquiredDevices = FindUnacquiredShadowDiskDevices(devices, acquired);
    if (FirstUnavailableRead &&
        FirstUnavailableRead->StartIndex >= UnacquiredDevices.BlocksEnd)
    {
        FirstUnavailableRead.reset();
    }
}

TShadowDiskReadAvailability::EReadStatus TShadowDiskReadAvailability::CheckRead(
    ui64 startIndex,
    TInstant now,
    TDuration timeout)
{
    // A sequential snapshot reader can proceed only past the last unavailable
    // device. Earlier reads must see the failure before reaching that device.
    if (startIndex >= UnacquiredDevices.BlocksEnd) {
        return EReadStatus::Available;
    }
    return CheckUnavailableRead(startIndex, now, timeout);
}

TShadowDiskReadAvailability::EReadStatus
TShadowDiskReadAvailability::CheckUnavailableRead(
    ui64 startIndex,
    TInstant now,
    TDuration timeout)
{
    if (!FirstUnavailableRead) {
        FirstUnavailableRead = TUnavailableRead{
            .StartIndex = startIndex,
            .Since = now,
        };
    }
    return now - FirstUnavailableRead->Since < timeout ? EReadStatus::Retry
                                                       : EReadStatus::Error;
}

TString TShadowDiskReadAvailability::GetErrorMessage() const
{
    return UnacquiredDevices.GetErrorMessage();
}

}   // namespace NCloud::NBlockStore::NStorage
