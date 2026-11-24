#include "shadow_disk_read_availability.h"

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TShadowDiskReadAvailability::UpdateDevices(
    const google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>& devices,
    const google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>& acquired,
    ui32 blockSize)
{
    THashSet<TString> acquiredUUIDs;
    for (const auto& device: acquired) {
        acquiredUUIDs.insert(device.GetDeviceUUID());
    }

    UnavailableBlocksEnd = 0;
    UnavailableDeviceUUIDs.clear();
    ui64 blocksEnd = 0;
    for (const auto& device: devices) {
        blocksEnd += device.GetBlocksCount() * device.GetBlockSize() / blockSize;
        if (!acquiredUUIDs.contains(device.GetDeviceUUID())) {
            UnavailableBlocksEnd = blocksEnd;
            if (!UnavailableDeviceUUIDs.empty()) {
                UnavailableDeviceUUIDs += ", ";
            }
            UnavailableDeviceUUIDs += device.GetDeviceUUID();
        }
    }
    if (UnavailableDeviceUUIDs.empty()) {
        UnavailableReadSince.reset();
    }
}

TShadowDiskReadAvailability::EReadStatus
TShadowDiskReadAvailability::CheckRead(
    ui64 startIndex,
    TInstant now,
    TDuration timeout)
{
    if (startIndex >= UnavailableBlocksEnd) {
        return EReadStatus::Available;
    }
    if (!UnavailableReadSince) {
        UnavailableReadSince = now;
    }
    return now - *UnavailableReadSince < timeout ? EReadStatus::Retry
                                                : EReadStatus::Error;
}

TString TShadowDiskReadAvailability::GetErrorMessage() const
{
    return TStringBuilder() << "Devices [" << UnavailableDeviceUUIDs
                            << "] are unavailable";
}

}   // namespace NCloud::NBlockStore::NStorage
