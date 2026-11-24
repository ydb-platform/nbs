#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TShadowDiskReadAvailability
{
private:
    ui64 UnavailableBlocksEnd = 0;
    TString UnavailableDeviceUUIDs;
    std::optional<TInstant> UnavailableReadSince;

public:
    enum class EReadStatus
    {
        Available,
        Retry,
        Error,
    };

    void UpdateDevices(
        const google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>& devices,
        const google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>& acquired,
        ui32 blockSize);
    EReadStatus CheckRead(ui64 startIndex, TInstant now, TDuration timeout);
    TString GetErrorMessage() const;
};

}   // namespace NCloud::NBlockStore::NStorage
