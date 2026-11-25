#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TDevices = google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>;

struct TUnacquiredShadowDiskDevices
{
    ui64 BlocksEnd = 0;
    TString DeviceUUIDs;

    TString GetErrorMessage() const;
};

TUnacquiredShadowDiskDevices FindUnacquiredShadowDiskDevices(
    const TDevices& devices,
    const TDevices& acquired);

////////////////////////////////////////////////////////////////////////////////

class TShadowDiskReadAvailability
{
private:
    TUnacquiredShadowDiskDevices UnacquiredDevices;
    std::optional<TInstant> UnavailableReadSince;

public:
    enum class EReadStatus
    {
        Available,
        Retry,
        Error,
    };

    void UpdateDevices(const TDevices& devices, const TDevices& acquired);
    EReadStatus CheckRead(ui64 startIndex, TInstant now, TDuration timeout);
    EReadStatus CheckUnavailableRead(TInstant now, TDuration timeout);
    TString GetErrorMessage() const;
};

}   // namespace NCloud::NBlockStore::NStorage
