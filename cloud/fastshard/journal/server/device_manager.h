#pragma once

#include "public.h"

#include <cloud/fastshard/protos/device.pb.h>

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IDeviceManager
{
    virtual ~IDeviceManager() = default;

    [[nodiscard]] virtual auto AcquireDevices(
        NProto::TAcquireDevicesRequest request)
        -> NThreading::TFuture<NProto::TAcquireDevicesResponse> = 0;

    [[nodiscard]] virtual auto ReleaseDevices(
        NProto::TReleaseDevicesRequest request)
        -> NThreading::TFuture<NProto::TReleaseDevicesResponse> = 0;

    [[nodiscard]] virtual NProto::TError AccessDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode) = 0;

    [[nodiscard]] virtual IDevicePtr CreateDevice(
        const TString& deviceUUID,
        TPageRangeRef region,
        ui32 blockSize) = 0;
};

}   // namespace NCloud::NJournalled
