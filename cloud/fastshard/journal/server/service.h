#pragma once

#include "public.h"

#include "config.h"

#include <cloud/fastshard/journal/iface/journalled_device.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IServerBackend : public IJournalledDevice
{
    virtual ~IServerBackend() = default;

    [[nodiscard]] virtual auto AcquireDevices(
        NProto::TAcquireDevicesRequest request)
        -> NThreading::TFuture<NProto::TAcquireDevicesResponse> = 0;

    [[nodiscard]] virtual auto ReleaseDevices(
        NProto::TReleaseDevicesRequest request)
        -> NThreading::TFuture<NProto::TReleaseDevicesResponse> = 0;

    [[nodiscard]] virtual auto FormatDevice(
        NProto::TFormatDeviceRequest request)
        -> NThreading::TFuture<NProto::TFormatDeviceResponse> = 0;

    [[nodiscard]] virtual auto ListDevices(NProto::TListDevicesRequest request)
        -> NThreading::TFuture<NProto::TListDevicesResponse> = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TJournalledDeviceSpec
{
    IJournalledDevicePtr Device;
    TJournalledDeviceConfig Config;
};

////////////////////////////////////////////////////////////////////////////////

IServerBackendPtr CreateService(
    IDeviceManagerPtr deviceManager,
    TVector<TJournalledDeviceSpec> journalledDevices);

}   // namespace NCloud::NJournalled
