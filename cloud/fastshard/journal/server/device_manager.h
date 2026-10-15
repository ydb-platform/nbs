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

    /**
     * Acquires the devices for the client in the request, making the client
     * a writer or a reader of each of them, according to the access mode.
     * The access mode is checked on every subsequent AccessDevice() call.
     */
    [[nodiscard]] virtual auto AcquireDevices(
        NProto::TAcquireDevicesRequest request)
        -> NThreading::TFuture<NProto::TAcquireDevicesResponse> = 0;

    /**
     * Releases the devices acquired by the client in the request. After this
     * the client's AccessDevice() calls fail.
     */
    [[nodiscard]] virtual auto ReleaseDevices(
        NProto::TReleaseDevicesRequest request)
        -> NThreading::TFuture<NProto::TReleaseDevicesResponse> = 0;

    /**
     * Checks that the client may access the device in the given mode. Call it
     * before every I/O request on behalf of a client: the check is cheap and
     * the client's rights may change between requests, as another client may
     * acquire the device for writing in the meantime.
     */
    [[nodiscard]] virtual NProto::TError AccessDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode) = 0;

    /**
     * Creates a device over a region of the device with the given UUID.
     * The result does not belong to any client and does not check access.
     * The user should check access with AccessDevice() before each I/O request.
     */
    [[nodiscard]] virtual IDevicePtr CreateDevice(
        const TString& deviceUUID,
        TPageRangeRef region,
        ui32 blockSize) = 0;
};

}   // namespace NCloud::NJournalled
