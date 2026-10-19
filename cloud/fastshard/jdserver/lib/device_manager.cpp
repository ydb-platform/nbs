#include "device_manager.h"

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/impl/file_device.h>
#include <cloud/fastshard/journal/server/device_manager.h>
#include <cloud/fastshard/protos/device.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/string/builder.h>

#include <mutex>
#include <optional>

namespace NCloud::NFastShard::NJdServer {

using namespace NJournalled;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// A page range of a device, addressed from its first page.
class TRegionDevice final: public IDevice
{
private:
    const IDevicePtr Device;
    const TPageRangeRef Region;

public:
    TRegionDevice(IDevicePtr device, TPageRangeRef region)
        : Device(std::move(device))
        , Region(region)
    {}

    // IDevice

    [[nodiscard]] auto ReadPages(TVector<TPageRangeRef> rangeRefs)
        -> TFuture<TResultOrError<TVector<TBuffer>>> final
    {
        for (auto& ref: rangeRefs) {
            if (auto error = Translate(ref.FirstPageNo, ref.PageCount);
                HasError(error))
            {
                return MakeFuture<TResultOrError<TVector<TBuffer>>>(
                    std::move(error));
            }
        }
        return Device->ReadPages(std::move(rangeRefs));
    }

    [[nodiscard]] auto WritePages(TVector<TPageRange> ranges)
        -> TFuture<NProto::TError> final
    {
        for (auto& range: ranges) {
            if (auto error = Translate(range.FirstPageNo, range.Pages.size());
                HasError(error))
            {
                return MakeFuture(std::move(error));
            }
        }
        return Device->WritePages(std::move(ranges));
    }

    [[nodiscard]] auto ZeroPages(TVector<TPageRangeRef> rangeRefs)
        -> TFuture<NProto::TError> final
    {
        for (auto& ref: rangeRefs) {
            if (auto error = Translate(ref.FirstPageNo, ref.PageCount);
                HasError(error))
            {
                return MakeFuture(std::move(error));
            }
        }
        return Device->ZeroPages(std::move(rangeRefs));
    }

private:
    // Checks that the range fits the region and shifts it to the device.
    NProto::TError Translate(ui64& firstPageNo, ui64 pageCount) const
    {
        if (firstPageNo > Region.PageCount ||
            pageCount > Region.PageCount - firstPageNo)
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "pages [" << firstPageNo << ", "
                    << firstPageNo + pageCount << ") are outside the region "
                    << "of " << Region.PageCount << " pages");
        }

        firstPageNo += Region.FirstPageNo;
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileDeviceManager final: public IDeviceManager
{
private:
    const TString DeviceUUID;
    const IDevicePtr Device;

    std::mutex Lock;
    std::optional<TString> Owner;   // the client id
    ui32 Generation = 0;            // of the last acquire or release

public:
    TFileDeviceManager(TString deviceUUID, IDevicePtr device)
        : DeviceUUID(std::move(deviceUUID))
        , Device(std::move(device))
    {}

    // IDeviceManager

    [[nodiscard]] auto AcquireDevices(NProto::TAcquireDevicesRequest request)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        const auto& clientId = request.GetHeaders().GetClientId();
        const ui32 generation = request.GetGeneration();

        std::lock_guard lock(Lock);

        for (const auto& uuid: request.GetDeviceUUIDs()) {
            if (auto error = Lookup(uuid); HasError(error)) {
                return MakeFuture<NProto::TAcquireDevicesResponse>(
                    TErrorResponse(error));
            }
        }

        if (auto error = CheckGeneration(generation); HasError(error)) {
            return MakeFuture<NProto::TAcquireDevicesResponse>(
                TErrorResponse(error));
        }

        if (Owner && *Owner != clientId && generation <= Generation) {
            return MakeFuture<NProto::TAcquireDevicesResponse>(TErrorResponse(
                E_BS_INVALID_SESSION,
                TStringBuilder()
                    << "Device " << DeviceUUID.Quote()
                    << " is acquired by another client"));
        }

        Owner = clientId;
        Generation = Max(Generation, generation);

        return MakeFuture<NProto::TAcquireDevicesResponse>();
    }

    [[nodiscard]] auto ReleaseDevices(NProto::TReleaseDevicesRequest request)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        const auto& clientId = request.GetHeaders().GetClientId();
        const ui32 generation = request.GetGeneration();

        std::lock_guard lock(Lock);

        for (const auto& uuid: request.GetDeviceUUIDs()) {
            if (auto error = Lookup(uuid); HasError(error)) {
                return MakeFuture<NProto::TReleaseDevicesResponse>(
                    TErrorResponse(error));
            }
        }

        if (auto error = CheckGeneration(generation); HasError(error)) {
            return MakeFuture<NProto::TReleaseDevicesResponse>(
                TErrorResponse(error));
        }

        Generation = Max(Generation, generation);

        // a release by a client that does not own the device is a no-op
        if (Owner == clientId) {
            Owner.reset();
        }

        return MakeFuture<NProto::TReleaseDevicesResponse>();
    }

    [[nodiscard]] NProto::TError AccessDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode /*accessMode*/) final
    {
        std::lock_guard lock(Lock);

        if (auto error = Lookup(deviceUUID); HasError(error)) {
            return error;
        }

        if (Owner != clientId) {
            return MakeError(
                E_BS_INVALID_SESSION,
                TStringBuilder()
                    << "Device " << DeviceUUID.Quote()
                    << " is not acquired by client " << clientId.Quote());
        }

        return {};
    }

    [[nodiscard]] IDevicePtr CreateDevice(
        const TString& deviceUUID,
        TPageRangeRef region,
        ui32 /*blockSize*/) final
    {
        Y_ENSURE(
            deviceUUID == DeviceUUID,
            "unknown device " << deviceUUID.Quote());

        return std::make_shared<TRegionDevice>(Device, region);
    }

private:
    NProto::TError Lookup(const TString& deviceUUID) const
    {
        if (deviceUUID != DeviceUUID) {
            return MakeError(
                E_NOT_FOUND,
                TStringBuilder()
                    << "Device " << deviceUUID.Quote() << " not found");
        }

        return {};
    }

    // A zero generation is not checked.
    NProto::TError CheckGeneration(ui32 generation) const
    {
        if (generation && generation < Generation) {
            return MakeError(
                E_BS_INVALID_SESSION,
                TStringBuilder()
                    << "generation " << generation << " is below the last "
                    << "seen " << Generation);
        }

        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceManagerPtr CreateFileDeviceManager(
    IFileIOServicePtr fileIO,
    TString deviceUUID,
    const TString& filePath,
    ui64 blockCount,
    ui32 blockSize)
{
    return std::make_shared<TFileDeviceManager>(
        std::move(deviceUUID),
        CreateFileDevice(std::move(fileIO), filePath, blockCount, blockSize));
}

}   // namespace NCloud::NFastShard::NJdServer
