#include "device_manager.h"

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/libs/journal/server/device_manager.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <mutex>
#include <optional>

namespace NCloud::NFastShard::NJdServer {

using namespace NJournalled;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSingleDeviceManager final: public IDeviceManager
{
private:
    const IDevicePtr Device;
    const TString DeviceUUID;

    std::mutex Lock;
    std::optional<TString> Writer;   // the client id
    THashSet<TString> Readers;       // the client ids

    // Of the writer's last acquire or release
    ui32 Generation = 0;

public:
    TSingleDeviceManager(IDevicePtr device, TString deviceUUID)
        : Device(std::move(device))
        , DeviceUUID(std::move(deviceUUID))
    {}

    // IDeviceManager

    [[nodiscard]] auto AcquireDevices(NProto::TAcquireDevicesRequest request)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        const auto& clientId = request.GetHeaders().GetClientId();
        const ui32 generation = request.GetGeneration();
        const auto accessMode = request.GetAccessMode();

        std::lock_guard lock(Lock);

        if (auto error = Lookup(request.GetDeviceUUIDs()); HasError(error)) {
            return MakeFuture<NProto::TAcquireDevicesResponse>(
                TErrorResponse(error));
        }

        // The generation is the writer's: readers neither check nor raise it
        switch (accessMode) {
            case NProto::ACCESS_READ_ONLY:
                if (Writer == clientId) {
                    Writer.reset();
                }
                Readers.insert(clientId);
                break;

            case NProto::ACCESS_READ_WRITE:
                if (auto error = CheckGeneration(clientId, generation);
                    HasError(error))
                {
                    return MakeFuture<NProto::TAcquireDevicesResponse>(
                        TErrorResponse(error));
                }
                if (Writer && *Writer != clientId && generation <= Generation) {
                    return MakeFuture<NProto::TAcquireDevicesResponse>(
                        TErrorResponse(
                            E_BS_INVALID_SESSION,
                            TStringBuilder()
                                << "Device " << DeviceUUID.Quote()
                                << " is acquired for writing by client "
                                << Writer->Quote() << " at generation "
                                << Generation << ", client " << clientId.Quote()
                                << " at generation " << generation
                                << " may not take it over"));
                }
                Readers.erase(clientId);
                Writer = clientId;
                Generation = Max(Generation, generation);
                break;

            default:
                return MakeFuture<NProto::TAcquireDevicesResponse>(
                    TErrorResponse(
                        E_ARGUMENT,
                        TStringBuilder() << "unknown access mode "
                                         << static_cast<int>(accessMode)));
        }

        return MakeFuture<NProto::TAcquireDevicesResponse>();
    }

    [[nodiscard]] auto ReleaseDevices(NProto::TReleaseDevicesRequest request)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        const auto& clientId = request.GetHeaders().GetClientId();
        const ui32 generation = request.GetGeneration();

        std::lock_guard lock(Lock);

        if (auto error = Lookup(request.GetDeviceUUIDs()); HasError(error)) {
            return MakeFuture<NProto::TReleaseDevicesResponse>(
                TErrorResponse(error));
        }

        // Only the writer's release is subject to the generation; a release
        // by a reader or by a client that has not acquired the device at all
        // leaves it alone
        if (Writer == clientId) {
            if (auto error = CheckGeneration(clientId, generation);
                HasError(error))
            {
                return MakeFuture<NProto::TReleaseDevicesResponse>(
                    TErrorResponse(error));
            }
            Writer.reset();
            Generation = Max(Generation, generation);
        }
        Readers.erase(clientId);

        return MakeFuture<NProto::TReleaseDevicesResponse>();
    }

    [[nodiscard]] NProto::TError AccessDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode) final
    {
        std::lock_guard lock(Lock);

        if (auto error = Lookup(deviceUUID); HasError(error)) {
            return error;
        }

        if (Writer == clientId) {
            return {};
        }

        if (accessMode != NProto::ACCESS_READ_ONLY ||
            !Readers.contains(clientId))
        {
            return MakeError(
                E_BS_INVALID_SESSION,
                TStringBuilder()
                    << "Device " << DeviceUUID.Quote()
                    << " is not acquired by client " << clientId.Quote()
                    << " for "
                    << (accessMode == NProto::ACCESS_READ_ONLY ? "reading"
                                                               : "writing")
                    << "; the writer is "
                    << (Writer ? Writer->Quote() : TString("none"))
                    << ", the readers are " << JoinSeq(", ", Readers));
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

        return CreateRegionDevice(Device, region);
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

    // Checks that the request names the configured device and nothing else.
    template <typename TDeviceUUIDs>
    NProto::TError Lookup(const TDeviceUUIDs& deviceUUIDs) const
    {
        if (deviceUUIDs.empty()) {
            return MakeError(E_ARGUMENT, "the request names no devices");
        }

        for (const auto& uuid: deviceUUIDs) {
            if (auto error = Lookup(uuid); HasError(error)) {
                return error;
            }
        }

        return {};
    }

    // A zero generation is not checked.
    NProto::TError CheckGeneration(
        const TString& clientId,
        ui32 generation) const
    {
        if (generation && generation < Generation) {
            return MakeError(
                E_BS_INVALID_SESSION,
                TStringBuilder()
                    << "Device " << DeviceUUID.Quote()
                    << " was last acquired or released for writing at "
                    << "generation " << Generation << " by client "
                    << (Writer ? Writer->Quote() : TString("none"))
                    << ", client " << clientId.Quote() << " at generation "
                    << generation << " is stale");
        }

        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceManagerPtr CreateSingleDeviceManager(
    IDevicePtr device,
    TString deviceUUID)
{
    return std::make_shared<TSingleDeviceManager>(
        std::move(device),
        std::move(deviceUUID));
}

}   // namespace NCloud::NFastShard::NJdServer
