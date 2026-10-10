#include "journalled_device_manager.h"

#include "journalled_device_adapter.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/fastshard/journal/server/device_manager.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>

#include <util/string/builder.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NJournalled;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Fastshard orders writers by generation only, so the mount sequence number of
// the disk agent is never used.
constexpr ui64 DefaultMountSeqNumber = 0;

////////////////////////////////////////////////////////////////////////////////

void CopyHeaders(
    NProto::THeaders& dst,
    const NProto::TDeviceRequestHeaders& src)
{
    dst.SetClientId(src.GetClientId());
    dst.SetRequestTimeout(src.GetRequestTimeout());
}

std::optional<NProto::EVolumeAccessMode> GetVolumeAccessMode(
    NProto::EAccessMode accessMode)
{
    switch (accessMode) {
        case NProto::ACCESS_READ_WRITE:
            return NProto::VOLUME_ACCESS_READ_WRITE;
        case NProto::ACCESS_READ_ONLY:
            return NProto::VOLUME_ACCESS_READ_ONLY;

        default:
            return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDeviceManager final: public IDeviceManager
{
private:
    const ITimerPtr Timer;
    const TDeviceClientPtr DeviceClient;
    TActorSystem* ActorSystem = nullptr;
    const TActorId DiskAgentActorId;

public:
    TDeviceManager(
        ITimerPtr timer,
        TDeviceClientPtr deviceClient,
        TActorSystem* actorSystem,
        const TActorId& diskAgentActorId)
        : Timer(std::move(timer))
        , DeviceClient(std::move(deviceClient))
        , ActorSystem(actorSystem)
        , DiskAgentActorId(diskAgentActorId)
    {}

    [[nodiscard]] auto AcquireDevices(
        NCloud::NProto::TAcquireDevicesRequest request)
        -> TFuture<NCloud::NProto::TAcquireDevicesResponse> final
    {
        const auto accessMode = GetVolumeAccessMode(request.GetAccessMode());
        if (!accessMode) {
            return MakeFuture<NCloud::NProto::TAcquireDevicesResponse>(
                TErrorResponse(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "unknown access mode: "
                        << static_cast<int>(request.GetAccessMode())));
        }

        auto ev = std::make_unique<TEvDiskAgent::TEvAcquireDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());
        ev->Record.SetAccessMode(*accessMode);
        ev->Record.SetMountSeqNumber(DefaultMountSeqNumber);
        ev->Record.SetDiskId(request.GetFastshardId());
        ev->Record.SetVolumeGeneration(request.GetGeneration());

        auto future = ActorSystem->Ask<TEvDiskAgent::TEvAcquireDevicesResponse>(
            DiskAgentActorId,
            THolder(ev.release()));

        return future.Apply(
            [](const auto& future)
            {
                NCloud::NProto::TAcquireDevicesResponse response;
                const auto& ev = future.GetValue();
                response.MutableError()->CopyFrom(ev->Record.GetError());

                return response;
            });
    }

    [[nodiscard]] auto ReleaseDevices(
        NCloud::NProto::TReleaseDevicesRequest request)
        -> TFuture<NCloud::NProto::TReleaseDevicesResponse> final
    {
        auto ev = std::make_unique<TEvDiskAgent::TEvReleaseDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());
        ev->Record.SetDiskId(request.GetFastshardId());
        ev->Record.SetVolumeGeneration(request.GetGeneration());

        auto future = ActorSystem->Ask<TEvDiskAgent::TEvReleaseDevicesResponse>(
            DiskAgentActorId,
            THolder(ev.release()));

        return future.Apply(
            [](const auto& future)
            {
                NCloud::NProto::TReleaseDevicesResponse response;
                const auto& ev = future.GetValue();
                response.MutableError()->CopyFrom(ev->Record.GetError());

                return response;
            });
    }

    [[nodiscard]] NProto::TError AccessDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode) final
    {
        const auto volumeAccessMode = GetVolumeAccessMode(accessMode);
        if (!volumeAccessMode) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "unknown access mode: " << static_cast<int>(accessMode));
        }

        auto [_, error] =
            DeviceClient->AccessDevice(deviceUUID, clientId, *volumeAccessMode);
        return error;
    }

    [[nodiscard]] IDevicePtr CreateDevice(
        const TString& deviceUUID,
        TPageRangeRef region,
        ui32 blockSize) final
    {
        return CreateDeviceAdapter(
            Timer,
            DeviceClient,
            deviceUUID,
            region,
            blockSize);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceManagerPtr CreateDeviceManager(
    ITimerPtr timer,
    TDeviceClientPtr deviceClient,
    TActorSystem* actorSystem,
    const TActorId& diskAgentActorId)
{
    return std::make_shared<TDeviceManager>(
        std::move(timer),
        std::move(deviceClient),
        actorSystem,
        diskAgentActorId);
}

}   // namespace NCloud::NBlockStore::NStorage
