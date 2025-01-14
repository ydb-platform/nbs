#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <util/generic/cast.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleAcquireDisk(
    const TEvDiskRegistry::TEvAcquireDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AcquireDisk);

    const auto* msg = ev->Get();

    auto clientId = msg->Record.GetHeaders().GetClientId();
    auto diskId = msg->Record.GetDiskId();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received AcquireDisk request: "
        "DiskId=%s, ClientId=%s, AccessMode=%u, MountSeqNumber=%lu"
        ", VolumeGeneration=%u",
        TabletID(),
        diskId.c_str(),
        clientId.c_str(),
        static_cast<ui32>(msg->Record.GetAccessMode()),
        msg->Record.GetMountSeqNumber(),
        msg->Record.GetVolumeGeneration());

    TDiskInfo diskInfo;
    auto error = State->StartAcquireDisk(diskId, diskInfo);

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "[%s] AcquireDisk %s error: %s",
            clientId.c_str(),
            diskId.c_str(),
            FormatError(error).c_str());

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistry::TEvAcquireDiskResponse>(
                std::move(error)));
        return;
    }

    State->FilterDevicesAtUnavailableAgents(diskInfo);

    TVector devices = std::move(diskInfo.Devices);
    for (auto& migration: diskInfo.Migrations) {
        devices.push_back(std::move(*migration.MutableTargetDevice()));
    }
    for (auto& replica: diskInfo.Replicas) {
        devices.insert(
            devices.end(),
            std::make_move_iterator(replica.begin()),
            std::make_move_iterator(replica.end()));
    }

    auto actor = NAcquireReleaseDevices::AcquireDevices(
        ctx,
        ctx.SelfID,
        std::move(devices),
        std::move(diskId),
        std::move(clientId),
        msg->Record.GetAccessMode(),
        msg->Record.GetMountSeqNumber(),
        msg->Record.GetVolumeGeneration(),
        Config->GetAgentRequestTimeout(),
        /*muteIOErrors=*/false,
        TBlockStoreComponents::DISK_REGISTRY);
    Actors.insert(actor);
    AcquireDiskRequests[actor] =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
}

void TDiskRegistryActor::HandleDevicesAcquireFinished(
    const NAcquireReleaseDevices::TEvDevicesAcquireFinished::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    State->FinishAcquireDisk(msg->DiskId);

    OnDiskAcquired(std::move(msg->SentRequests));

    auto reqInfo = AcquireDiskRequests.at(ev->Sender);

    auto response = std::make_unique<TEvDiskRegistry::TEvAcquireDiskResponse>(
        std::move(msg->Error));

    if (HasError(response->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "[%s] AcquireDisk %s targets %s error: %s",
            msg->ClientId.c_str(),
            msg->DiskId.c_str(),
            LogDevices(msg->Devices).c_str(),
            FormatError(response->GetError()).c_str());
    } else {
        response->Record.MutableDevices()->Reserve(msg->Devices.size());

        for (auto& device: msg->Devices) {
            *response->Record.AddDevices() = std::move(device);
        }
    }

    NCloud::Reply(ctx, *reqInfo, std::move(response));
    Actors.erase(ev->Sender);
    AcquireDiskRequests.erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleReleaseDisk(
    const TEvDiskRegistry::TEvReleaseDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ReleaseDisk);

    auto replyWithError = [&](auto error)
    {
        auto response =
            std::make_unique<TEvDiskRegistry::TEvReleaseDiskResponse>(
                std::move(error));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    auto* msg = ev->Get();
    TString& diskId = *msg->Record.MutableDiskId();
    TString& clientId = *msg->Record.MutableHeaders()->MutableClientId();
    ui32 volumeGeneration = msg->Record.GetVolumeGeneration();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received ReleaseDisk request: DiskId=%s, ClientId=%s"
        ", VolumeGeneration=%u",
        TabletID(),
        diskId.c_str(),
        clientId.c_str(),
        volumeGeneration);

    if (!clientId) {
        replyWithError(MakeError(E_ARGUMENT, "empty client id"));
        return;
    }

    if (!diskId) {
        replyWithError(MakeError(E_ARGUMENT, "empty disk id"));
        return;
    }

    TDiskInfo diskInfo;
    const auto error = State->GetDiskInfo(diskId, diskInfo);
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "ReleaseDisk %s. GetDiskInfo error: %s",
            diskId.c_str(),
            FormatError(error).c_str());

        replyWithError(error);
        return;
    }

    if (!State->FilterDevicesAtUnavailableAgents(diskInfo)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "ReleaseDisk %s. Nothing to release",
            diskId.c_str());

        replyWithError(MakeError(S_ALREADY, {}));
        return;
    }

    TVector<NProto::TDeviceConfig> devices = std::move(diskInfo.Devices);
    for (auto& migration: diskInfo.Migrations) {
        devices.push_back(std::move(*migration.MutableTargetDevice()));
    }
    for (auto& replica: diskInfo.Replicas) {
        for (auto& device: replica) {
            devices.push_back(std::move(device));
        }
    }

    auto actor = NAcquireReleaseDevices::ReleaseDevices(
        ctx,
        ctx.SelfID,
        std::move(diskId),
        std::move(clientId),
        volumeGeneration,
        Config->GetAgentRequestTimeout(),
        std::move(devices),
        /*muteIOErrors=*/false,
        TBlockStoreComponents::DISK_REGISTRY);

    Actors.insert(actor);
    ReleaseDiskRequests[actor] =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
}

void TDiskRegistryActor::HandleDevicesReleaseFinished(
    const NAcquireReleaseDevices::TEvDevicesReleaseFinished::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    OnDiskReleased(msg->SentRequests);

    State->FinishAcquireDisk(msg->DiskId);
    auto reqInfo = ReleaseDiskRequests.at(ev->Sender);

    auto response =
        std::make_unique<TEvDiskRegistry::TEvReleaseDiskResponse>(msg->Error);
    NCloud::Reply(ctx, *reqInfo, std::move(response));

    Actors.erase(ev->Sender);
    ReleaseDiskRequests.erase(ev->Sender);
}

}   // namespace NCloud::NBlockStore::NStorage
