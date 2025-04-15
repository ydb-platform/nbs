#include "volume_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/volume/actors/release_devices_actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendReleaseDevicesToAgents(
    const TString& clientId,
    const TActorContext& ctx,
    TVector<NProto::TDeviceConfig> devicesToRelease)
{
    auto replyWithError = [&](auto error)
    {
        NCloud::Send(
            ctx,
            SelfId(),
            std::make_unique<TEvVolumePrivate::TEvDevicesReleaseFinished>(
                std::move(error)));
    };

    TString diskId = State->GetDiskId();
    ui32 volumeGeneration = Executor()->Generation();

    if (!clientId) {
        replyWithError(MakeError(E_ARGUMENT, "empty client id"));
        return;
    }

    if (!diskId) {
        replyWithError(MakeError(E_ARGUMENT, "empty disk id"));
        return;
    }

    if (devicesToRelease.empty()) {
        devicesToRelease = State->GetAllDevicesForRelease();
    }

    auto actor = NCloud::Register<TReleaseDevicesActor>(
        ctx,
        ctx.SelfID,
        std::move(diskId),
        clientId,
        volumeGeneration,
        Config->GetAgentRequestTimeout(),
        std::move(devicesToRelease),
        State->GetMeta().GetMuteIOErrors());

    Actors.insert(actor);
}

void TVolumeActor::HandleDevicesReleasedFinished(
    const TEvVolumePrivate::TEvDevicesReleaseFinished::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    HandleDevicesReleasedFinishedImpl(ev->Get()->GetError(), ctx);
}

void TVolumeActor::ReleaseReplacedDevices(
    const NActors::TActorContext& ctx,
    const TVector<NProto::TDeviceConfig>& replacedDevices)
{
    if (replacedDevices.empty()) {
        return;
    }

    for (const auto& [clientId, _]: State->GetClients()) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%lu] Releasing devices that don't belong to the volume anymore "
            "for client: %s",
            TabletID(),
            clientId.c_str());

        TAcquireReleaseDiskRequest request{clientId, nullptr, replacedDevices};
        AcquireReleaseDiskRequests.push_back(std::move(request));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
