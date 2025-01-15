#include "volume_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <util/generic/cast.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendAcquireDevicesToAgents(
    TString clientId,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    const TActorContext& ctx)
{
    auto devices = State->GetAllDevicesForAcquireRelease();
    auto actor = NAcquireReleaseDevices::AcquireDevices(
        ctx,
        ctx.SelfID,
        std::move(devices),
        State->GetDiskId(),
        std::move(clientId),
        accessMode,
        mountSeqNumber,
        Executor()->Generation(),
        Config->GetAgentRequestTimeout(),
        State->GetMeta().GetMuteIOErrors(),
        TBlockStoreComponents::VOLUME);
    Actors.insert(actor);
}

void TVolumeActor::HandleDevicesAcquireFinished(
    const NAcquireReleaseDevices::TEvDevicesAcquireFinished::TPtr& ev,
    const TActorContext& ctx)
{
    HandleDevicesAcquireFinishedImpl(ev->Get()->Error, ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendReleaseDevicesToAgents(
    const TString& clientId,
    const TActorContext& ctx)
{
    auto returnError = [&](auto error)
    {
        auto event =
            std::make_unique<NAcquireReleaseDevices::TEvDevicesReleaseFinished>(
                NAcquireReleaseDevices::TDevicesReleaseFinished{
                    {},
                    {},
                    {},
                    std::move(error),
                });
        NCloud::Send(ctx, SelfId(), std::move(event));
    };

    TString diskId = State->GetDiskId();
    ui32 volumeGeneration = Executor()->Generation();

    if (!clientId) {
        returnError(MakeError(E_ARGUMENT, "empty client id"));
        return;
    }

    if (!diskId) {
        returnError(MakeError(E_ARGUMENT, "empty disk id"));
        return;
    }

    auto devices = State->GetAllDevicesForAcquireRelease();

    auto actor = NAcquireReleaseDevices::ReleaseDevices(
        ctx,
        ctx.SelfID,
        std::move(diskId),
        clientId,
        volumeGeneration,
        Config->GetAgentRequestTimeout(),
        std::move(devices),
        State->GetMeta().GetMuteIOErrors(),
        TBlockStoreComponents::VOLUME);

    Actors.insert(actor);
}

void TVolumeActor::HandleDevicesReleasedFinished(
    const NAcquireReleaseDevices::TEvDevicesReleaseFinished::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    HandleDevicesReleasedFinishedImpl(ev->Get()->Error, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
