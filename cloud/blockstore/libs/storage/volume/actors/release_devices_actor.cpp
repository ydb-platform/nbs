#include "release_devices_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TReleaseDevicesActor::TReleaseDevicesActor(
        const TActorId& owner,
        TString diskId,
        TString clientId,
        ui32 volumeGeneration,
        TDuration requestTimeout,
        TVector<NProto::TDeviceConfig> devices,
        bool muteIOErrors)
    : Owner(owner)
    , DiskId(std::move(diskId))
    , ClientId(std::move(clientId))
    , VolumeGeneration(volumeGeneration)
    , RequestTimeout(requestTimeout)
    , MuteIOErrors(muteIOErrors)
    , Devices(std::move(devices))
{}

void TReleaseDevicesActor::PrepareRequest(
    NProto::TReleaseDevicesRequest& request)
{
    request.MutableHeaders()->SetClientId(ClientId);
    request.SetDiskId(DiskId);
    request.SetVolumeGeneration(VolumeGeneration);
}

void TReleaseDevicesActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (Devices.empty()) {
        ReplyAndDie(ctx, {});
        return;
    }

    SortBy(Devices, [](auto& d) { return d.GetNodeId(); });

    auto it = Devices.begin();
    while (it != Devices.end()) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvReleaseDevicesRequest>();
        PrepareRequest(request->Record);

        const ui32 nodeId = it->GetNodeId();
        const TString& agentId = it->GetAgentId();

        for (; it != Devices.end() && it->GetNodeId() == nodeId; ++it) {
            *request->Record.AddDeviceUUIDs() = it->GetDeviceUUID();
        }

        auto [_, inserted] = PendingAgents.emplace(nodeId, agentId);
        Y_ABORT_UNLESS(inserted);

        NCloud::Send(
            ctx,
            MakeDiskAgentServiceId(nodeId),
            std::move(request),
            nodeId);
    }

    ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());
}

void TReleaseDevicesActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvVolumePrivate::TEvDevicesReleaseFinished>(
            std::move(error)));

    Die(ctx);
}

void TReleaseDevicesActor::OnReleaseResponse(
    const TActorContext& ctx,
    TNodeId nodeId,
    NProto::TError error)
{
    Y_ABORT_UNLESS(!PendingAgents.empty());

    if (HasError(error)) {
        LOG_LOG(
            ctx,
            MuteIOErrors ? NLog::PRI_WARN : NLog::PRI_ERROR,
            TBlockStoreComponents::VOLUME,
            "ReleaseDevices DiskId: %s, NodeId: %d, AgentId: %s, "
            "PendingAgents: %s, error: %s",
            DiskId.Quote().c_str(),
            nodeId,
            PendingAgents.at(nodeId).c_str(),
            LogPendingAgents().c_str(),
            FormatError(error).c_str());
    }

    auto eraseCount = PendingAgents.erase(nodeId);
    Y_ABORT_UNLESS(eraseCount == 1);
    if (PendingAgents.empty()) {
        ReplyAndDie(ctx, {});
    }
}

TString TReleaseDevicesActor::LogTargets() const
{
    return LogDevices(Devices);
}

TString TReleaseDevicesActor::LogPendingAgents() const
{
    TStringBuilder sb;
    sb << "(";

    for (const auto& [nodeId, agent]: PendingAgents) {
        sb << "(NodeId: " << nodeId << ", AgentId: " << agent << ") ";
    }
    sb << ")";

    return sb;
}

void TReleaseDevicesActor::HandleReleaseDevicesResponse(
    const TEvDiskAgent::TEvReleaseDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    OnReleaseResponse(ctx, ev->Cookie, ev->Get()->GetError());
}

void TReleaseDevicesActor::HandleReleaseDevicesUndelivery(
    const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    OnReleaseResponse(ctx, ev->Cookie, MakeError(E_REJECTED, "not delivered"));
}

void TReleaseDevicesActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "Tablet is dead"));
}

void TReleaseDevicesActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto err = TStringBuilder()
                     << "TReleaseDevicesActor timeout."
                     << " DiskId: " << DiskId.Quote()
                     << " ClientId: " << ClientId
                     << " PendingAgents: " << LogPendingAgents()
                     << " VolumeGeneration: " << VolumeGeneration
                     << " MuteIoErrors: " << MuteIOErrors;

    LOG_WARN(ctx, TBlockStoreComponents::VOLUME, err);

    ReplyAndDie(ctx, MakeError(E_TIMEOUT, err));
}

STFUNC(TReleaseDevicesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(
            TEvDiskAgent::TEvReleaseDevicesResponse,
            HandleReleaseDevicesResponse);
        HFunc(
            TEvDiskAgent::TEvReleaseDevicesRequest,
            HandleReleaseDevicesUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
