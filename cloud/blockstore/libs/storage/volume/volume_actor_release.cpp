#include "volume_actor.h"
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReleaseDevicesActor final
    : public TActorBootstrapped<TReleaseDevicesActor>
{
private:
    const TActorId Owner;
    const TString DiskId;
    const TString ClientId;
    const ui32 VolumeGeneration;
    const TDuration RequestTimeout;
    const bool MuteIOErrors;

    TVector<NProto::TDeviceConfig> Devices;
    int PendingRequests = 0;

public:
    TReleaseDevicesActor(
        const TActorId& owner,
        TString diskId,
        TString clientId,
        ui32 volumeGeneration,
        TDuration requestTimeout,
        TVector<NProto::TDeviceConfig> devices,
        bool muteIOErrors);

    void Bootstrap(const TActorContext& ctx);

private:
    void PrepareRequest(NProto::TReleaseDevicesRequest& request);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void OnReleaseResponse(
        const TActorContext& ctx,
        ui64 cookie,
        NProto::TError error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleReleaseDevicesResponse(
        const TEvDiskAgent::TEvReleaseDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReleaseDevicesUndelivery(
        const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    TString LogTargets() const;
};

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

void TReleaseDevicesActor::PrepareRequest(NProto::TReleaseDevicesRequest& request)
{
    request.MutableHeaders()->SetClientId(ClientId);
    request.SetDiskId(DiskId);
    request.SetVolumeGeneration(VolumeGeneration);
}

void TReleaseDevicesActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    SortBy(Devices, [](auto& d) { return d.GetNodeId(); });

    auto it = Devices.begin();
    while (it != Devices.end()) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvReleaseDevicesRequest>();
        PrepareRequest(request->Record);

        const ui32 nodeId = it->GetNodeId();

        for (; it != Devices.end() && it->GetNodeId() == nodeId; ++it) {
            *request->Record.AddDeviceUUIDs() = it->GetDeviceUUID();
        }

        ++PendingRequests;
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
    ui64 cookie,
    NProto::TError error)
{
    Y_ABORT_UNLESS(PendingRequests > 0);

    if (HasError(error)) {
        LOG_LOG(
            ctx,
            MuteIOErrors ? NLog::PRI_WARN : NLog::PRI_ERROR,
            TBlockStoreComponents::VOLUME,
            "ReleaseDevices %s error: %s, %llu",
            LogTargets().c_str(),
            FormatError(error).c_str(),
            cookie);
    }

    if (--PendingRequests == 0) {
        ReplyAndDie(ctx, {});
    }
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
                     << "TReleaseDiskActor timeout." << " DiskId: " << DiskId
                     << " ClientId: " << ClientId
                     << " Targets: " << LogTargets()
                     << " VolumeGeneration: " << VolumeGeneration
                     << " PendingRequests: " << PendingRequests;

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
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

TString TReleaseDevicesActor::LogTargets() const
{
    return LogDevices(Devices);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendReleaseDevicesToAgents(
    const TString& clientId,
    const TActorContext& ctx)
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

    auto devices = State->GetAllDevicesForAcquireRelease();

    auto actor = NCloud::Register<TReleaseDevicesActor>(
        ctx,
        ctx.SelfID,
        std::move(diskId),
        clientId,
        volumeGeneration,
        Config->GetAgentRequestTimeout(),
        std::move(devices),
        State->GetMeta().GetMuteIOErrors());

    Actors.insert(actor);
}

void TVolumeActor::HandleDevicesReleasedFinished(
    const TEvVolumePrivate::TEvDevicesReleaseFinished::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    HandleDevicesReleasedFinishedImpl(ev->Get()->GetError(), ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
