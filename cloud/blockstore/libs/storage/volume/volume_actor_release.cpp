#include "volume_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

namespace {

TString LogDevices(const TVector<NProto::TDeviceConfig>& devices)
{
    TStringBuilder sb;
    sb << "( ";
    for (const auto& d: devices) {
        sb << d.GetDeviceUUID() << "@" << d.GetAgentId() << " ";
    }
    sb << ")";
    return sb;
}

////////////////////////////////////////////////////////////////////////////////

class TReleaseDiskActor final: public TActorBootstrapped<TReleaseDiskActor>
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
    TReleaseDiskActor(
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

TReleaseDiskActor::TReleaseDiskActor(
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

void TReleaseDiskActor::PrepareRequest(NProto::TReleaseDevicesRequest& request)
{
    request.MutableHeaders()->SetClientId(ClientId);
    request.SetDiskId(DiskId);
    request.SetVolumeGeneration(VolumeGeneration);
}

void TReleaseDiskActor::Bootstrap(const TActorContext& ctx)
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

void TReleaseDiskActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto responseRecord = NProto::TReleaseDiskResponse();
    *responseRecord.MutableError() = std::move(error);

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvVolumePrivate::TEvRemoveDiskSessionRequest>(
            std::move(responseRecord)));

    Die(ctx);
}

void TReleaseDiskActor::OnReleaseResponse(
    const TActorContext& ctx,
    ui64 cookie,
    NProto::TError error)
{
    Y_ABORT_UNLESS(PendingRequests > 0);

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
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

void TReleaseDiskActor::HandleReleaseDevicesResponse(
    const TEvDiskAgent::TEvReleaseDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    OnReleaseResponse(ctx, ev->Cookie, ev->Get()->GetError());
}

void TReleaseDiskActor::HandleReleaseDevicesUndelivery(
    const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (MuteIOErrors) {
        --PendingRequests;
        if (PendingRequests == 0) {
            ReplyAndDie(ctx, {});
        }
        return;
    }
    OnReleaseResponse(ctx, ev->Cookie, MakeError(E_REJECTED, "not delivered"));
}

void TReleaseDiskActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "Tablet is dead"));
}

void TReleaseDiskActor::HandleTimeout(
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

STFUNC(TReleaseDiskActor::StateWork)
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

TString TReleaseDiskActor::LogTargets() const
{
    return LogDevices(Devices);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendProxylessReleaseDisk(
    std::unique_ptr<TEvDiskRegistry::TEvReleaseDiskRequest> msg,
    const TActorContext& ctx)
{
    auto replyWithError = [&](auto error)
    {
        auto responseRecord = NProto::TReleaseDiskResponse();
        *responseRecord.MutableError() = std::move(error);
        NCloud::Send(
            ctx,
            SelfId(),
            std::make_unique<TEvVolumePrivate::TEvRemoveDiskSessionRequest>(
                std::move(responseRecord)));
    };

    TString& diskId = *msg->Record.MutableDiskId();
    TString& clientId = *msg->Record.MutableHeaders()->MutableClientId();
    ui32 volumeGeneration = msg->Record.GetVolumeGeneration();

    if (!clientId) {
        replyWithError(MakeError(E_ARGUMENT, "empty client id"));
        return;
    }

    if (!diskId) {
        replyWithError(MakeError(E_ARGUMENT, "empty disk id"));
        return;
    }

    const auto& devicesFromMeta = State->GetMeta().GetDevices();

    TVector<NProto::TDeviceConfig> devices(
        devicesFromMeta.begin(),
        devicesFromMeta.end());

    for (const auto& migration: State->GetMeta().GetMigrations()) {
        devices.emplace_back(migration.GetTargetDevice());
    }
    for (const auto& replica: State->GetMeta().GetReplicas()) {
        for (const auto& device: replica.GetDevices()) {
            devices.push_back(device);
        }
    }

    auto actor = NCloud::Register<TReleaseDiskActor>(
        ctx,
        ctx.SelfID,
        std::move(diskId),
        std::move(clientId),
        volumeGeneration,
        Config->GetAgentRequestTimeout(),
        std::move(devices),
        State->GetMeta().GetMuteIOErrors());

    Actors.insert(actor);
}

void TVolumeActor::HandleRemoveDiskSession(
    const TEvVolumePrivate::TEvRemoveDiskSessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    State->FinishAcquireDisk();

    auto record = ev->Get()->ResponceRecord;

    HandleReleaseDiskResponseImpl(record, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
