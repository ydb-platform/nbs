#include "volume_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <util/generic/cast.h>
#include <util/string/join.h>

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

struct TAgentAcquireDevicesCachedRequest
{
    TString AgentId;
    NProto::TAcquireDevicesRequest Request;
    TInstant RequestTime;
};

////////////////////////////////////////////////////////////////////////////////

class TAcquireDiskActor final: public TActorBootstrapped<TAcquireDiskActor>
{
private:
    const TActorId Owner;
    TVector<NProto::TDeviceConfig> Devices;
    const TString DiskId;
    const TString ClientId;
    const NProto::EVolumeAccessMode AccessMode;
    const ui64 MountSeqNumber;
    const ui32 VolumeGeneration;
    const TDuration RequestTimeout;
    const bool MuteIOErrors;

    int PendingRequests = 0;

public:
    TAcquireDiskActor(
        const TActorId& owner,
        TVector<NProto::TDeviceConfig> devices,
        TString diskId,
        TString clientId,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        ui32 volumeGeneration,
        TDuration requestTimeout,
        bool muteIOErrors);

    void Bootstrap(const TActorContext& ctx);

private:
    void PrepareRequest(NProto::TAcquireDevicesRequest& request) const;
    void PrepareRequest(NProto::TReleaseDevicesRequest& request) const;

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void OnAcquireResponse(
        const TActorContext& ctx,
        ui32 nodeId,
        NProto::TError error);

    template <typename TRequest>
    struct TSentRequest
    {
        TString AgentId;
        ui32 NodeId = 0;
        decltype(TRequest::Record) Record;
    };

    template <typename TRequest>
    TVector<TSentRequest<TRequest>> CreateRequests() const;

    template <typename TRequest>
    void SendRequests(
        const TActorContext& ctx,
        const TVector<TSentRequest<TRequest>>& requests);

private:
    STFUNC(StateAcquire);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleAcquireDevicesResponse(
        const TEvDiskAgent::TEvAcquireDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAcquireDevicesUndelivery(
        const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    TString LogTargets() const;
};

////////////////////////////////////////////////////////////////////////////////

TAcquireDiskActor::TAcquireDiskActor(
        const TActorId& owner,
        TVector<NProto::TDeviceConfig> devices,
        TString diskId,
        TString clientId,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        ui32 volumeGeneration,
        TDuration requestTimeout,
        bool muteIOErrors)
    : Owner(owner)
    , Devices(std::move(devices))
    , DiskId(std::move(diskId))
    , ClientId(std::move(clientId))
    , AccessMode(accessMode)
    , MountSeqNumber(mountSeqNumber)
    , VolumeGeneration(volumeGeneration)
    , RequestTimeout(requestTimeout)
    , MuteIOErrors(muteIOErrors)
{
    SortBy(Devices, [](auto& d) { return d.GetNodeId(); });
}

void TAcquireDiskActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateAcquire);

    if (Devices.empty()) {
        ReplyAndDie(ctx, {});
        return;
    }

    ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%s] Sending acquire devices requests for disk %s, targets %s",
        ClientId.c_str(),
        DiskId.c_str(),
        LogTargets().c_str());

    auto sentRequests =
        CreateRequests<TEvDiskAgent::TEvAcquireDevicesRequest>();
    SendRequests(ctx, sentRequests);
}

void TAcquireDiskActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    using TType = TEvVolumePrivate::TEvFinishAcquireDiskRequest;

    auto responseRecord = NProto::TAcquireDiskResponse();
    *responseRecord.MutableError() = std::move(error);

    if (HasError(responseRecord.GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%s] AcquireDisk %s targets %s error: %s",
            ClientId.c_str(),
            DiskId.c_str(),
            LogTargets().c_str(),
            FormatError(responseRecord.GetError()).c_str());
    } else {
        responseRecord.MutableDevices()->Reserve(Devices.size());

        for (auto& device: Devices) {
            *responseRecord.AddDevices() = std::move(device);
        }
    }

    NCloud::Send(ctx, Owner, std::make_unique<TType>(responseRecord));

    Die(ctx);
}

void TAcquireDiskActor::PrepareRequest(
    NProto::TAcquireDevicesRequest& request) const
{
    request.MutableHeaders()->SetClientId(ClientId);
    request.SetAccessMode(AccessMode);
    request.SetMountSeqNumber(MountSeqNumber);
    request.SetDiskId(DiskId);
    request.SetVolumeGeneration(VolumeGeneration);
}

void TAcquireDiskActor::PrepareRequest(
    NProto::TReleaseDevicesRequest& request) const
{
    request.MutableHeaders()->SetClientId(ClientId);
}

template <typename TRequest>
auto TAcquireDiskActor::CreateRequests() const
    -> TVector<TSentRequest<TRequest>>
{
    auto it = Devices.begin();
    TVector<TSentRequest<TRequest>> requests;
    while (it != Devices.end()) {
        const ui32 nodeId = it->GetNodeId();

        auto& request = requests.emplace_back();
        request.AgentId = it->GetAgentId();
        request.NodeId = nodeId;
        PrepareRequest(request.Record);

        for (; it != Devices.end() && it->GetNodeId() == nodeId; ++it) {
            *request.Record.AddDeviceUUIDs() = it->GetDeviceUUID();
        }
    }
    return requests;
}

template <typename TRequest>
void TAcquireDiskActor::SendRequests(
    const TActorContext& ctx,
    const TVector<TSentRequest<TRequest>>& requests)
{
    PendingRequests = 0;

    for (const auto& r: requests) {
        auto request = std::make_unique<TRequest>(TCallContextPtr{}, r.Record);

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%s] Send an acquire request to node #%d. Devices: %s",
            ClientId.c_str(),
            r.NodeId,
            JoinSeq(", ", request->Record.GetDeviceUUIDs()).c_str());

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(r.NodeId),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            r.NodeId,
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(event.release());

        ++PendingRequests;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAcquireDiskActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "Tablet is dead"));
}

void TAcquireDiskActor::OnAcquireResponse(
    const TActorContext& ctx,
    ui32 nodeId,
    NProto::TError error)
{
    Y_ABORT_UNLESS(PendingRequests > 0);

    if (HasError(error) && !MuteIOErrors) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%s] AcquireDevices on the node #%d %s error: %s",
            ClientId.c_str(),
            nodeId,
            LogTargets().c_str(),
            FormatError(error).c_str());

        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::VOLUME,
                "[%s] Canceling acquire operation for disk %s, targets %s",
                ClientId.c_str(),
                DiskId.c_str(),
                LogTargets().c_str());

            SendRequests(
                ctx,
                CreateRequests<TEvDiskAgent::TEvReleaseDevicesRequest>());
        }

        ReplyAndDie(ctx, std::move(error));

        return;
    }

    if (--PendingRequests == 0) {
        ReplyAndDie(ctx, {});
    }
}

void TAcquireDiskActor::HandleAcquireDevicesResponse(
    const TEvDiskAgent::TEvAcquireDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    OnAcquireResponse(
        ctx,
        SafeIntegerCast<ui32>(ev->Cookie),
        ev->Get()->GetError());
}

void TAcquireDiskActor::HandleAcquireDevicesUndelivery(
    const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    OnAcquireResponse(
        ctx,
        SafeIntegerCast<ui32>(ev->Cookie),
        MakeError(E_REJECTED, "not delivered"));
}

void TAcquireDiskActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    OnAcquireResponse(
        ctx,
        SafeIntegerCast<ui32>(ev->Cookie),
        MakeError(E_REJECTED, "timeout"));
}

////////////////////////////////////////////////////////////////////////////////

TString TAcquireDiskActor::LogTargets() const
{
    return LogDevices(Devices);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TAcquireDiskActor::StateAcquire)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskAgent::TEvAcquireDevicesResponse,
            HandleAcquireDevicesResponse);
        HFunc(
            TEvDiskAgent::TEvAcquireDevicesRequest,
            HandleAcquireDevicesUndelivery);

        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendProxylessAcquireDisk(
    std::unique_ptr<TEvDiskRegistry::TEvAcquireDiskRequest> ev,
    const TActorContext& ctx)
{
    const auto* msg = ev.get();

    auto clientId = msg->Record.GetHeaders().GetClientId();
    auto diskId = msg->Record.GetDiskId();

    TVector<NProto::TDeviceConfig> devices;
    auto error = State->StartAcquireDisk(devices);

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%s] AcquireDisk %s error: %s",
            clientId.c_str(),
            diskId.c_str(),
            FormatError(error).c_str());

        NProto::TAcquireDiskResponse response;
        *response.MutableError() = std::move(error);

        auto reply =
            std::make_unique<TEvVolumePrivate::TEvFinishAcquireDiskRequest>(
                std::move(response));

        NCloud::Send(ctx, SelfId(), std::move(reply));
        return;
    }

    for (const auto& migration: State->GetMeta().GetMigrations()) {
        devices.push_back(migration.GetTargetDevice());
    }

    auto actor = NCloud::Register<TAcquireDiskActor>(
        ctx,
        ctx.SelfID,
        std::move(devices),
        std::move(diskId),
        std::move(clientId),
        msg->Record.GetAccessMode(),
        msg->Record.GetMountSeqNumber(),
        msg->Record.GetVolumeGeneration(),
        Config->GetAgentRequestTimeout(),
        State->GetMeta().GetMuteIOErrors());
    Actors.insert(actor);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleFinishAcquireDisk(
    const TEvVolumePrivate::TEvFinishAcquireDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    State->FinishAcquireDisk();

    auto acquireDiskResponceRecord = ev->Get()->ResponceRecord;
    HandleAcquireDiskResponseImpl(acquireDiskResponceRecord, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
