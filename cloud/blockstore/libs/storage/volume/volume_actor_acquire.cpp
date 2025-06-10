#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <util/generic/cast.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAcquireDevicesActor final
    : public TActorBootstrapped<TAcquireDevicesActor>
{
    using TAgentId = TString;

    using TNodeId = ui32;

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

    THashMap<TNodeId, TAgentId> PendingAgents;

public:
    TAcquireDevicesActor(
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
        TNodeId nodeId,
        NProto::TError error);

    template <typename TRequest>
    struct TSentRequest
    {
        TAgentId AgentId;
        TNodeId NodeId = 0;
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

    TString LogPendingAgents() const;
};

////////////////////////////////////////////////////////////////////////////////

TAcquireDevicesActor::TAcquireDevicesActor(
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

void TAcquireDevicesActor::Bootstrap(const TActorContext& ctx)
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
        DiskId.Quote().c_str(),
        LogTargets().c_str());

    auto acquireRequests =
        CreateRequests<TEvDiskAgent::TEvAcquireDevicesRequest>();
    PendingAgents.reserve(acquireRequests.size());
    for (const auto& r: acquireRequests) {
        PendingAgents[r.NodeId] = r.AgentId;
    }

    SendRequests(ctx, acquireRequests);
}

void TAcquireDevicesActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    using TType = TEvVolumePrivate::TEvDevicesAcquireFinished;

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%s] AcquireDevices DiskId: %s PendingAgents %s error: %s",
            ClientId.c_str(),
            DiskId.c_str(),
            LogPendingAgents().c_str(),
            FormatError(error).c_str());
    }

    NCloud::Send(ctx, Owner, std::make_unique<TType>(std::move(error)));

    Die(ctx);
}

void TAcquireDevicesActor::PrepareRequest(
    NProto::TAcquireDevicesRequest& request) const
{
    request.MutableHeaders()->SetClientId(ClientId);
    request.SetAccessMode(AccessMode);
    request.SetMountSeqNumber(MountSeqNumber);
    request.SetDiskId(DiskId);
    request.SetVolumeGeneration(VolumeGeneration);
}

void TAcquireDevicesActor::PrepareRequest(
    NProto::TReleaseDevicesRequest& request) const
{
    request.MutableHeaders()->SetClientId(ClientId);
}

template <typename TRequest>
auto TAcquireDevicesActor::CreateRequests() const
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
void TAcquireDevicesActor::SendRequests(
    const TActorContext& ctx,
    const TVector<TSentRequest<TRequest>>& requests)
{
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
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAcquireDevicesActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "Tablet is dead"));
}

void TAcquireDevicesActor::OnAcquireResponse(
    const TActorContext& ctx,
    TNodeId nodeId,
    NProto::TError error)
{
    Y_ABORT_UNLESS(!PendingAgents.empty());

    if (HasError(error) && !MuteIOErrors) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%s] AcquireDevices for disk %s on the node #%d and agent %s "
            "error: %s, PendingAgents: %s",
            ClientId.c_str(),
            DiskId.Quote().c_str(),
            nodeId,
            PendingAgents.at(nodeId).c_str(),
            FormatError(error).c_str(),
            LogPendingAgents().c_str());

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

    auto eraseCount = PendingAgents.erase(nodeId);
    Y_ABORT_UNLESS(eraseCount == 1);
    if (PendingAgents.empty()) {
        ReplyAndDie(ctx, {});
    }
}

void TAcquireDevicesActor::HandleAcquireDevicesResponse(
    const TEvDiskAgent::TEvAcquireDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    OnAcquireResponse(
        ctx,
        SafeIntegerCast<ui32>(ev->Cookie),
        ev->Get()->GetError());
}

void TAcquireDevicesActor::HandleAcquireDevicesUndelivery(
    const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    OnAcquireResponse(
        ctx,
        SafeIntegerCast<ui32>(ev->Cookie),
        MakeError(E_REJECTED, "not delivered"));
}

void TAcquireDevicesActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto err = TStringBuilder()
                     << "TAcquireDevicesActor timeout." << " DiskId: " << DiskId
                     << " ClientId: " << ClientId
                     << " Pending agents: " << LogPendingAgents()
                     << " VolumeGeneration: " << VolumeGeneration
                     << " MuteIoErrors: " << MuteIOErrors;

    LOG_ERROR(ctx, TBlockStoreComponents::VOLUME, err);

    NProto::TError errorToReply;
    if (!MuteIOErrors) {
        errorToReply = MakeError(E_TIMEOUT, err);
    }

    ReplyAndDie(ctx, errorToReply);
}

////////////////////////////////////////////////////////////////////////////////

TString TAcquireDevicesActor::LogTargets() const
{
    return LogDevices(Devices);
}

TString TAcquireDevicesActor::LogPendingAgents() const
{
    TStringBuilder sb;
    sb << "(";

    for (const auto& [nodeId, agent]: PendingAgents) {
        sb << "(NodeId: " << nodeId << ", AgentId: " << agent << ") ";
    }
    sb << ")";

    return sb;
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TAcquireDevicesActor::StateAcquire)
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
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::SendAcquireDevicesToAgents(
    TString clientId,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    const TActorContext& ctx)
{
    auto devices = State->GetDevicesForAcquireOrRelease();

    auto actor = NCloud::Register<TAcquireDevicesActor>(
        ctx,
        ctx.SelfID,
        std::move(devices),
        State->GetDiskId(),
        std::move(clientId),
        accessMode,
        mountSeqNumber,
        Executor()->Generation(),
        Config->GetAgentRequestTimeout(),
        State->GetMeta().GetMuteIOErrors());
    Actors.insert(actor);
}

void TVolumeActor::HandleDevicesAcquireFinished(
    const TEvVolumePrivate::TEvDevicesAcquireFinished::TPtr& ev,
    const TActorContext& ctx)
{
    HandleDevicesAcquireFinishedImpl(ev->Get()->GetError(), ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
