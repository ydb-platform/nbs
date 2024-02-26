#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <cloud/blockstore/libs/kikimr/events.h>

#include <util/string/join.h>
#include <util/generic/cast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAcquireDiskActor final
    : public TActorBootstrapped<TAcquireDiskActor>
{
private:
    const TActorId Owner;
    TRequestInfoPtr RequestInfo;
    TVector<NProto::TDeviceConfig> Devices;
    const ui32 LogicalBlockSize = 0;
    const TString DiskId;
    const TString ClientId;
    const NProto::EVolumeAccessMode AccessMode;
    const ui64 MountSeqNumber;
    const ui32 VolumeGeneration;
    const TDuration RequestTimeout;

    int PendingRequests = 0;

    TVector<TAgentAcquireDevicesCachedRequest> SentAcquireRequests;

public:
    TAcquireDiskActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        TVector<NProto::TDeviceConfig> devices,
        ui32 logicalBlockSize,
        TString diskId,
        TString clientId,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        ui32 volumeGeneration,
        TDuration requestTimeout);

    void Bootstrap(const TActorContext& ctx);

private:
    void PrepareRequest(NProto::TAcquireDevicesRequest& request);
    void PrepareRequest(NProto::TReleaseDevicesRequest& request);

    void FinishAcquireDisk(const TActorContext& ctx, NProto::TError error);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void OnAcquireResponse(
        const TActorContext& ctx,
        ui32 nodeId,
        NProto::TError error);

    template <typename TRequest>
    struct TSentRequest
    {
        TString AgentId;
        std::unique_ptr<TRequest> Request;
    };

    template <typename TRequest>
    TVector<TSentRequest<TRequest>> SendRequests(const TActorContext& ctx);

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
        TRequestInfoPtr requestInfo,
        TVector<NProto::TDeviceConfig> devices,
        ui32 logicalBlockSize,
        TString diskId,
        TString clientId,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        ui32 volumeGeneration,
        TDuration requestTimeout)
    : Owner(owner)
    , RequestInfo(std::move(requestInfo))
    , Devices(std::move(devices))
    , LogicalBlockSize(logicalBlockSize)
    , DiskId(std::move(diskId))
    , ClientId(std::move(clientId))
    , AccessMode(accessMode)
    , MountSeqNumber(mountSeqNumber)
    , VolumeGeneration(volumeGeneration)
    , RequestTimeout(requestTimeout)
{
    ActivityType = TBlockStoreActivities::DISK_REGISTRY_WORKER;

    SortBy(Devices, [] (auto& d) {
        return d.GetNodeId();
    });
}

void TAcquireDiskActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateAcquire);

    if (Devices.empty()) {
        FinishAcquireDisk(ctx, MakeError(E_REJECTED, "nothing to acquire"));
        return;
    }

    ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "[%s] Sending acquire devices requests for disk %s, targets %s",
        ClientId.c_str(),
        DiskId.c_str(),
        LogTargets().c_str());

    auto sentRequests =
        SendRequests<TEvDiskAgent::TEvAcquireDevicesRequest>(ctx);
    Y_ABORT_UNLESS(SentAcquireRequests.empty());
    SentAcquireRequests.reserve(sentRequests.size());
    TInstant now = ctx.Now();
    for (auto& x: sentRequests) {
        SentAcquireRequests.emplace_back(
            std::move(x.AgentId),
            std::move(x.Request->Record),
            now);
    }
}

void TAcquireDiskActor::FinishAcquireDisk(
    const TActorContext& ctx,
    NProto::TError error)
{
    using TType = TEvDiskRegistryPrivate::TEvFinishAcquireDiskRequest;
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TType>(
            DiskId,
            ClientId,
            std::move(SentAcquireRequests)));

    ReplyAndDie(ctx, std::move(error));
}

void TAcquireDiskActor::PrepareRequest(NProto::TAcquireDevicesRequest& request)
{
    request.MutableHeaders()->SetClientId(ClientId);
    request.SetAccessMode(AccessMode);
    request.SetMountSeqNumber(MountSeqNumber);
    request.SetDiskId(DiskId);
    request.SetVolumeGeneration(VolumeGeneration);
}

void TAcquireDiskActor::PrepareRequest(NProto::TReleaseDevicesRequest& request)
{
    request.MutableHeaders()->SetClientId(ClientId);
}

template <typename TRequest>
TVector<TAcquireDiskActor::TSentRequest<TRequest>> TAcquireDiskActor::SendRequests(
    const TActorContext& ctx)
{
    auto* it = Devices.begin();
    TVector<TSentRequest<TRequest>> sentRequests;
    while (it != Devices.end()) {
        auto request = std::make_unique<TRequest>();
        TSentRequest<TRequest> requestCopy{
            it->GetAgentId(),
            std::make_unique<TRequest>()};
        PrepareRequest(request->Record);
        PrepareRequest(requestCopy.Request->Record);

        const ui32 nodeId = it->GetNodeId();

        for (; it != Devices.end() && it->GetNodeId() == nodeId; ++it) {
            *request->Record.AddDeviceUUIDs() = it->GetDeviceUUID();
            *requestCopy.Request->Record.AddDeviceUUIDs() = it->GetDeviceUUID();
        }

        ++PendingRequests;

        LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "[%s] Send an acquire request to node #%d. Devices: %s",
            ClientId.c_str(),
            nodeId,
            JoinSeq(", ", request->Record.GetDeviceUUIDs()).c_str());

        sentRequests.push_back(std::move(requestCopy));
        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(nodeId),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            nodeId,
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(event.release());
    }
    return sentRequests;
}

void TAcquireDiskActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvDiskRegistry::TEvAcquireDiskResponse>(
        std::move(error));

    if (HasError(response->GetError())) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "[%s] AcquireDisk %s targets %s error: %s",
            ClientId.c_str(),
            DiskId.c_str(),
            LogTargets().c_str(),
            FormatError(response->GetError()).c_str());
    } else {
        response->Record.MutableDevices()->Reserve(Devices.size());

        for (auto& device: Devices) {
            ToLogicalBlocks(device, LogicalBlockSize);
            *response->Record.AddDevices() = std::move(device);
        }
    }
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());
    Die(ctx);
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

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "[%s] AcquireDevices on the node #%d %s error: %s",
            ClientId.c_str(),
            nodeId,
            LogTargets().c_str(),
            FormatError(error).c_str());

        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
                "[%s] Canceling acquire operation for disk %s, targets %s",
                ClientId.c_str(),
                DiskId.c_str(),
                LogTargets().c_str());

            SendRequests<TEvDiskAgent::TEvReleaseDevicesRequest>(ctx);
        }

        SentAcquireRequests.clear();
        FinishAcquireDisk(ctx, std::move(error));

        return;
    }

    if (--PendingRequests == 0) {
        FinishAcquireDisk(ctx, {});
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

        HFunc(TEvDiskAgent::TEvAcquireDevicesResponse,
            HandleAcquireDevicesResponse);
        HFunc(TEvDiskAgent::TEvAcquireDevicesRequest,
            HandleAcquireDevicesUndelivery);

        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_REGISTRY_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleAcquireDisk(
    const TEvDiskRegistry::TEvAcquireDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AcquireDisk);

    const auto* msg = ev->Get();

    auto clientId = msg->Record.GetHeaders().GetClientId();
    auto diskId = msg->Record.GetDiskId();

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
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
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
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
        devices.insert(devices.end(),
            std::make_move_iterator(replica.begin()),
            std::make_move_iterator(replica.end()));
    }

    auto actor = NCloud::Register<TAcquireDiskActor>(
        ctx,
        ctx.SelfID,
        CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            msg->CallContext),
        std::move(devices),
        diskInfo.LogicalBlockSize,
        std::move(diskId),
        std::move(clientId),
        msg->Record.GetAccessMode(),
        msg->Record.GetMountSeqNumber(),
        msg->Record.GetVolumeGeneration(),
        Config->GetAgentRequestTimeout());
    Actors.insert(actor);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleFinishAcquireDisk(
    const TEvDiskRegistryPrivate::TEvFinishAcquireDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    State->FinishAcquireDisk(msg->DiskId);

    OnDiskAcquired(std::move(msg->SentRequests));

    auto response = std::make_unique<
        TEvDiskRegistryPrivate::TEvFinishAcquireDiskResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
