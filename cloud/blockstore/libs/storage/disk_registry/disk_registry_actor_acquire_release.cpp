#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <util/generic/cast.h>
#include <util/string/join.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<NProto::TDeviceConfig> ExtractDevicesFromDiskInfo(TDiskInfo& diskInfo)
{
    TVector devices = std::move(diskInfo.Devices);
    devices.reserve(
        devices.size() * (diskInfo.Replicas.size() + 1) +
        diskInfo.Migrations.size());

    for (auto& migration: diskInfo.Migrations) {
        devices.emplace_back(std::move(*migration.MutableTargetDevice()));
    }
    for (auto& replica: diskInfo.Replicas) {
        devices.insert(
            devices.end(),
            std::make_move_iterator(replica.begin()),
            std::make_move_iterator(replica.end()));
    }

    return devices;
}

////////////////////////////////////////////////////////////////////////////////

class TAcquireReleaseDiskProxyActor final
    : public TActorBootstrapped<TAcquireReleaseDiskProxyActor>
{
public:
    enum EOperationType
    {
        ACQUIRE_DISK,
        RELEASE_DISK,
    };

private:
    const TActorId Owner;

    NAcquireReleaseDevices::TAcquireReleaseDevicesInfo AcquireReleaseInfo;

    const ui32 LogicalBlockSize;

    TRequestInfoPtr RequestInfo;

    EOperationType OperationType;

    std::optional<TActorId> WorkerId;

    std::optional<std::variant<
        NAcquireReleaseDevices::TDevicesAcquireFinished,
        NAcquireReleaseDevices::TDevicesReleaseFinished>>
        OperationFinishedResponce;

public:
    TAcquireReleaseDiskProxyActor(
        const TActorId& owner,
        NAcquireReleaseDevices::TAcquireReleaseDevicesInfo acquireReleaseInfo,
        ui32 logicalBlockSize,
        TRequestInfoPtr requestInfo,
        EOperationType operationType);

    void Bootstrap(const TActorContext& ctx);

private:
    template <typename TReqType, typename TEventType>
    void SendOperationFinishedToOwner(
        const TActorContext& ctx,
        const TEventType& ev)
    {
        auto* msg = ev->Get();

        WorkerId = std::nullopt;

        OperationFinishedResponce = *msg;
        auto request = std::make_unique<TReqType>(
            msg->DiskId,
            msg->ClientId,
            msg->SentRequests);
        NCloud::Send(ctx, Owner, std::move(request));
    }

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void ReplyAndDieAcquire(const TActorContext& ctx, NProto::TError error);

    void ReplyAndDieRelease(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleDevicesAcquireFinished(
        const NAcquireReleaseDevices::TEvDevicesAcquireFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFinishAcquireDiskResponse(
        const TEvDiskRegistryPrivate::TEvFinishAcquireDiskResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDevicesReleaseFinished(
        const NAcquireReleaseDevices::TEvDevicesReleaseFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRemoveDiskSessionResponse(
        const TEvDiskRegistryPrivate::TEvRemoveDiskSessionResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TAcquireReleaseDiskProxyActor::TAcquireReleaseDiskProxyActor(
        const TActorId& owner,
        NAcquireReleaseDevices::TAcquireReleaseDevicesInfo acquireReleaseInfo,
        ui32 logicalBlockSize,
        TRequestInfoPtr requestInfo,
        EOperationType operationType)
    : Owner(owner)
    , AcquireReleaseInfo(std::move(acquireReleaseInfo))
    , LogicalBlockSize(logicalBlockSize)
    , RequestInfo(std::move(requestInfo))
    , OperationType(operationType)
{}

void TAcquireReleaseDiskProxyActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    WorkerId = [&]()
    {
        switch (OperationType) {
            case ACQUIRE_DISK:
                return NAcquireReleaseDevices::CreateAcquireDevicesActor(
                    ctx,
                    ctx.SelfID,
                    std::move(AcquireReleaseInfo),
                    TBlockStoreComponents::DISK_REGISTRY_WORKER);
            case RELEASE_DISK:
                return NAcquireReleaseDevices::CreateReleaseDevicesActor(
                    ctx,
                    ctx.SelfID,
                    std::move(AcquireReleaseInfo),
                    TBlockStoreComponents::DISK_REGISTRY_WORKER);
        }
    }();
}

void TAcquireReleaseDiskProxyActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    switch (OperationType) {
        case ACQUIRE_DISK:
            ReplyAndDieAcquire(ctx, std::move(error));
            return;
        case RELEASE_DISK:
            ReplyAndDieRelease(ctx, std::move(error));
            return;
    }
}

void TAcquireReleaseDiskProxyActor::ReplyAndDieAcquire(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto* msg =
        OperationFinishedResponce.has_value()
            ? &std::get<NAcquireReleaseDevices::TDevicesAcquireFinished>(
                  OperationFinishedResponce.value())
            : nullptr;

    auto response = std::make_unique<TEvDiskRegistry::TEvAcquireDiskResponse>(
        !HasError(error) && msg ? std::move(msg->Error) : std::move(error));

    if (!HasError(response->GetError()) && msg) {
        response->Record.MutableDevices()->Reserve(msg->Devices.size());

        for (auto& device: msg->Devices) {
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

void TAcquireReleaseDiskProxyActor::ReplyAndDieRelease(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvDiskRegistry::TEvReleaseDiskResponse>(
        std::move(error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());
    Die(ctx);
}

STFUNC(TAcquireReleaseDiskProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            NAcquireReleaseDevices::TEvDevicesAcquireFinished,
            HandleDevicesAcquireFinished);
        HFunc(
            TEvDiskRegistryPrivate::TEvFinishAcquireDiskResponse,
            HandleFinishAcquireDiskResponse);

        HFunc(
            NAcquireReleaseDevices::TEvDevicesReleaseFinished,
            HandleDevicesReleaseFinished);
        HFunc(
            TEvDiskRegistryPrivate::TEvRemoveDiskSessionResponse,
            HandleRemoveDiskSessionResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER);
            break;
    }
}

void TAcquireReleaseDiskProxyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (WorkerId) {
        NCloud::Send(
            ctx,
            WorkerId.value(),
            std::make_unique<TEvents::TEvPoisonPill>());
    }

    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TAcquireReleaseDiskProxyActor::HandleDevicesAcquireFinished(
    const NAcquireReleaseDevices::TEvDevicesAcquireFinished::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(OperationType == ACQUIRE_DISK);
    SendOperationFinishedToOwner<
        TEvDiskRegistryPrivate::TEvFinishAcquireDiskRequest>(ctx, ev);
}

void TAcquireReleaseDiskProxyActor::HandleFinishAcquireDiskResponse(
    const TEvDiskRegistryPrivate::TEvFinishAcquireDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(OperationType == ACQUIRE_DISK);
    Y_UNUSED(ev);

    ReplyAndDie(ctx, {});
}

void TAcquireReleaseDiskProxyActor::HandleDevicesReleaseFinished(
    const NAcquireReleaseDevices::TEvDevicesReleaseFinished::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_ABORT_UNLESS(OperationType == RELEASE_DISK);
    SendOperationFinishedToOwner<
        TEvDiskRegistryPrivate::TEvRemoveDiskSessionRequest>(ctx, ev);
}

void TAcquireReleaseDiskProxyActor::HandleRemoveDiskSessionResponse(
    const TEvDiskRegistryPrivate::TEvRemoveDiskSessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(OperationType == RELEASE_DISK);
    ReplyAndDie(ctx, ev->Get()->GetError());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleAcquireDisk(
    const TEvDiskRegistry::TEvAcquireDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AcquireDisk);

    auto replyWithError = [&](auto error)
    {
        auto response =
            std::make_unique<TEvDiskRegistry::TEvAcquireDiskResponse>(
                std::move(error));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

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
    if (auto error = State->StartAcquireDisk(diskId, diskInfo); HasError(error))
    {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "[%s] AcquireDisk %s error: %s",
            clientId.c_str(),
            diskId.c_str(),
            FormatError(error).c_str());

        replyWithError(std::move(error));
        return;
    }

    if (!State->FilterDevicesAtUnavailableAgents(diskInfo)) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "AcquireeDisk %s. Nothing to acquire",
            diskId.c_str());

        replyWithError(MakeError(S_ALREADY, {}));
        return;
    }

    TVector devices = ExtractDevicesFromDiskInfo(diskInfo);
    auto actor = NCloud::Register<TAcquireReleaseDiskProxyActor>(
        ctx,
        ctx.SelfID,
        NAcquireReleaseDevices::TAcquireReleaseDevicesInfo{
            .Devices = std::move(devices),
            .DiskId = std::move(diskId),
            .ClientId = std::move(clientId),
            .AccessMode = msg->Record.GetAccessMode(),
            .MountSeqNumber = msg->Record.GetMountSeqNumber(),
            .VolumeGeneration = msg->Record.GetVolumeGeneration(),
            .RequestTimeout = Config->GetAgentRequestTimeout(),
            .MuteIOErrors = false,
        },
        diskInfo.LogicalBlockSize,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        TAcquireReleaseDiskProxyActor::ACQUIRE_DISK);
    Actors.insert(actor);
}

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

    if (const auto error = State->GetDiskInfo(diskId, diskInfo);
        HasError(error))
    {
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

    auto actor = NCloud::Register<TAcquireReleaseDiskProxyActor>(
        ctx,
        ctx.SelfID,
        NAcquireReleaseDevices::TAcquireReleaseDevicesInfo{
            .Devices = ExtractDevicesFromDiskInfo(diskInfo),
            .DiskId = std::move(diskId),
            .ClientId = std::move(clientId),
            .AccessMode = std::nullopt,
            .MountSeqNumber = std::nullopt,
            .VolumeGeneration = msg->Record.GetVolumeGeneration(),
            .RequestTimeout = Config->GetAgentRequestTimeout(),
            .MuteIOErrors = false,
        },
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        TAcquireReleaseDiskProxyActor::RELEASE_DISK);
    Actors.insert(actor);
}

void TDiskRegistryActor::HandleRemoveDiskSession(
    const TEvDiskRegistryPrivate::TEvRemoveDiskSessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    OnDiskReleased(msg->SentRequests);

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    State->FinishAcquireDisk(msg->DiskId);
    auto response = std::make_unique<
        TEvDiskRegistryPrivate::TEvRemoveDiskSessionResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
