#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReplaceActor final
    : public TActorBootstrapped<TReplaceActor>
{
private:
    const TChildLogTitle LogTitle;
    const TActorId Owner;
    const TRequestInfoPtr Request;
    const TString DiskId;
    const TString DeviceId;
    const TString DeviceReplacementId;
    const TInstant Timestamp;

public:
    TReplaceActor(
        TChildLogTitle logTitle,
        const TActorId& owner,
        TRequestInfoPtr request,
        TString diskId,
        TString deviceId,
        TString deviceReplacementId,
        TInstant timestamp);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleReplaceDiskDeviceResponse(
        const TEvDiskRegistryPrivate::TEvReplaceDiskDeviceResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TReplaceActor::TReplaceActor(
        TChildLogTitle logTitle,
        const TActorId& owner,
        TRequestInfoPtr request,
        TString diskId,
        TString deviceId,
        TString deviceReplacementId,
        TInstant timestamp)
    : LogTitle(std::move(logTitle))
    , Owner(owner)
    , Request(std::move(request))
    , DiskId(std::move(diskId))
    , DeviceId(std::move(deviceId))
    , DeviceReplacementId(std::move(deviceReplacementId))
    , Timestamp(timestamp)
{}

void TReplaceActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvReplaceDiskDeviceRequest>(
        DiskId,
        DeviceId,
        DeviceReplacementId,
        Timestamp);

    NCloud::Send(ctx, Owner, std::move(request));
}

void TReplaceActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvDiskRegistry::TEvReplaceDeviceResponse>(
        std::move(error));

    NCloud::Reply(ctx, *Request, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

void TReplaceActor::HandleReplaceDiskDeviceResponse(
    const TEvDiskRegistryPrivate::TEvReplaceDiskDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "%s Can't replace device %s: %s",
            LogTitle.GetWithTime().c_str(),
            DeviceId.c_str(),
            FormatError(msg->GetError()).c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    ReplyAndDie(ctx, NProto::TError());
}

void TReplaceActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TReplaceActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistryPrivate::TEvReplaceDiskDeviceResponse,
            HandleReplaceDiskDeviceResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleReplaceDevice(
    const TEvDiskRegistry::TEvReplaceDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ReplaceDevice);

    auto* msg = ev->Get();

    const auto& diskId = msg->Record.GetDiskId();
    const auto& deviceId = msg->Record.GetDeviceUUID();
    const auto& deviceReplacementId = msg->Record.GetDeviceReplacementUUID();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received ReplaceDevice request: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str());

    auto actor = NCloud::Register<TReplaceActor>(
        ctx,
        LogTitle.GetChildWithTags(GetCycleCount(), {{"DiskId", diskId}}),
        SelfId(),
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        diskId,
        deviceId,
        deviceReplacementId,
        Now());
    Actors.insert(actor);
}

void TDiskRegistryActor::HandleReplaceDiskDevice(
    const TEvDiskRegistryPrivate::TEvReplaceDiskDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ReplaceDiskDevice);

    auto* msg = ev->Get();

    using TMethod = TEvDiskRegistryPrivate::TReplaceDiskDeviceMethod;

    auto requestInfo = CreateRequestInfo<TMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TReplaceDevice>(
        ctx,
        std::move(requestInfo),
        std::move(msg->DiskId),
        std::move(msg->DeviceId),
        std::move(msg->DeviceReplacementId),
        msg->Timestamp);
}

bool TDiskRegistryActor::PrepareReplaceDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TReplaceDevice& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteReplaceDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TReplaceDevice& args)
{
    Y_UNUSED(ctx);

    bool updated = false;
    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->ReplaceDevice(
        db,
        args.DiskId,
        args.DeviceId,
        args.DeviceReplacementId,
        args.Timestamp,
        "replaced",
        true,   // manual
        &updated);

    Y_UNUSED(updated);
}

void TDiskRegistryActor::CompleteReplaceDevice(
    const TActorContext& ctx,
    TTxDiskRegistry::TReplaceDevice& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s ReplaceDevice error %s: %s",
            LogTitle.GetWithTime().c_str(),
            args.DiskId.Quote().c_str(),
            FormatError(args.Error).c_str());
    }

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);

    auto response = std::make_unique<TEvDiskRegistryPrivate::TEvReplaceDiskDeviceResponse>(
        args.Error);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
