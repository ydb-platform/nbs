#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNotifyActor final
    : public TActorBootstrapped<TNotifyActor>
{
private:
    const TActorId Owner;
    const ui64 TabletID;
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TVector<TDiskNotification> DiskNotifications;

    TVector<TDiskNotificationResult> NotifiedDisks;
    int PendingOperations = 0;

public:
    TNotifyActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TVector<TDiskNotification> diskIds);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReallocateDisks(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleReallocateDiskResponse(
        const TEvVolume::TEvReallocateDiskResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TNotifyActor::TNotifyActor(
        const TActorId& owner,
        const ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TVector<TDiskNotification> diskIds)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , DiskNotifications(std::move(diskIds))
{}

void TNotifyActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    ReallocateDisks(ctx);

    if (PendingOperations) {
        ctx.Schedule(
            Config->GetNonReplicatedVolumeNotificationTimeout(),
            new TEvents::TEvWakeup());

        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER, "Nothing to do");
    ReplyAndDie(ctx);
}

void TNotifyActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvDiskRegistryPrivate::TEvNotifyDisksResponse>(
            RequestInfo->CallContext,
            std::move(NotifiedDisks));

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());
    Die(ctx);
}

void TNotifyActor::ReallocateDisks(const TActorContext& ctx)
{
    PendingOperations = DiskNotifications.size();

    ui64 cookie = 0;
    for (const auto& [diskId, seqNo]: DiskNotifications) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "[%lu] Notifying volume: DiskId=%s",
            TabletID,
            diskId.Quote().c_str());

        auto request = std::make_unique<TEvVolume::TEvReallocateDiskRequest>();
        request->Record.SetDiskId(diskId);
        NCloud::Send(
            ctx,
            MakeVolumeProxyServiceId(),
            std::move(request),
            cookie++
        );
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNotifyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx);
}

void TNotifyActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "Notify disks request timed out");

    ReplyAndDie(ctx);
}

void TNotifyActor::HandleReallocateDiskResponse(
    const TEvVolume::TEvReallocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto cookie = ev->Cookie;
    auto* msg = ev->Get();

    --PendingOperations;

    Y_ABORT_UNLESS(PendingOperations >= 0);

    const auto& diskId = DiskNotifications[cookie].DiskId;

    if (HasError(msg->Record)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "[%lu] Volume notification failed: DiskId=%s, Error=%s",
            TabletID,
            diskId.Quote().c_str(),
            msg->Record.GetError().GetMessage().Quote().c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "[%lu] Volume notification succeeded: DiskId=%s",
            TabletID,
            diskId.Quote().c_str());

        TVector<NProto::TLaggingDevice> outdatedLaggingDevices;
        for (auto& laggingDevice: *msg->Record.MutableOutdatedLaggingDevices())
        {
            outdatedLaggingDevices.emplace_back(std::move(laggingDevice));
        }
        NotifiedDisks.emplace_back(
            DiskNotifications[cookie],
            std::move(outdatedLaggingDevices));
    }

    if (!PendingOperations) {
        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TNotifyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(TEvVolume::TEvReallocateDiskResponse, HandleReallocateDiskResponse);

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

void TDiskRegistryActor::HandleListDisksToNotify(
    const TEvDiskRegistryPrivate::TEvListDisksToNotifyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ListDisksToNotify);

    const auto& disksToReallocate = State->GetDisksToReallocate();

    TVector<TString> diskIds(Reserve(disksToReallocate.size()));
    for (const auto& [diskId, seqNo]: disksToReallocate) {
        diskIds.push_back(diskId);
    }

    auto response = std::make_unique<TEvDiskRegistryPrivate::TEvListDisksToNotifyResponse>(
        std::move(diskIds));

    NCloud::Send(ctx, ev->Sender, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::ReallocateDisks(const TActorContext& ctx)
{
    if (DisksNotificationInProgress || State->GetDisksToReallocate().empty()) {
        return;
    }

    DisksNotificationInProgress = true;

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyDisksRequest>();

    auto deadline = Min(DisksNotificationStartTs, ctx.Now()) +
                    Config->GetDiskRegistryDisksNotificationTimeout();
    if (deadline > ctx.Now()) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Scheduled disks notification, now: %lu, deadline: %lu",
            TabletID(),
            ctx.Now().MicroSeconds(),
            deadline.MicroSeconds());

        ctx.Schedule(deadline, request.release());
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Sending disks notification request",
            TabletID());

        NCloud::Send(ctx, ctx.SelfID, std::move(request));
    }
}

void TDiskRegistryActor::HandleNotifyDisks(
    const TEvDiskRegistryPrivate::TEvNotifyDisksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    BLOCKSTORE_DISK_REGISTRY_COUNTER(NotifyDisks);

    DisksNotificationStartTs = ctx.Now();

    DisksBeingNotified.reserve(State->GetDisksToReallocate().size());
    for (const auto& [diskId, seqNo]: State->GetDisksToReallocate()) {
        DisksBeingNotified.emplace_back(diskId, seqNo);
    }

    auto actor = NCloud::Register<TNotifyActor>(
        ctx,
        SelfId(),
        TabletID(),
        CreateRequestInfo(
            SelfId(),
            0,
            MakeIntrusive<TCallContext>()
        ),
        Config,
        DisksBeingNotified
    );
    Actors.insert(actor);
}

void TDiskRegistryActor::HandleNotifyDisksResponse(
    const TEvDiskRegistryPrivate::TEvNotifyDisksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ExecuteTx<TDeleteNotifiedDisks>(
        ctx,
        CreateRequestInfo<TEvDiskRegistryPrivate::TNotifyDisksMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        std::move(ev->Get()->NotifiedDisks)
    );
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareDeleteNotifiedDisks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteNotifiedDisks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteDeleteNotifiedDisks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteNotifiedDisks& args)
{
    TDiskRegistryDatabase db(tx.DB);
    for (auto& notification: args.DiskNotifications) {
        State->DeleteDiskToReallocate(ctx.Now(), db, std::move(notification));
    }
}

void TDiskRegistryActor::CompleteDeleteNotifiedDisks(
    const TActorContext& ctx,
    TTxDiskRegistry::TDeleteNotifiedDisks& args)
{
    Y_UNUSED(args);

    DisksNotificationInProgress = false;
    DisksBeingNotified.clear();

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);
    StartMigration(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
