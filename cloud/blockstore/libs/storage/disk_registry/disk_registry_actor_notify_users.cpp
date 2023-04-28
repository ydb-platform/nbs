#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/notify/notify.h>

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

    TVector<TString> DiskIds;
    TVector<TString> Succeeded;
    TVector<TString> Failures;

    int PendingOperations = 0;

public:
    TNotifyActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr request,
        TVector<TString> notifications);

    void Bootstrap(const TActorContext& ctx);

private:
    void NotifyUsers(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleNotifyDiskErrorResponse(
        const TEvDiskRegistryPrivate::TEvNotifyDiskErrorResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TNotifyActor::TNotifyActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TVector<TString> diskIds)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , DiskIds(std::move(diskIds))
{
    ActivityType = TBlockStoreActivities::DISK_REGISTRY_WORKER;
}

void TNotifyActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (DiskIds.empty()) {
        ReplyAndDie(ctx);
    } else {
        NotifyUsers(ctx);
    }
}

void TNotifyActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyUsersResponse>(
        RequestInfo->CallContext);
    response->Succeeded = std::move(Succeeded);
    response->Failures = std::move(Failures);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());
    Die(ctx);
}

void TNotifyActor::NotifyUsers(const TActorContext& ctx)
{
    PendingOperations = DiskIds.size();

    ui64 cookie = 0;
    for (const auto& diskId: DiskIds) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Notifying users: DiskId=%s",
            TabletID,
            diskId.Quote().c_str());

        auto request = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyDiskErrorRequest>(
            std::move(diskId));
        NCloud::Send(
            ctx,
            Owner,
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

void TNotifyActor::HandleNotifyDiskErrorResponse(
    const TEvDiskRegistryPrivate::TEvNotifyDiskErrorResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto cookie = ev->Cookie;

    --PendingOperations;

    Y_VERIFY(PendingOperations >= 0);

    const auto& diskId = DiskIds[cookie];
    const auto& error = ev->Get()->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Users notification failed: DiskId=%s, Error=%s",
            TabletID,
            diskId.Quote().c_str(),
            FormatError(error).c_str());

        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            ReportUserNotificationError();

            Failures.push_back(diskId);
        }
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Users notification succeeded: DiskId=%s Error=%s",
            TabletID,
            diskId.Quote().c_str(),
            FormatError(error).c_str());

        Succeeded.push_back(diskId);
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

        HFunc(
            TEvDiskRegistryPrivate::TEvNotifyDiskErrorResponse,
            HandleNotifyDiskErrorResponse
        );

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_REGISTRY_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleNotifyDiskError(
    const TEvDiskRegistryPrivate::TEvNotifyDiskErrorRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(NotifyDiskError);

    const auto* msg = ev->Get();

     auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId)
    );

    TDiskInfo diskInfo;
    auto error = State->GetDiskInfo(msg->DiskId, diskInfo);

    if (error.GetCode() == E_NOT_FOUND) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Disk %s not found. Cancel notification.",
            TabletID(),
            msg->DiskId.Quote().c_str());

        auto response = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyDiskErrorResponse>(
            requestInfo->CallContext);

        NCloud::Reply(ctx, *requestInfo, std::move(response));

        return;
    }

    if (HasError(error)) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Unexpected error %s for disk %s. Try send notification anyway.",
            TabletID(),
            FormatError(error).c_str(),
            msg->DiskId.Quote().c_str());
    }

    if (diskInfo.CloudId.empty()) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Empty Cloud Id for disk %s. Postpone notification.",
            TabletID(),
            msg->DiskId.Quote().c_str());

        auto response = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyDiskErrorResponse>(
            MakeError(E_REJECTED, "Empty Cloud Id"),
            requestInfo->CallContext);

        NCloud::Reply(ctx, *requestInfo, std::move(response));

        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Notify users about disk error. DiskId=%s CloudId=%s FolderId=%s UserId=%s",
        TabletID(),
        msg->DiskId.Quote().c_str(),
        diskInfo.CloudId.Quote().c_str(),
        diskInfo.FolderId.Quote().c_str(),
        diskInfo.UserId.Quote().c_str());

    auto* actorSystem = ctx.ActorSystem();
    auto replyFrom = ctx.SelfID;

    NotifyService->NotifyDiskError({
        .DiskId = msg->DiskId,
        .CloudId = diskInfo.CloudId,
        .FolderId = diskInfo.FolderId,
        .UserId = diskInfo.UserId
    }).Subscribe([=] (const auto& future) {
        auto response = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyDiskErrorResponse>(
            future.GetValue(),
            requestInfo->CallContext);

        actorSystem->Send(
            new IEventHandle(
                requestInfo->Sender,
                replyFrom,
                response.release(),
                0,          // flags
                requestInfo->Cookie,
                nullptr,    // forwardOnNondelivery
                std::move(requestInfo->TraceId)));
    });
}

void TDiskRegistryActor::HandleNotifyUsers(
    const TEvDiskRegistryPrivate::TEvNotifyUsersRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    BLOCKSTORE_DISK_REGISTRY_COUNTER(NotifyUsers);

    UsersNotificationStartTs = ctx.Now();

    ErrorNotifications.assign(
        State->GetErrorNotifications().begin(),
        State->GetErrorNotifications().end());

    auto actor = NCloud::Register<TNotifyActor>(
        ctx,
        SelfId(),
        TabletID(),
        CreateRequestInfo(
            SelfId(),
            0,
            MakeIntrusive<TCallContext>(),
            NWilson::TTraceId::NewTraceId()
        ),
        ErrorNotifications
    );
    Actors.insert(actor);
}

void TDiskRegistryActor::NotifyUsers(const NActors::TActorContext& ctx)
{
    if (UsersNotificationInProgress || State->GetErrorNotifications().empty()) {
        return;
    }

    UsersNotificationInProgress = true;

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyUsersRequest>();

    auto deadline = Min(UsersNotificationStartTs, ctx.Now()) + TDuration::Seconds(5);
    if (deadline > ctx.Now()) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Scheduled users notification, now: %lu, deadline: %lu",
            TabletID(),
            ctx.Now().MicroSeconds(),
            deadline.MicroSeconds());

        ctx.Schedule(deadline, request.release());
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Sending users notification request",
            TabletID());

        NCloud::Send(ctx, ctx.SelfID, std::move(request));
    }
}

void TDiskRegistryActor::HandleNotifyUsersResponse(
    const TEvDiskRegistryPrivate::TEvNotifyUsersResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto& msg = *ev->Get();

    TVector<TString> disks = std::move(msg.Succeeded);
    disks.insert(
        disks.end(),
        msg.Failures.begin(),
        msg.Failures.end());

    ExecuteTx<TDeleteErrorNotifications>(
        ctx,
        CreateRequestInfo<TEvDiskRegistryPrivate::TNotifyUsersMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext,
            std::move(ev->TraceId)
        ),
        std::move(disks));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareDeleteErrorNotifications(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteErrorNotifications& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteDeleteErrorNotifications(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteErrorNotifications& args)
{
    Y_UNUSED(ctx);

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Delete notifications: %d",
        TabletID(),
        args.DiskIds.size());

    TDiskRegistryDatabase db(tx.DB);
    for (const auto& diskId: args.DiskIds) {
        State->DeleteErrorNotification(db, diskId);
    }
}

void TDiskRegistryActor::CompleteDeleteErrorNotifications(
    const TActorContext& ctx,
    TTxDiskRegistry::TDeleteErrorNotifications& args)
{
    Y_UNUSED(args);

    UsersNotificationInProgress = false;
    ErrorNotifications.clear();
    NotifyUsers(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
