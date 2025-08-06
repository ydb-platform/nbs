#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/notify/notify.h>
#include <cloud/blockstore/libs/storage/disk_registry/model/user_notification.h>
#include <cloud/storage/core/libs/common/format.h>

#include <util/generic/yexception.h>

#include <type_traits>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

TUserNotificationKey MakeNotificationKey(const NProto::TUserNotification& notif)
{
    return TUserNotificationKey{
        GetEntityId(notif),
        notif.GetSeqNo()};
}

TResultOrError<NNotify::TEvent> ConvertNotificationEvent(
    const NProto::TUserNotification& notif)
{
    using EEventCase = NProto::TUserNotification::EventCase;

    switch (notif.GetEventCase()) {
        case EEventCase::kDiskError:
            return NNotify::TEvent{
                NNotify::TDiskError{
                    .DiskId = notif.GetDiskError().GetDiskId()
                }};
        case EEventCase::kDiskBackOnline:
            return NNotify::TEvent{
                NNotify::TDiskBackOnline{
                    .DiskId = notif.GetDiskBackOnline().GetDiskId()
                }};
        case EEventCase::EVENT_NOT_SET:
            return MakeError(
                E_INVALID_STATE,
                "User notification event is not set");
    }
}

////////////////////////////////////////////////////////////////////////////////

class TNotifyActor final
    : public TActorBootstrapped<TNotifyActor>
{
private:
    const TActorId Owner;
    const ui64 TabletID;
    const TRequestInfoPtr RequestInfo;

    TVector<NProto::TUserNotification> Notifications;
    TVector<TUserNotificationKey> Succeeded;
    TVector<TUserNotificationKey> Failures;

    int PendingOperations = 0;

public:
    TNotifyActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr request,
        TVector<NProto::TUserNotification> notifications);

    void Bootstrap(const TActorContext& ctx);

private:
    void NotifyUsers(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleNotifyUserEventResponse(
        const TEvDiskRegistryPrivate::TEvNotifyUserEventResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TNotifyActor::TNotifyActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TVector<NProto::TUserNotification> notifications)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , Notifications(std::move(notifications))
{}

void TNotifyActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (Notifications.empty()) {
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
    PendingOperations = Notifications.size();

    ui64 cookie = 0;
    for (const auto& notif: Notifications) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Notifying users: %s",
            TabletID,
            ToString(notif).c_str());

        auto request =
            std::make_unique<TEvDiskRegistryPrivate::TEvNotifyUserEventRequest>(notif);
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

void TNotifyActor::HandleNotifyUserEventResponse(
    const TEvDiskRegistryPrivate::TEvNotifyUserEventResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto cookie = ev->Cookie;

    --PendingOperations;

    Y_ABORT_UNLESS(PendingOperations >= 0);

    const auto& notif = Notifications[cookie];
    const auto& error = ev->Get()->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Users notification failed: %s, Error=%s",
            TabletID,
            ToString(notif).c_str(),
            FormatError(error).c_str());

        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            ReportUserNotificationError(
                {{"TabletID", TabletID}, {"Notification", ToString(notif)}});

            Failures.push_back(MakeNotificationKey(notif));
        }
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Users notification succeeded: %s, Error=%s",
            TabletID,
            ToString(notif).c_str(),
            FormatError(error).c_str());

        Succeeded.push_back(MakeNotificationKey(notif));
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
            TEvDiskRegistryPrivate::TEvNotifyUserEventResponse,
            HandleNotifyUserEventResponse
        );

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

void TDiskRegistryActor::HandleNotifyUserEvent(
    const TEvDiskRegistryPrivate::TEvNotifyUserEventRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(NotifyUserEvent);

    const auto* msg = ev->Get();

     auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext
    );

    const auto& notif = msg->Notification;
    // Note: Only disk events supported
    const auto& diskId = GetEntityId(notif);
    TDiskInfo diskInfo;
    auto error = State->GetDiskInfo(diskId, diskInfo);

    if (error.GetCode() == E_NOT_FOUND) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Disk not found. Cancel notification of the event %s.",
            TabletID(),
            ToString(notif).c_str());

        auto response = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyUserEventResponse>(
            requestInfo->CallContext);

        NCloud::Reply(ctx, *requestInfo, std::move(response));

        return;
    }

    if (HasError(error)) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Unexpected error %s for disk %s. Try send notification anyway.",
            TabletID(),
            FormatError(error).c_str(),
            diskId.Quote().c_str());
    }

    if (diskInfo.CloudId.empty()) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Empty Cloud Id for disk %s. Postpone notification.",
            TabletID(),
            diskId.Quote().c_str());

        auto response = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyUserEventResponse>(
            MakeError(E_REJECTED, "Empty Cloud Id"),
            requestInfo->CallContext);

        NCloud::Reply(ctx, *requestInfo, std::move(response));

        return;
    }

    auto eventOrError = ConvertNotificationEvent(notif);

    if (HasError(eventOrError)) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Bad event: %s. Drop it.",
            TabletID(),
            FormatError(eventOrError.GetError()).c_str());

        auto response = std::make_unique<TEvDiskRegistryPrivate::TEvNotifyUserEventResponse>(
            eventOrError.GetError(),
            requestInfo->CallContext);

        NCloud::Reply(ctx, *requestInfo, std::move(response));

        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Notify users: %s, CloudId=%s, FolderId=%s, UserId=%s",
        TabletID(),
        ToString(notif).c_str(),
        diskInfo.CloudId.Quote().c_str(),
        diskInfo.FolderId.Quote().c_str(),
        diskInfo.UserId.Quote().c_str());

    auto* actorSystem = ctx.ActorSystem();
    auto replyFrom = ctx.SelfID;

    // TODO: Remove legacy compatibility in next release
    auto timestamp = notif.GetTimestamp()
        ? TInstant::MicroSeconds(notif.GetTimestamp())
        : ctx.Now();

    NotifyService->Notify({
        .CloudId = diskInfo.CloudId,
        .FolderId = diskInfo.FolderId,
        .UserId = diskInfo.UserId,
        .Timestamp = timestamp,
        .Event = eventOrError.ExtractResult()
    }).Subscribe([=] (const auto& future) {
        auto response = std::make_unique<
                TEvDiskRegistryPrivate::TEvNotifyUserEventResponse>(
            future.GetValue(),
            requestInfo->CallContext);

        actorSystem->Send(
            new IEventHandle(
                requestInfo->Sender,
                replyFrom,
                response.release(),
                0,          // flags
                requestInfo->Cookie));
    });
}

void TDiskRegistryActor::HandleNotifyUsers(
    const TEvDiskRegistryPrivate::TEvNotifyUsersRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    BLOCKSTORE_DISK_REGISTRY_COUNTER(NotifyUsers);

    UsersNotificationStartTs = ctx.Now();

    State->GetUserNotifications(UserNotificationsBeingProcessed);

    auto actor = NCloud::Register<TNotifyActor>(
        ctx,
        SelfId(),
        TabletID(),
        CreateRequestInfo(
            SelfId(),
            0,
            MakeIntrusive<TCallContext>()),
        UserNotificationsBeingProcessed
    );
    Actors.insert(actor);
}

void TDiskRegistryActor::NotifyUsers(const NActors::TActorContext& ctx)
{
    if (UsersNotificationInProgress || !State->GetUserNotifications().Count) {
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

    TVector<TUserNotificationKey> notifications = std::move(msg.Succeeded);
    notifications.insert(
        notifications.end(),
        msg.Failures.begin(),
        msg.Failures.end());

    ExecuteTx<TDeleteUserNotifications>(
        ctx,
        CreateRequestInfo<TEvDiskRegistryPrivate::TNotifyUsersMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        std::move(notifications));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareDeleteUserNotifications(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteUserNotifications& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteDeleteUserNotifications(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteUserNotifications& args)
{
    Y_UNUSED(ctx);

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Delete notifications: %d",
        TabletID(),
        args.Notifications.size());

    TDiskRegistryDatabase db(tx.DB);
    for (const auto& notif: args.Notifications) {
        State->DeleteUserNotification(db, notif.EntityId, notif.SeqNo);
    }
}

void TDiskRegistryActor::CompleteDeleteUserNotifications(
    const TActorContext& ctx,
    TTxDiskRegistry::TDeleteUserNotifications& args)
{
    Y_UNUSED(args);

    UsersNotificationInProgress = false;
    UserNotificationsBeingProcessed.clear();
    NotifyUsers(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
