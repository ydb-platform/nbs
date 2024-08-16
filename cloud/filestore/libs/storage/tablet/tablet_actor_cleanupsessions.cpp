#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSyncFollowerSessionsActor final
    : public TActorBootstrapped<TSyncFollowerSessionsActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TString FileSystemId;
    const TVector<TString> FollowerIds;
    const TRequestInfoPtr RequestInfo;

    NProto::TError Error;
    TMap<TString, TEvIndexTabletPrivate::TFollowerSessionsInfo> Follower2Info;

public:
    TSyncFollowerSessionsActor(
        TString logTag,
        TActorId tablet,
        TString fileSystemId,
        TVector<TString> followerIds,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeSessions(const TActorContext& ctx);
    void HandleDescribeSessionsResponse(
        const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

    void SyncFollowerSessions(
        const TActorContext& ctx,
        const TString& followerId,
        NProtoPrivate::TDescribeSessionsResponse response);
    void HandleSyncFollowerSessionsResponse(
        const TEvIndexTabletPrivate::TEvSyncFollowerSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSyncFollowerSessionsActor::TSyncFollowerSessionsActor(
        TString logTag,
        TActorId tablet,
        TString fileSystemId,
        TVector<TString> followerIds,
        TRequestInfoPtr requestInfo)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , FileSystemId(std::move(fileSystemId))
    , FollowerIds(std::move(followerIds))
    , RequestInfo(std::move(requestInfo))
{}

void TSyncFollowerSessionsActor::Bootstrap(const TActorContext& ctx)
{
    DescribeSessions(ctx);
    Become(&TThis::StateWork);
}

void TSyncFollowerSessionsActor::DescribeSessions(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& followerId: FollowerIds) {
        auto request =
            std::make_unique<TEvIndexTablet::TEvDescribeSessionsRequest>();
        request->Record.SetFileSystemId(followerId);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Sending DescribeSessionsRequest to follower %s",
            LogTag.c_str(),
            followerId.c_str());

        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            {}, // flags
            cookie++);
    }
}

void TSyncFollowerSessionsActor::HandleDescribeSessionsResponse(
    const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < FollowerIds.size());
    const auto& followerId = FollowerIds[ev->Cookie];
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] sessions described: %lu (error %s)",
        LogTag.c_str(),
        followerId.c_str(),
        msg->Record.SessionsSize(),
        FormatError(msg->GetError()).c_str());

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
        Follower2Info[followerId] = {};

        if (Follower2Info.size() == FollowerIds.size()) {
            ReplyAndDie(ctx);
        }
    } else {
        SyncFollowerSessions(ctx, followerId, std::move(msg->Record));
    }
}

void TSyncFollowerSessionsActor::SyncFollowerSessions(
    const TActorContext& ctx,
    const TString& followerId,
    NProtoPrivate::TDescribeSessionsResponse response)
{
    using TRequest = TEvIndexTabletPrivate::TEvSyncFollowerSessionsRequest;
    auto request = std::make_unique<TRequest>();
    request->FollowerId = followerId;
    request->Sessions = std::move(response);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending SyncFollowerSessionsRequest for follower %s",
        LogTag.c_str(),
        followerId.c_str());

    ctx.Send(Tablet, request.release());
}

void TSyncFollowerSessionsActor::HandleSyncFollowerSessionsResponse(
    const TEvIndexTabletPrivate::TEvSyncFollowerSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] sessions synced: %lu (error %s)",
        LogTag.c_str(),
        msg->Info.FollowerId.c_str(),
        msg->Info.SessionCount,
        FormatError(msg->GetError()).c_str());

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    Follower2Info[msg->Info.FollowerId] = std::move(msg->Info);
    if (Follower2Info.size() == FollowerIds.size()) {
        ReplyAndDie(ctx);
    }
}

void TSyncFollowerSessionsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Error = MakeError(E_REJECTED, "tablet is shutting down");
    ReplyAndDie(ctx);
}

void TSyncFollowerSessionsActor::ReplyAndDie(const TActorContext& ctx)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvSyncSessionsCompleted;
        auto response = std::make_unique<TCompletion>(Error);
        for (auto& x: Follower2Info) {
            response->FollowerSessionsInfos.push_back(std::move(x.second));
        }
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        using TResponse = TEvIndexTabletPrivate::TEvSyncSessionsResponse;
        auto response = std::make_unique<TResponse>(Error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TSyncFollowerSessionsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDescribeSessionsResponse,
            HandleDescribeSessionsResponse);

        HFunc(
            TEvIndexTabletPrivate::TEvSyncFollowerSessionsResponse,
            HandleSyncFollowerSessionsResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCleanupSessionsActor final
    : public TActorBootstrapped<TCleanupSessionsActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TString FileSystemId;
    const TRequestInfoPtr RequestInfo;
    const TVector<NProto::TSession> Sessions;

    size_t ResponsesCollected = 0;

public:
    TCleanupSessionsActor(
        TString logTag,
        TActorId tablet,
        TString fileSystemId,
        TRequestInfoPtr requestInfo,
        TVector<NProto::TSession> sessions);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DestroySessions(const TActorContext& ctx);
    void HandleSessionDestroyed(
        const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TCleanupSessionsActor::TCleanupSessionsActor(
        TString logTag,
        TActorId tablet,
        TString fileSystemId,
        TRequestInfoPtr requestInfo,
        TVector<NProto::TSession> sessions)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , FileSystemId(std::move(fileSystemId))
    , RequestInfo(std::move(requestInfo))
    , Sessions(std::move(sessions))
{}

void TCleanupSessionsActor::Bootstrap(const TActorContext& ctx)
{
    DestroySessions(ctx);
    Become(&TThis::StateWork);
}

void TCleanupSessionsActor::DestroySessions(const TActorContext& ctx)
{
    for (size_t i = 0; i < Sessions.size(); ++i) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s[%s] initiating session cleanup",
            LogTag.c_str(),
            Sessions[i].GetSessionId().c_str());

        auto request = std::make_unique<TEvIndexTablet::TEvDestroySessionRequest>();
        request->CallContext = MakeIntrusive<TCallContext>(FileSystemId);

        auto* headers = request->Record.MutableHeaders();
        headers->SetSessionId(Sessions[i].GetSessionId());
        headers->SetClientId(Sessions[i].GetClientId());
        headers->SetSessionSeqNo(Sessions[i].GetMaxSeqNo());

        NCloud::Send(ctx, Tablet, std::move(request), i);
    }
}

void TCleanupSessionsActor::HandleSessionDestroyed(
    const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < Sessions.size());
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] session cleaned: (error %s)",
        LogTag.c_str(),
        Sessions[ev->Cookie].GetSessionId().c_str(),
        FormatError(msg->GetError()).c_str());

    if (++ResponsesCollected == Sessions.size()) {
        ReplyAndDie(ctx);
    }
}

void TCleanupSessionsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TCleanupSessionsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvCleanupSessionsCompleted;
        auto response = std::make_unique<TCompletion>(error);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        using TResponse = TEvIndexTabletPrivate::TEvCleanupSessionsResponse;
        auto response = std::make_unique<TResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TCleanupSessionsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTablet::TEvDestroySessionResponse, HandleSessionDestroyed);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::ScheduleSyncSessions(const TActorContext& ctx)
{
    if (!SyncSessionsScheduled) {
        ctx.Schedule(
            // TODO: make a separate config setting
            Config->GetIdleSessionTimeout() / 3,
            new TEvIndexTabletPrivate::TEvSyncSessionsRequest());
        SyncSessionsScheduled = true;
    }
}

void TIndexTabletActor::HandleSyncSessions(
    const TEvIndexTabletPrivate::TEvSyncSessionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (ev->Sender == ctx.SelfID) {
        SyncSessionsScheduled = false;
    }

    TVector<TString> followerIds;
    for (const auto& followerId: GetFileSystem().GetFollowerFileSystemIds()) {
        followerIds.push_back(followerId);
    }

    if (followerIds.empty()) {
        using TResponse = TEvIndexTabletPrivate::TEvSyncSessionsResponse;
        if (ev->Sender != ctx.SelfID) {
            auto response = std::make_unique<TResponse>();
            NCloud::Reply(ctx, *ev, std::move(response));
        }

        ScheduleSyncSessions(ctx);
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto actor = std::make_unique<TSyncFollowerSessionsActor>(
        LogTag,
        ctx.SelfID,
        GetFileSystemId(),
        std::move(followerIds),
        std::move(requestInfo));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleSyncSessionsCompleted(
    const TEvIndexTabletPrivate::TEvSyncSessionsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s SyncSessions completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    for (const auto& followerSessionsInfo: msg->FollowerSessionsInfos) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s Synced %lu sessions for follower %s",
            LogTag.c_str(),
            followerSessionsInfo.SessionCount,
            followerSessionsInfo.FollowerId.c_str());
    }

    WorkerActors.erase(ev->Sender);
    ScheduleSyncSessions(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::ScheduleCleanupSessions(const TActorContext& ctx)
{
    if (!CleanupSessionsScheduled) {
        ctx.Schedule(
            Config->GetIdleSessionTimeout(),
            new TEvIndexTabletPrivate::TEvCleanupSessionsRequest());
        CleanupSessionsScheduled = true;
    }
}

void TIndexTabletActor::HandleCleanupSessions(
    const TEvIndexTabletPrivate::TEvCleanupSessionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (ev->Sender == ctx.SelfID) {
        CleanupSessionsScheduled = false;
    }

    auto sessions = GetTimeoutedSessions(ctx.Now());
    if (!sessions) {
        // nothing to do
        using TResponse = TEvIndexTabletPrivate::TEvCleanupSessionsResponse;
        if (ev->Sender != ctx.SelfID) {
            auto response = std::make_unique<TResponse>();
            NCloud::Reply(ctx, *ev, std::move(response));
        }

        ScheduleCleanupSessions(ctx);
        return;
    }

    Metrics.SessionTimeouts.fetch_add(
        sessions.size(),
        std::memory_order_relaxed);

    TVector<NProto::TSession> list(sessions.size());
    for (size_t i = 0; i < sessions.size(); ++i) {
        list[i].CopyFrom(*sessions[i]);
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto actor = std::make_unique<TCleanupSessionsActor>(
        LogTag,
        ctx.SelfID,
        GetFileSystemId(),
        std::move(requestInfo),
        std::move(list));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleCleanupSessionsCompleted(
    const TEvIndexTabletPrivate::TEvCleanupSessionsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s CleanupSessions completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    WorkerActors.erase(ev->Sender);
    ScheduleCleanupSessions(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
