#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSyncShardSessionsActor final
    : public TActorBootstrapped<TSyncShardSessionsActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TVector<TString> ShardIds;
    const TRequestInfoPtr RequestInfo;

    NProto::TError Error;
    TMap<TString, TEvIndexTabletPrivate::TShardSessionsInfo> Shard2Info;

public:
    TSyncShardSessionsActor(
        TString logTag,
        TActorId tablet,
        TVector<TString> shardIds,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeSessions(const TActorContext& ctx);
    void HandleDescribeSessionsResponse(
        const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

    void SyncShardSessions(
        const TActorContext& ctx,
        const TString& shardId,
        NProtoPrivate::TDescribeSessionsResponse response);
    void HandleSyncShardSessionsResponse(
        const TEvIndexTabletPrivate::TEvSyncShardSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSyncShardSessionsActor::TSyncShardSessionsActor(
        TString logTag,
        TActorId tablet,
        TVector<TString> shardIds,
        TRequestInfoPtr requestInfo)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , ShardIds(std::move(shardIds))
    , RequestInfo(std::move(requestInfo))
{}

void TSyncShardSessionsActor::Bootstrap(const TActorContext& ctx)
{
    DescribeSessions(ctx);
    Become(&TThis::StateWork);
}

void TSyncShardSessionsActor::DescribeSessions(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& shardId: ShardIds) {
        auto request =
            std::make_unique<TEvIndexTablet::TEvDescribeSessionsRequest>();
        request->Record.SetFileSystemId(shardId);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Sending DescribeSessionsRequest to shard %s",
            LogTag.c_str(),
            shardId.c_str());

        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            {}, // flags
            cookie++);
    }
}

void TSyncShardSessionsActor::HandleDescribeSessionsResponse(
    const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < ShardIds.size());
    const auto& shardId = ShardIds[ev->Cookie];
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] sessions described: %lu (error %s)",
        LogTag.c_str(),
        shardId.c_str(),
        msg->Record.SessionsSize(),
        FormatError(msg->GetError()).c_str());

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
        Shard2Info[shardId] = {};

        if (Shard2Info.size() == ShardIds.size()) {
            ReplyAndDie(ctx);
        }
    } else {
        SyncShardSessions(ctx, shardId, std::move(msg->Record));
    }
}

void TSyncShardSessionsActor::SyncShardSessions(
    const TActorContext& ctx,
    const TString& shardId,
    NProtoPrivate::TDescribeSessionsResponse response)
{
    using TRequest = TEvIndexTabletPrivate::TEvSyncShardSessionsRequest;
    auto request = std::make_unique<TRequest>();
    request->ShardId = shardId;
    request->Sessions = std::move(response);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending SyncShardSessionsRequest for shard %s",
        LogTag.c_str(),
        shardId.c_str());

    ctx.Send(Tablet, request.release());
}

void TSyncShardSessionsActor::HandleSyncShardSessionsResponse(
    const TEvIndexTabletPrivate::TEvSyncShardSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] sessions synced: %lu (error %s)",
        LogTag.c_str(),
        msg->Info.ShardId.c_str(),
        msg->Info.SessionCount,
        FormatError(msg->GetError()).c_str());

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    Shard2Info[msg->Info.ShardId] = std::move(msg->Info);
    if (Shard2Info.size() == ShardIds.size()) {
        ReplyAndDie(ctx);
    }
}

void TSyncShardSessionsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Error = MakeError(E_REJECTED, "tablet is shutting down");
    ReplyAndDie(ctx);
}

void TSyncShardSessionsActor::ReplyAndDie(const TActorContext& ctx)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvSyncSessionsCompleted;
        auto response = std::make_unique<TCompletion>(Error);
        for (auto& x: Shard2Info) {
            response->ShardSessionsInfos.push_back(std::move(x.second));
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

STFUNC(TSyncShardSessionsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDescribeSessionsResponse,
            HandleDescribeSessionsResponse);

        HFunc(
            TEvIndexTabletPrivate::TEvSyncShardSessionsResponse,
            HandleSyncShardSessionsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
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
    const TRequestInfoPtr RequestInfo;
    const TVector<NProto::TSession> Sessions;

    size_t ResponsesCollected = 0;

public:
    TCleanupSessionsActor(
        TString logTag,
        TActorId tablet,
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
        TRequestInfoPtr requestInfo,
        TVector<NProto::TSession> sessions)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
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

        auto request =
            std::make_unique<TEvIndexTablet::TEvDestroySessionRequest>();

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
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
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

    SyncSessionsScheduled = false;

    TVector<TString> shardIds;
    // session sync should be enabled only in the main tablet
    if (IsMainTablet()) {
        for (const auto& shardId: GetFileSystem().GetShardFileSystemIds()) {
            shardIds.push_back(shardId);
        }
    }

    if (shardIds.empty()) {
        using TResponse = TEvIndexTabletPrivate::TEvSyncSessionsResponse;
        auto response = std::make_unique<TResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));

        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s SyncSessions dud",
            LogTag.c_str());

        ScheduleSyncSessions(ctx);
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    auto actor = std::make_unique<TSyncShardSessionsActor>(
        LogTag,
        ctx.SelfID,
        std::move(shardIds),
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

    ui32 sessionsSynced = 0;
    for (const auto& shardSessionsInfo: msg->ShardSessionsInfos) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s Synced %lu sessions for shard %s",
            LogTag.c_str(),
            shardSessionsInfo.SessionCount,
            shardSessionsInfo.ShardId.c_str());

        sessionsSynced += shardSessionsInfo.SessionCount;
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Synced %u sessions for %lu shards",
        LogTag.c_str(),
        sessionsSynced,
        msg->ShardSessionsInfos.size());

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

    CleanupSessionsScheduled = false;

    Metrics.SessionCleanupAttempts.fetch_add(
        1,
        std::memory_order_relaxed);

    auto sessions = GetTimedOutSessions(ctx.Now());
    if (sessions.empty()) {
        // nothing to do
        using TResponse = TEvIndexTabletPrivate::TEvCleanupSessionsResponse;
        auto response = std::make_unique<TResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));

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
    requestInfo->StartedTs = ctx.Now();

    auto actor = std::make_unique<TCleanupSessionsActor>(
        LogTag,
        ctx.SelfID,
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
