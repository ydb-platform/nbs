#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>

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
    // the shards which have been processed - successfully or not
    THashSet<TString> ProcessedShardIds;
    ui32 SessionCount = 0;

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
    void HandleBuildShardSessionsDiffResponse(
        const TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse::TPtr&
            ev,
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
        ProcessedShardIds.insert(shardId);

        if (ProcessedShardIds.size() == ShardIds.size()) {
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
    using TRequest = TEvIndexTabletPrivate::TEvBuildShardSessionsDiffRequest;
    auto request = std::make_unique<TRequest>();
    request->ShardId = shardId;
    request->Sessions = std::move(response);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending BuildShardSessionsDiffRequest for shard %s",
        LogTag.c_str(),
        shardId.c_str());

    ctx.Send(Tablet, request.release());
}

void TSyncShardSessionsActor::HandleBuildShardSessionsDiffResponse(
    const TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] sessions synced: %u (error %s)",
        LogTag.c_str(),
        msg->ShardId.c_str(),
        msg->SessionCount,
        FormatError(msg->GetError()).c_str());

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    if (ProcessedShardIds.insert(msg->ShardId).second) {
        SessionCount += msg->SessionCount;
    }

    if (ProcessedShardIds.size() == ShardIds.size()) {
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
        response->ShardCount = static_cast<ui32>(ProcessedShardIds.size());
        response->SessionCount = SessionCount;
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
            TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse,
            HandleBuildShardSessionsDiffResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

//
// Does the same thing as TSyncShardSessionsActor but in a way which is
// suitable for the filesystems with lots of shards and sessions:
// * the number of shards which are processed simultaneously is limited;
// * the tablet only tells what has to be done with the sessions of a shard,
//   it's this actor who talks to the shard;
// * everything that has to be done with the sessions of a shard is sent to
//   it via a single SyncShardSessions request, so there's never more than one
//   such request per shard in flight;
// * a round which is triggered by a change of the sessions (see
//   ChangesOnlyRound) doesn't ask the shards for their sessions: the tablet
//   remembers what each shard has and tells what has been changed since
//   then. A shard is asked for its sessions only if the tablet knows nothing
//   about it or if the shard turns out to have been restarted.
//
// Used if LazyShardSessionCreationEnabled is set.
//

class TBatchedSyncShardSessionsActor final
    : public TActorBootstrapped<TBatchedSyncShardSessionsActor>
{
private:
    struct TShardState
    {
        // the shard works in adapter mode - it's synced the old way
        bool AdapterMode = false;
        // the number of sessions sent to the shard
        ui32 SessionCount = 0;
        bool Completed = false;
        // the shard has done everything it had to do
        bool Synced = false;
        // the shard hasn't been asked for its sessions: it gets only what
        // has been changed since its last sync
        bool ChangesOnly = false;
        // The generation which the shard tablet is supposed to have: the one
        // which has described the sessions of the shard or (ChangesOnly) the
        // one whose sessions are known to the tablet.
        ui32 Generation = 0;
    };

private:
    const TString LogTag;
    const TActorId Tablet;
    const TVector<TString> ShardIds;
    const THashSet<TString> AdapterModeShardIds;
    const TRequestInfoPtr RequestInfo;
    const ui32 MaxInflight;
    // The shards are not asked for their sessions unless it's needed, see
    // RequestShardChanges.
    const bool ChangesOnlyRound;

    TVector<TShardState> ShardStates;
    ui32 NextShardIdx = 0;
    ui32 InflightShardCount = 0;
    ui32 CompletedShardCount = 0;

    NProto::TError Error;

public:
    TBatchedSyncShardSessionsActor(
        TString logTag,
        TActorId tablet,
        TVector<TString> shardIds,
        THashSet<TString> adapterModeShardIds,
        TRequestInfoPtr requestInfo,
        ui32 maxInflight,
        bool changesOnlyRound);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ProcessNextShards(const TActorContext& ctx);

    void RequestShardChanges(const TActorContext& ctx, ui32 shardIdx);

    void DescribeShard(const TActorContext& ctx, ui32 shardIdx);
    void HandleDescribeShardSessionsResponse(
        const TEvIndexTablet::TEvDescribeShardSessionsResponse::TPtr& ev,
        const TActorContext& ctx);
    void HandleDescribeSessionsResponse(
        const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleBuildShardSessionsDiffResponse(
        const TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse::TPtr&
            ev,
        const TActorContext& ctx);

    void HandleSyncShardSessionsResponse(
        const TEvIndexTablet::TEvSyncShardSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

    void OnShardCompleted(const TActorContext& ctx, ui32 shardIdx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TBatchedSyncShardSessionsActor::TBatchedSyncShardSessionsActor(
        TString logTag,
        TActorId tablet,
        TVector<TString> shardIds,
        THashSet<TString> adapterModeShardIds,
        TRequestInfoPtr requestInfo,
        ui32 maxInflight,
        bool changesOnlyRound)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , ShardIds(std::move(shardIds))
    , AdapterModeShardIds(std::move(adapterModeShardIds))
    , RequestInfo(std::move(requestInfo))
    , MaxInflight(Max<ui32>(maxInflight, 1))
    , ChangesOnlyRound(changesOnlyRound)
    , ShardStates(ShardIds.size())
{}

void TBatchedSyncShardSessionsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    ProcessNextShards(ctx);
}

void TBatchedSyncShardSessionsActor::ProcessNextShards(
    const TActorContext& ctx)
{
    while (InflightShardCount < MaxInflight && NextShardIdx < ShardIds.size()) {
        const ui32 shardIdx = NextShardIdx++;

        auto& state = ShardStates[shardIdx];
        state.AdapterMode = AdapterModeShardIds.contains(ShardIds[shardIdx]);

        if (ChangesOnlyRound && state.AdapterMode) {
            // The sessions of such a shard are managed via the per-session
            // requests which the tablet sends upon each change, so there are
            // no changes which the shard hasn't got. Its sessions are
            // compared with the sessions of the tablet by the regular rounds.
            state.Completed = true;
            ++CompletedShardCount;
            continue;
        }

        ++InflightShardCount;

        if (ChangesOnlyRound) {
            RequestShardChanges(ctx, shardIdx);
        } else {
            DescribeShard(ctx, shardIdx);
        }
    }

    if (CompletedShardCount == ShardIds.size()) {
        ReplyAndDie(ctx);
    }
}

void TBatchedSyncShardSessionsActor::RequestShardChanges(
    const TActorContext& ctx,
    ui32 shardIdx)
{
    //
    // Asking each shard for all its sessions upon each change of the
    // sessions is too expensive. The tablet remembers which version of its
    // sessions each shard has, so it's able to tell what the shard lacks.
    //

    ShardStates[shardIdx].ChangesOnly = true;

    using TRequest = TEvIndexTabletPrivate::TEvBuildShardSessionsDiffRequest;
    auto request = std::make_unique<TRequest>();
    request->ShardId = ShardIds[shardIdx];
    request->ChangesOnly = true;

    ctx.Send(
        Tablet,
        request.release(),
        {}, // flags
        shardIdx);
}

void TBatchedSyncShardSessionsActor::DescribeShard(
    const TActorContext& ctx,
    ui32 shardIdx)
{
    const auto& shardId = ShardIds[shardIdx];

    // the sessions of the shard are going to be compared with the sessions
    // of the tablet
    ShardStates[shardIdx].ChangesOnly = false;

    if (ShardStates[shardIdx].AdapterMode) {
        // the sessions of such a shard are managed via the per-session
        // requests - the tablet takes care of it, it needs the same
        // description of the sessions as the sync which works the old way
        auto request =
            std::make_unique<TEvIndexTablet::TEvDescribeSessionsRequest>();
        request->Record.SetFileSystemId(shardId);

        LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s Sending DescribeSessionsRequest to shard %s",
            LogTag.c_str(),
            shardId.c_str());

        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            {}, // flags
            shardIdx);
        return;
    }

    auto request =
        std::make_unique<TEvIndexTablet::TEvDescribeShardSessionsRequest>();
    request->Record.SetFileSystemId(shardId);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s Sending DescribeShardSessionsRequest to shard %s",
        LogTag.c_str(),
        shardId.c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release(),
        {}, // flags
        shardIdx);
}

void TBatchedSyncShardSessionsActor::HandleDescribeShardSessionsResponse(
    const TEvIndexTablet::TEvDescribeShardSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < ShardIds.size());
    const auto shardIdx = static_cast<ui32>(ev->Cookie);
    const auto& shardId = ShardIds[shardIdx];

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] shard sessions described: %lu, adapter mode: %d (error %s)",
        LogTag.c_str(),
        shardId.c_str(),
        static_cast<ui64>(msg->Record.SessionsSize()),
        msg->Record.GetAdapterModeEnabled(),
        FormatError(msg->GetError()).c_str());

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
        OnShardCompleted(ctx, shardIdx);
        return;
    }

    if (msg->Record.GetAdapterModeEnabled()) {
        // the shard turned out to work in adapter mode - switching to the
        // old way, the tablet will remember it
        ShardStates[shardIdx].AdapterMode = true;
        DescribeShard(ctx, shardIdx);
        return;
    }

    // the shard is supposed to process the request which follows without
    // being restarted in between, see HandleSyncShardSessionsResponse
    ShardStates[shardIdx].Generation = msg->Record.GetGeneration();

    //
    // The tablet compares the sessions of the shard with its own ones.
    //

    using TRequest = TEvIndexTabletPrivate::TEvBuildShardSessionsDiffRequest;
    auto request = std::make_unique<TRequest>();
    request->ShardId = shardId;
    request->ShardSessions = std::move(msg->Record);

    ctx.Send(
        Tablet,
        request.release(),
        {}, // flags
        shardIdx);
}

void TBatchedSyncShardSessionsActor::HandleDescribeSessionsResponse(
    const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < ShardIds.size());
    const auto shardIdx = static_cast<ui32>(ev->Cookie);
    const auto& shardId = ShardIds[shardIdx];

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] sessions described: %lu (error %s)",
        LogTag.c_str(),
        shardId.c_str(),
        msg->Record.SessionsSize(),
        FormatError(msg->GetError()).c_str());

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
        OnShardCompleted(ctx, shardIdx);
        return;
    }

    //
    // It's a shard which works in adapter mode: the tablet syncs its
    // sessions itself.
    //

    using TRequest = TEvIndexTabletPrivate::TEvBuildShardSessionsDiffRequest;
    auto request = std::make_unique<TRequest>();
    request->ShardId = shardId;
    request->Sessions = std::move(msg->Record);
    request->AdapterMode = true;

    ctx.Send(
        Tablet,
        request.release(),
        {}, // flags
        shardIdx);
}

void TBatchedSyncShardSessionsActor::HandleBuildShardSessionsDiffResponse(
    const TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < ShardIds.size());
    const auto shardIdx = static_cast<ui32>(ev->Cookie);
    const auto& shardId = ShardIds[shardIdx];
    auto& state = ShardStates[shardIdx];

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] sessions to create or recover: %lu, to reset: %lu"
        ", to destroy: %lu (error %s)",
        LogTag.c_str(),
        shardId.c_str(),
        msg->SessionsToSync.size(),
        msg->SessionsToReset.size(),
        msg->SessionsToDestroy.size(),
        FormatError(msg->GetError()).c_str());

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
        OnShardCompleted(ctx, shardIdx);
        return;
    }

    if (msg->NeedShardSessions) {
        // the tablet doesn't know what this shard has
        DescribeShard(ctx, shardIdx);
        return;
    }

    if (state.ChangesOnly) {
        state.Generation = msg->ShardGeneration;
    }

    state.SessionCount = msg->SessionCount;

    if (msg->SessionsToSync.empty()
            && msg->SessionsToReset.empty()
            && msg->SessionsToDestroy.empty())
    {
        // the sessions of the shard are in sync
        state.Synced = true;
        OnShardCompleted(ctx, shardIdx);
        return;
    }

    using TRequest = TEvIndexTablet::TEvSyncShardSessionsRequest;
    auto request = std::make_unique<TRequest>();
    request->Record.SetFileSystemId(shardId);
    for (auto& session: msg->SessionsToSync) {
        *request->Record.AddSessions() = std::move(session);
    }
    for (auto& session: msg->SessionsToReset) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s[%s] resetting session s:%s, n:%lu",
            LogTag.c_str(),
            shardId.c_str(),
            session.GetSessionId().Quote().c_str(),
            session.GetSessionSeqNo());

        *request->Record.AddSessionsToReset() = std::move(session);
    }
    for (auto& session: msg->SessionsToDestroy) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s[%s] destroying session s:%s, n:%lu",
            LogTag.c_str(),
            shardId.c_str(),
            session.GetSessionId().Quote().c_str(),
            session.GetSessionSeqNo());

        *request->Record.AddSessionsToDestroy() = std::move(session);
    }

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release(),
        {}, // flags
        shardIdx);
}

void TBatchedSyncShardSessionsActor::HandleSyncShardSessionsResponse(
    const TEvIndexTablet::TEvSyncShardSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < ShardIds.size());
    const auto shardIdx = static_cast<ui32>(ev->Cookie);
    const auto& shardId = ShardIds[shardIdx];
    auto& state = ShardStates[shardIdx];

    if (HasError(msg->GetError())) {
        LOG_ERROR(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s[%s] failed to sync sessions: %s, adapter mode: %d",
            LogTag.c_str(),
            shardId.c_str(),
            FormatError(msg->GetError()).c_str(),
            msg->Record.GetAdapterModeEnabled());

        if (state.ChangesOnly && msg->Record.GetAdapterModeEnabled()) {
            // The shard turns out to work in adapter mode. Asking it for its
            // sessions is the way to let the tablet know about it, see
            // HandleDescribeShardSessionsResponse.
            DescribeShard(ctx, shardIdx);
            return;
        }

        // if the shard turns out to work in adapter mode, then the next
        // sync which asks it for its sessions will learn it from
        // DescribeShardSessionsResponse and will let the tablet create its
        // sessions the other way
        Error = msg->GetError();
    } else if (msg->Record.GetGeneration() != state.Generation) {
        //
        // The shard has been restarted. None of its sessions is active after
        // that: the shard is going to destroy them (see IdleSessionTimeout)
        // unless they get recovered, and what has just been sent to the
        // shard recovers only the sessions which were sent.
        //

        LOG_WARN(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s[%s] shard generation changed: %u -> %u, changes only: %d",
            LogTag.c_str(),
            shardId.c_str(),
            state.Generation,
            msg->Record.GetGeneration(),
            state.ChangesOnly);

        if (state.ChangesOnly) {
            // so the sessions of the shard have to be compared with the
            // sessions of the tablet
            DescribeShard(ctx, shardIdx);
            return;
        }

        // The shard has been restarted after it described its sessions, so
        // the description was outdated. The shard is not reported as a
        // synced one - the next round deals with it.
        Error = MakeError(E_REJECTED, TStringBuilder()
            << "shard " << shardId << " has been restarted");
    } else {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s[%s] sessions created: %u, recovered: %u",
            LogTag.c_str(),
            shardId.c_str(),
            msg->Record.GetCreatedSessionCount(),
            msg->Record.GetRecoveredSessionCount());

        state.Synced = true;
    }

    OnShardCompleted(ctx, shardIdx);
}

void TBatchedSyncShardSessionsActor::OnShardCompleted(
    const TActorContext& ctx,
    ui32 shardIdx)
{
    auto& state = ShardStates[shardIdx];

    TABLET_VERIFY(!state.Completed);
    state.Completed = true;

    TABLET_VERIFY(InflightShardCount);
    --InflightShardCount;
    ++CompletedShardCount;

    ProcessNextShards(ctx);
}

void TBatchedSyncShardSessionsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Error = MakeError(E_REJECTED, "tablet is shutting down");
    ReplyAndDie(ctx);
}

void TBatchedSyncShardSessionsActor::ReplyAndDie(const TActorContext& ctx)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvSyncSessionsCompleted;
        auto response = std::make_unique<TCompletion>(Error);
        for (ui32 i = 0; i < ShardStates.size(); ++i) {
            const auto& state = ShardStates[i];
            if (state.Completed) {
                ++response->ShardCount;
                response->SessionCount += state.SessionCount;
            }
            if (state.Synced) {
                response->SyncedShards.push_back(
                    {ShardIds[i], state.Generation});
            }
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

STFUNC(TBatchedSyncShardSessionsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDescribeShardSessionsResponse,
            HandleDescribeShardSessionsResponse);
        HFunc(
            TEvIndexTablet::TEvDescribeSessionsResponse,
            HandleDescribeSessionsResponse);

        HFunc(
            TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse,
            HandleBuildShardSessionsDiffResponse);

        HFunc(
            TEvIndexTablet::TEvSyncShardSessionsResponse,
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

void TIndexTabletActor::ScheduleOutOfScheduleSyncSessions(
    const TActorContext& ctx)
{
    if (SyncSessionsInProgress) {
        // the current sync might have already processed some of the shards
        SyncSessionsRequested = true;
        return;
    }

    if (!OutOfScheduleSyncSessionsScheduled) {
        auto request =
            std::make_unique<TEvIndexTabletPrivate::TEvSyncSessionsRequest>();
        request->OutOfSchedule = true;

        // the delay lets a single sync process the sessions of all the
        // clients which connect at about the same time
        ctx.Schedule(Config->GetShardSessionsSyncDelay(), request.release());
        OutOfScheduleSyncSessionsScheduled = true;
    }
}

void TIndexTabletActor::ScheduleUrgentSyncSessions(const TActorContext& ctx)
{
    if (SyncSessionsInProgress) {
        // the next sync is started upon the completion of the current one,
        // see HandleSyncSessionsCompleted
        return;
    }

    if (!UrgentSyncSessionsScheduled) {
        auto request =
            std::make_unique<TEvIndexTabletPrivate::TEvSyncSessionsRequest>();
        request->OutOfSchedule = true;
        request->Urgent = true;

        NCloud::Send(ctx, SelfId(), std::move(request));
        UrgentSyncSessionsScheduled = true;
    }
}

void TIndexTabletActor::HandleSyncSessions(
    const TEvIndexTabletPrivate::TEvSyncSessionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->Urgent) {
        UrgentSyncSessionsScheduled = false;
    } else if (msg->OutOfSchedule) {
        OutOfScheduleSyncSessionsScheduled = false;
    } else {
        SyncSessionsScheduled = false;
    }

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

    const bool lazyShardSessionCreationEnabled =
        Config->GetLazyShardSessionCreationEnabled();

    if (lazyShardSessionCreationEnabled && SyncSessionsInProgress) {
        // The sync may touch lots of shards - there is no point in running
        // several syncs simultaneously. One more sync will be run after the
        // completion of the current one.
        SyncSessionsRequested = true;
        if (!msg->OutOfSchedule) {
            // and it should do what this request is for: compare the
            // sessions of all the shards with the sessions of this tablet
            FullSyncSessionsRequested = true;
        }

        using TResponse = TEvIndexTabletPrivate::TEvSyncSessionsResponse;
        auto response = std::make_unique<TResponse>(
            MakeError(S_ALREADY, "sync is in progress"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    NActors::IActorPtr actor;
    if (lazyShardSessionCreationEnabled) {
        SyncSessionsInProgress = true;
        // the round delivers all the changes which have been made so far
        SyncSessionsStartVersion = ShardSessionsVersion;
        SyncSessionsDeltas.clear();

        //
        // The rounds which are run on the regular schedule (and the ones
        // which are requested explicitly) ask each shard for its sessions
        // and compare them with the sessions of this tablet. That's what
        // fixes any discrepancy, but it's expensive: all the sessions of all
        // the shards have to be transferred and looked through. The rounds
        // which are triggered by the changes of the sessions don't do it: a
        // shard gets only what has been changed since its last sync, see
        // HandleBuildShardSessionsDiff.
        //

        const bool changesOnly =
            msg->OutOfSchedule && !FullSyncSessionsRequested;
        FullSyncSessionsRequested = false;

        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s SyncSessions round started, version: %lu, changes only: %d",
            LogTag.c_str(),
            SyncSessionsStartVersion,
            changesOnly);

        auto adapterModeShardIds = GetAdapterModeShardIds();

        actor = std::make_unique<TBatchedSyncShardSessionsActor>(
            LogTag,
            ctx.SelfID,
            std::move(shardIds),
            THashSet<TString>(
                adapterModeShardIds.begin(),
                adapterModeShardIds.end()),
            std::move(requestInfo),
            Config->GetMaxShardSessionsSyncInflight(),
            changesOnly);
    } else {
        actor = std::make_unique<TSyncShardSessionsActor>(
            LogTag,
            ctx.SelfID,
            std::move(shardIds),
            std::move(requestInfo));
    }

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

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Synced %u sessions for %u shards",
        LogTag.c_str(),
        msg->SessionCount,
        msg->ShardCount);

    WorkerActors.erase(ev->Sender);
    ScheduleSyncSessions(ctx);

    if (!SyncSessionsInProgress) {
        // It was the sync which doesn't work in rounds (it's used if
        // LazyShardSessionCreationEnabled is not set) - it doesn't deliver
        // ResetSession and DestroySession requests to the shards.
        RejectPendingShardSessionRequests(
            ctx,
            MakeError(E_REJECTED, "shard sessions sync has been disabled"));
        return;
    }

    SyncSessionsInProgress = false;
    SyncSessionsDeltas.clear();

    // each of these shards has got everything that had been changed by the
    // start of the round and has done everything this tablet told it to do
    for (const auto& shard: msg->SyncedShards) {
        ShardSyncStates[shard.ShardId] =
            TShardSyncState{SyncSessionsStartVersion, shard.Generation};
    }

    // the requests which have been processed by all the shards (and the
    // ones which have been waiting for the shards for too long) can be
    // processed by this tablet now
    ProcessPendingShardSessionRequests(ctx);

    const bool syncRequested = SyncSessionsRequested;
    SyncSessionsRequested = false;

    const bool hasUndeliveredRequests = AnyOf(
        PendingShardSessionRequests,
        [] (const auto& x) {
            return !x.second.Applying;
        });

    if (hasUndeliveredRequests) {
        if (HasError(msg->GetError())) {
            // Some of the shards are unavailable. The requests stay here -
            // the next round tries to deliver them once again, but there's
            // no point in doing it right away.
            ScheduleOutOfScheduleSyncSessions(ctx);
        } else {
            // these requests arrived after the start of this round
            ScheduleUrgentSyncSessions(ctx);
        }
    } else if (syncRequested) {
        // the sessions changed while the sync was running
        ScheduleOutOfScheduleSyncSessions(ctx);
    }
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

    Metrics->SessionCleanupAttempts.fetch_add(
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

    Metrics->SessionTimeouts.fetch_add(
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
