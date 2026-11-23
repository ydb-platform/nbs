#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

//
// Completes a SyncShardSessions request which contains ResetSession and
// DestroySession requests of the main tablet. The tablet processes them via
// its regular transactions, this actor waits for all of them and replies to
// the sender of SyncShardSessions.
//

class TShardSessionOpsActor final
    : public TActorBootstrapped<TShardSessionOpsActor>
{
private:
    using TResponse = TEvIndexTablet::TEvSyncShardSessionsResponse;

    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    // the number of transactions which have been started by the tablet
    const ui32 OpCount;
    // the result of the part of the request which has already been processed:
    // the sessions which had to be created or recovered
    NProtoPrivate::TSyncShardSessionsResponse Response;

    ui32 CompletedOpCount = 0;
    NProto::TError Error;

public:
    TShardSessionOpsActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui32 opCount,
        NProtoPrivate::TSyncShardSessionsResponse response);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDestroySessionResponse(
        const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleResetSessionResponse(
        const TEvService::TEvResetSessionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void OnOpCompleted(const TActorContext& ctx, const NProto::TError& error);
    void ReplyAndDie(const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TShardSessionOpsActor::TShardSessionOpsActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui32 opCount,
        NProtoPrivate::TSyncShardSessionsResponse response)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , OpCount(opCount)
    , Response(std::move(response))
{}

void TShardSessionOpsActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    // the transactions have already been started by the tablet
    Become(&TThis::StateWork);
}

void TShardSessionOpsActor::HandleDestroySessionResponse(
    const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    OnOpCompleted(ctx, ev->Get()->GetError());
}

void TShardSessionOpsActor::HandleResetSessionResponse(
    const TEvService::TEvResetSessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    OnOpCompleted(ctx, ev->Get()->GetError());
}

void TShardSessionOpsActor::OnOpCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        LOG_ERROR(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s Failed to reset or destroy a session: %s",
            LogTag.c_str(),
            FormatError(error).c_str());

        Error = error;
    }

    if (++CompletedOpCount == OpCount) {
        ReplyAndDie(ctx);
    }
}

void TShardSessionOpsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "tablet is shutting down");
    ReplyAndDie(ctx);
}

void TShardSessionOpsActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TResponse>();
    response->Record = std::move(Response);
    if (HasError(Error)) {
        *response->Record.MutableError() = Error;
    }

    using TCompletion = TEvIndexTabletPrivate::TEvShardRequestCompleted;
    auto completion = std::make_unique<TCompletion>(Error);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    NCloud::Send(ctx, Tablet, std::move(completion));

    Die(ctx);
}

STFUNC(TShardSessionOpsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDestroySessionResponse,
            HandleDestroySessionResponse);
        HFunc(
            TEvService::TEvResetSessionResponse,
            HandleResetSessionResponse);

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
// Misc helpers.

TVector<TString> TIndexTabletActor::GetAdapterModeShardIds() const
{
    THashSet<TString> fastShardIds;
    for (const auto& shard: GetFileSystem().GetShardTopology().GetShards()) {
        if (shard.GetType() == NProtoPrivate::TShardTopology::E_FASTSHARD) {
            fastShardIds.insert(shard.GetShardId());
        }
    }

    TVector<TString> shardIds;
    if (fastShardIds.empty() && AdapterModeShardIds.empty()) {
        return shardIds;
    }

    for (const auto& shardId: GetFileSystem().GetShardFileSystemIds()) {
        if (fastShardIds.contains(shardId)
                || AdapterModeShardIds.contains(shardId))
        {
            shardIds.push_back(shardId);
        }
    }

    return shardIds;
}

bool TIndexTabletActor::HasShardsWithSyncedSessions(
    const TVector<TString>& adapterModeShardIds) const
{
    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    return adapterModeShardIds.size() < static_cast<size_t>(shardIds.size());
}

bool TIndexTabletActor::ShouldSyncShardSessionRequests() const
{
    return IsMainTablet()
        && Config->GetLazyShardSessionCreationEnabled()
        && HasShardsWithSyncedSessions(GetAdapterModeShardIds());
}

////////////////////////////////////////////////////////////////////////////////
// The changes of the sessions which the shards should get.

void TIndexTabletActor::OnShardSessionChanged(
    const TActorContext& ctx,
    const TString& sessionId)
{
    auto* session = FindSession(sessionId);
    if (!session) {
        return;
    }

    //
    // The sync rounds which are triggered by the changes don't compare the
    // sessions of the shards with the sessions of this tablet: a shard which
    // has been synced at some version gets the sessions whose versions are
    // newer, see GetShardSessionsDelta.
    //

    session->ShardSyncVersion = ++ShardSessionsVersion;

    if (Config->GetBackgroundShardSessionCreationEnabled()) {
        // no need to wait for the regular sync
        ScheduleOutOfScheduleSyncSessions(ctx);
    }
}

const TIndexTabletState::TShardSessionsDiff&
TIndexTabletActor::GetShardSessionsDelta(ui64 fromVersion)
{
    if (const auto* delta = SyncSessionsDeltas.FindPtr(fromVersion)) {
        return *delta;
    }

    // The changes which have been made after the start of the round are left
    // for the next one - the same way as the requests are, see
    // AddPendingShardSessionRequests.
    const ui64 toVersion = SyncSessionsStartVersion;

    TShardSessionsDiff delta;

    if (Config->GetBackgroundShardSessionCreationEnabled()) {
        // Otherwise the sessions get to the shards only on demand: a shard
        // requests a session (and thus gets its current subsessions) when
        // it can't process a request of a client.
        delta.SessionsToSync = BuildShardSessionsDelta(fromVersion, toVersion);

        // A session which is being reset or destroyed is not synced on its
        // own, see AddPendingShardSessionRequests. It gets a new version
        // upon the completion of the request.
        EraseIf(
            delta.SessionsToSync,
            [&] (const NProtoPrivate::TShardSessionInfo& info) {
                const auto& sessionId = info.GetSessionId();
                return PendingShardSessionRequests.contains(sessionId);
            });
    }

    //
    // The shard hasn't been asked for its sessions, so it's unknown whether
    // it has the sessions which the requests are for. Each request which the
    // shard hasn't seen is sent to it: a request for a session which the
    // shard doesn't have does nothing there.
    //

    for (const auto& [sessionId, request]: PendingShardSessionRequests) {
        if (request.Applying
                || request.Version <= fromVersion
                || request.Version > toVersion)
        {
            continue;
        }

        auto shardRequest = BuildShardSessionRequest(sessionId, request);
        if (request.Reset) {
            delta.SessionsToReset.push_back(std::move(shardRequest));
        } else {
            delta.SessionsToDestroy.push_back(std::move(shardRequest));
        }
    }

    return SyncSessionsDeltas.emplace(fromVersion, std::move(delta))
        .first->second;
}

////////////////////////////////////////////////////////////////////////////////
// ResetSession and DestroySession requests of the clients: the shards
// process them first, this tablet processes them last.

void TIndexTabletActor::EnqueueShardSessionRequest(
    const TActorContext& ctx,
    TString sessionId,
    TPendingShardSessionRequest request)
{
    request.Version = ++ShardSessionsVersion;
    const auto timeout = Config->GetShardSessionRequestTimeout();
    if (timeout != TDuration::Zero()) {
        request.Deadline = ctx.Now() + timeout;
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s %s s:%s n:%lu waits for the shard sessions sync, version: %lu",
        LogTag.c_str(),
        request.Reset ? "ResetSession" : "DestroySession",
        sessionId.Quote().c_str(),
        request.SessionSeqNo,
        request.Version);

    PendingShardSessionRequests[std::move(sessionId)] = std::move(request);

    // the request shouldn't wait for the regular sync
    ScheduleUrgentSyncSessions(ctx);
}

NProtoPrivate::TShardSessionRequest
TIndexTabletActor::BuildShardSessionRequest(
    const TString& sessionId,
    const TPendingShardSessionRequest& request) const
{
    //
    // The shard should make its decision against the same subsessions as
    // this tablet does (see CheckSessionForDestroy), so the session always
    // goes along with the request - the shard makes its copy look the same.
    // That's needed even if the shard is just waiting for the session: the
    // response with the session might have been sent to it long ago.
    //

    NProtoPrivate::TShardSessionRequest shardRequest;
    shardRequest.SetSessionId(sessionId);
    shardRequest.SetSessionSeqNo(request.SessionSeqNo);
    if (auto info = BuildShardSessionInfo(sessionId)) {
        *shardRequest.MutableSession() = std::move(*info);
    }

    return shardRequest;
}

void TIndexTabletActor::AddPendingShardSessionRequests(
    const TString& shardId,
    const NProtoPrivate::TDescribeShardSessionsResponse& shardSessions,
    TShardSessionsDiff& diff) const
{
    if (PendingShardSessionRequests.empty()) {
        return;
    }

    // the requests which are not newer than the version of the shard have
    // already been processed by it
    const auto* syncState = ShardSyncStates.FindPtr(shardId);

    // A sync round delivers the requests which arrived before its start. A
    // newer request can't be completed by this round anyway: the shards
    // which the round has already processed haven't seen it. So it's left
    // for the next round - this way a shard gets a request only once.
    const ui64 syncVersion = SyncSessionsInProgress
        ? SyncSessionsStartVersion
        : ShardSessionsVersion;

    //
    // A session which is being reset or destroyed is not synced on its own.
    // The shards which don't have it don't get it: there's nothing to reset
    // or to destroy there. The shards which have it get its subsessions
    // only together with the request.
    //

    EraseIf(
        diff.SessionsToSync,
        [&] (const NProtoPrivate::TShardSessionInfo& info) {
            return PendingShardSessionRequests.contains(info.GetSessionId());
        });

    THashSet<TString> processedSessionIds;

    auto addRequest = [&] (const TString& sessionId, bool recovering) {
        const auto* request = PendingShardSessionRequests.FindPtr(sessionId);
        if (!request) {
            return;
        }

        if (request->Version > syncVersion) {
            return;
        }

        if (syncState && request->Version <= syncState->Version) {
            // The shard has already processed the request: the request
            // either waits for the other shards or is being processed by
            // this tablet. The session is not touched here until the
            // completion of the request even if the request has left it
            // alive.
            return;
        }

        //
        // The request is sent even if this tablet is already processing it.
        // That's what happens to DestroySession which didn't wait for this
        // shard (see ShardSessionRequestTimeout): the shard should still do
        // the same thing to its copy of the session.
        //

        if (recovering && request->Reset) {
            // the shard is about to get a new copy of the session - there's
            // nothing to reset there
            return;
        }

        if (!processedSessionIds.insert(sessionId).second) {
            return;
        }

        auto shardRequest = BuildShardSessionRequest(sessionId, *request);
        if (request->Reset) {
            diff.SessionsToReset.push_back(std::move(shardRequest));
        } else {
            diff.SessionsToDestroy.push_back(std::move(shardRequest));
        }
    };

    for (const auto& shardSession: shardSessions.GetSessions()) {
        addRequest(shardSession.GetSessionId(), false);
    }

    for (const auto& sessionId: shardSessions.GetRecoveringSessionIds()) {
        addRequest(sessionId, true);
    }
}

void TIndexTabletActor::ProcessPendingShardSessionRequests(
    const TActorContext& ctx)
{
    if (PendingShardSessionRequests.empty()) {
        return;
    }

    //
    // A shard has processed a request if its sessions have been synced by a
    // round which was started after the arrival of the request: such a round
    // delivers the request to the shard. The shards which work in adapter
    // mode don't take part in it: this tablet sends the request to them
    // itself after its own tx.
    //

    const auto adapterModeShardIds = GetAdapterModeShardIds();
    const THashSet<TString> adapterModeShards(
        adapterModeShardIds.begin(),
        adapterModeShardIds.end());

    ui64 syncedVersion = Max<ui64>();
    for (const auto& shardId: GetFileSystem().GetShardFileSystemIds()) {
        if (adapterModeShards.contains(shardId)) {
            continue;
        }

        const auto* syncState = ShardSyncStates.FindPtr(shardId);
        syncedVersion =
            Min<ui64>(syncedVersion, syncState ? syncState->Version : 0);
    }

    const auto now = ctx.Now();

    // a tx may complete (and remove its request) right away
    TVector<TString> sessionIds;
    // the requests which have been waiting for the shards for too long
    TVector<TString> expiredSessionIds;
    for (const auto& [sessionId, request]: PendingShardSessionRequests) {
        if (request.Applying) {
            continue;
        }

        if (request.Version <= syncedVersion) {
            sessionIds.push_back(sessionId);
        } else if (request.Deadline != TInstant::Zero()
                && request.Deadline <= now
                && request.Version <= SyncSessionsStartVersion)
        {
            // The round which has just been completed tried to deliver the
            // request. A request which no round has tried to deliver yet
            // waits for the next one whatever its deadline is.
            expiredSessionIds.push_back(sessionId);
        }
    }

    //
    // A shard which is unavailable shouldn't block the requests forever:
    // nothing else is allowed to change a session while its request is
    // waiting, and the clients are waiting as well.
    //
    // DestroySession is processed by this tablet anyway. The shards which
    // haven't processed the request keep their copies of the session, but
    // that's something the sync is able to fix later: a session which this
    // tablet doesn't have is destroyed in a shard as soon as its sessions
    // get compared with the sessions of this tablet.
    //
    // ResetSession can't be completed this way: the session stays alive, so
    // there would be nothing to tell a shard which missed the request that
    // its copy of the session has to be reset. The request fails instead -
    // the client is supposed to retry it. The shards which have processed
    // the request have reset their copies of the session by this moment.
    //

    // the newest of the DestroySession requests which are not going to be
    // delivered to all the shards
    ui64 abandonedVersion = 0;

    for (const auto& sessionId: expiredSessionIds) {
        auto it = PendingShardSessionRequests.find(sessionId);
        if (it == PendingShardSessionRequests.end()) {
            continue;
        }

        auto& request = it->second;

        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s %s s:%s n:%lu hasn't been processed by all the shards in"
            " time, version: %lu, synced version: %lu",
            LogTag.c_str(),
            request.Reset ? "ResetSession" : "DestroySession",
            sessionId.Quote().c_str(),
            request.SessionSeqNo,
            request.Version,
            syncedVersion);

        if (!request.Reset) {
            abandonedVersion = Max(abandonedVersion, request.Version);
            sessionIds.push_back(sessionId);
            continue;
        }

        using TResponse = TEvService::TEvResetSessionResponse;
        NCloud::Reply(
            ctx,
            *request.RequestInfo,
            std::make_unique<TResponse>(MakeError(
                E_REJECTED,
                "the request hasn't been processed by all the shards in"
                " time")));

        PendingShardSessionRequests.erase(it);

        // the session wasn't synced while the request was in progress
        OnShardSessionChanged(ctx, sessionId);
    }

    if (abandonedVersion) {
        //
        // The shards which haven't seen these requests are not going to get
        // them as the changes: there will be nothing left of the sessions
        // here. So this tablet forgets what it knows about such shards -
        // the next round compares their sessions with the sessions of this
        // tablet and destroys the ones which don't exist here.
        //

        for (const auto& shardId: GetFileSystem().GetShardFileSystemIds()) {
            const auto* syncState = ShardSyncStates.FindPtr(shardId);
            if (syncState && syncState->Version < abandonedVersion) {
                ShardSyncStates.erase(shardId);
            }
        }

        // one more attempt to reach these shards, the regular rounds do the
        // rest
        ScheduleOutOfScheduleSyncSessions(ctx);
    }

    for (const auto& sessionId: sessionIds) {
        auto* request = PendingShardSessionRequests.FindPtr(sessionId);
        if (!request || request->Applying) {
            continue;
        }

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s %s s:%s n:%lu is not waiting for the shards anymore"
            ", version: %lu",
            LogTag.c_str(),
            request->Reset ? "ResetSession" : "DestroySession",
            sessionId.Quote().c_str(),
            request->SessionSeqNo,
            request->Version);

        request->Applying = true;

        auto requestInfo = request->RequestInfo;
        const ui64 sessionSeqNo = request->SessionSeqNo;

        if (request->Reset) {
            auto record = std::move(request->ResetRequest);

            AddInFlightRequest<TEvService::TResetSessionMethod>(*requestInfo);

            ExecuteTx<TResetSession>(
                ctx,
                std::move(requestInfo),
                sessionId,
                sessionSeqNo,
                Config->GetMaxDeleteSessionHandlesPerTx(),
                false /* isContinuation */,
                std::move(record));
        } else {
            auto record = std::move(request->DestroyRequest);

            AddInFlightRequest<TEvIndexTablet::TDestroySessionMethod>(
                *requestInfo);

            ExecuteTx<TDestroySession>(
                ctx,
                std::move(requestInfo),
                sessionId,
                sessionSeqNo,
                Config->GetMaxDeleteSessionHandlesPerTx(),
                false /* isContinuation */,
                std::move(record));
        }
    }
}

void TIndexTabletActor::OnShardSessionRequestCompleted(
    const TActorContext& ctx,
    const TString& sessionId,
    const TRequestInfoPtr& requestInfo)
{
    auto it = PendingShardSessionRequests.find(sessionId);
    if (it == PendingShardSessionRequests.end()
            || it->second.RequestInfo != requestInfo)
    {
        // the request didn't have to wait for the shards
        return;
    }

    PendingShardSessionRequests.erase(it);

    // The session wasn't synced while the request was in progress. If it's
    // still alive, then the shards should get it the way it is now.
    OnShardSessionChanged(ctx, sessionId);
}

void TIndexTabletActor::RejectPendingShardSessionRequests(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    for (auto it = PendingShardSessionRequests.begin();
        it != PendingShardSessionRequests.end();)
    {
        auto& request = it->second;
        if (request.Applying) {
            // the tx of this tablet will reply
            ++it;
            continue;
        }

        if (request.Reset) {
            using TResponse = TEvService::TEvResetSessionResponse;
            NCloud::Reply(
                ctx,
                *request.RequestInfo,
                std::make_unique<TResponse>(error));
        } else {
            using TResponse = TEvIndexTablet::TEvDestroySessionResponse;
            NCloud::Reply(
                ctx,
                *request.RequestInfo,
                std::make_unique<TResponse>(error));
        }

        PendingShardSessionRequests.erase(it++);
    }
}

////////////////////////////////////////////////////////////////////////////////
// RecoverShardSessions: a shard requests the sessions which it needs from the
// main tablet.

void TIndexTabletActor::HandleRecoverShardSessions(
    const TEvIndexTablet::TEvRecoverShardSessionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvIndexTablet::TEvRecoverShardSessionsResponse;

    auto* msg = ev->Get();

    if (!IsMainTablet()) {
        // the sessions are owned by the main tablet
        auto response = std::make_unique<TResponse>(MakeError(
            E_ARGUMENT,
            "sessions can be requested only from the main tablet"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    for (const auto& sessionId: msg->Record.GetSessionIds()) {
        const auto* request = PendingShardSessionRequests.FindPtr(sessionId);
        if (request && !request->Reset) {
            //
            // DestroySession for this session is being delivered to the
            // shards: a shard which doesn't have the session shouldn't get
            // it now. Whether the session survives (the request may destroy
            // just one of its subsessions) will be known upon the completion
            // of the request - the shard should retry.
            //

            auto response = std::make_unique<TResponse>(MakeError(
                E_REJECTED,
                TStringBuilder() << "session " << sessionId.Quote()
                    << " is being destroyed"));
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }
    }

    auto response = std::make_unique<TResponse>();
    for (const auto& sessionId: msg->Record.GetSessionIds()) {
        auto info = BuildShardSessionInfo(sessionId);
        if (!info) {
            // unknown session - the shard will reject the requests of the
            // clients which try to use it
            continue;
        }

        *response->Record.AddSessions() = std::move(*info);
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s RecoverShardSessions from shard %s: %lu sessions requested"
        ", %lu found",
        LogTag.c_str(),
        msg->Record.GetShardId().Quote().c_str(),
        static_cast<ui64>(msg->Record.SessionIdsSize()),
        static_cast<ui64>(response->Record.SessionsSize()));

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////
// DescribeShardSessions: the main tablet asks a shard for its sessions.

void TIndexTabletActor::HandleDescribeShardSessions(
    const TEvIndexTablet::TEvDescribeShardSessionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvIndexTablet::TEvDescribeShardSessionsResponse;

    if (GetFileSystem().GetIsFastShard()) {
        // The tablets which work in adapter mode don't keep the sessions in
        // the tablet state - that's all the main tablet needs to know.
        auto response = std::make_unique<TResponse>();
        response->Record.SetAdapterModeEnabled(true);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (IsMainTablet()) {
        auto response = std::make_unique<TResponse>(MakeError(
            E_ARGUMENT,
            "only a shard has shard sessions"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto response = std::make_unique<TResponse>();
    response->Record.SetGeneration(GetGeneration());
    for (auto& info: DescribeShardSessions()) {
        *response->Record.AddSessions() = std::move(info);
    }

    // The main tablet should know about the sessions which are on their way
    // to this tablet: it might be destroying some of them right now.
    for (const auto& [sessionId, recovery]: SessionRecoveries) {
        response->Record.AddRecoveringSessionIds(sessionId);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////
// SyncShardSessions: the main tablet brings the sessions of a shard in line
// with its own sessions.

void TIndexTabletActor::HandleSyncShardSessions(
    const TEvIndexTablet::TEvSyncShardSessionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvIndexTablet::TEvSyncShardSessionsResponse;

    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s SyncShardSessions: %lu sessions, %lu sessions to reset"
        ", %lu sessions to destroy",
        LogTag.c_str(),
        static_cast<ui64>(msg->Record.SessionsSize()),
        static_cast<ui64>(msg->Record.SessionsToResetSize()),
        static_cast<ui64>(msg->Record.SessionsToDestroySize()));

    if (GetFileSystem().GetIsFastShard()) {
        //
        // The tablets which work in adapter mode don't keep the sessions in
        // the tablet state - their sessions should be created one by one via
        // CreateSession requests.
        //

        auto response = std::make_unique<TResponse>(
            MakeError(E_NOT_IMPLEMENTED, "not supported in adapter mode"));
        response->Record.SetAdapterModeEnabled(true);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (IsMainTablet()) {
        auto response = std::make_unique<TResponse>(MakeError(
            E_ARGUMENT,
            "sessions can be synced this way only in a shard"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (msg->Record.SessionsToResetSize()
            || msg->Record.SessionsToDestroySize())
    {
        // the same check is done for the regular DestroySession requests
        if (auto error = IsDataOperationAllowed(); HasError(error)) {
            auto response = std::make_unique<TResponse>(std::move(error));
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TEvIndexTablet::TSyncShardSessionsMethod>(
        *requestInfo);

    TVector<NProtoPrivate::TShardSessionInfo> sessions;
    sessions.reserve(msg->Record.SessionsSize());
    for (auto& session: *msg->Record.MutableSessions()) {
        sessions.push_back(std::move(session));
    }

    TVector<NProtoPrivate::TShardSessionRequest> sessionsToReset(
        msg->Record.GetSessionsToReset().begin(),
        msg->Record.GetSessionsToReset().end());

    TVector<NProtoPrivate::TShardSessionRequest> sessionsToDestroy(
        msg->Record.GetSessionsToDestroy().begin(),
        msg->Record.GetSessionsToDestroy().end());

    for (const auto& session: sessionsToDestroy) {
        const auto* recovery =
            SessionRecoveries.FindPtr(session.GetSessionId());
        if (!recovery) {
            continue;
        }

        //
        // This tablet has requested the session from the main tablet, but
        // the main tablet either doesn't have it or is destroying it right
        // now - the response is outdated even if it's on its way: it will
        // be ignored. The requests which wait for the session should be
        // retried - the next attempt will get an up-to-date response from
        // the main tablet.
        //

        const ui64 sessionRecoveryId = recovery->Id;
        RejectPostponedSessionRequests(
            ctx,
            sessionRecoveryId,
            MakeError(E_REJECTED, TStringBuilder()
                << "session " << session.GetSessionId().Quote()
                << " is being destroyed by the main tablet"));
    }

    // the sessions are synced first, see CompleteTx_SyncShardSessions for
    // the rest
    ExecuteTx<TSyncShardSessions>(
        ctx,
        std::move(requestInfo),
        ui64(0),    // sessionRecoveryId
        std::move(sessions),
        std::move(sessionsToReset),
        std::move(sessionsToDestroy));
}

bool TIndexTabletActor::PrepareTx_SyncShardSessions(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSyncShardSessions& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_SyncShardSessions(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSyncShardSessions& args)
{
    Y_UNUSED(ctx);

    auto db = CreateIndexTabletDatabase(tx.DB);

    const auto sessionOptions = TSession::CreateSessionOptions(Config);

    for (const auto& info: args.Sessions) {
        //
        // The clients don't establish these sessions in the shards themselves
        // - the main tablet owns the sessions and the shards keep their
        // copies. So the subsessions of these copies have no owners.
        //

        const auto result = SyncSession(*db, info, sessionOptions);

        switch (result) {
            case ESyncSessionResult::Created: {
                ++args.CreatedSessionCount;
                break;
            }
            case ESyncSessionResult::Recovered: {
                ++args.RecoveredSessionCount;
                break;
            }
            case ESyncSessionResult::Skipped: {
                break;
            }
        }
    }

    //
    // ResetSession and DestroySession requests of the main tablet are
    // processed after this tx, see CompleteTx_SyncShardSessions. What such
    // a request does to a session depends on the subsessions of the session
    // (see CheckSessionForDestroy), and this tablet should do the same thing
    // as the main tablet, so it needs the same subsessions - even if there
    // are none of them left.
    //

    for (const auto* requests: {&args.SessionsToReset, &args.SessionsToDestroy})
    {
        for (const auto& request: *requests) {
            if (request.HasSession()) {
                SyncSubSessions(*db, request.GetSession());
            }
        }
    }
}

void TIndexTabletActor::CompleteTx_SyncShardSessions(
    const TActorContext& ctx,
    TTxIndexTablet::TSyncShardSessions& args)
{
    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s SyncShardSessions completed: %lu sessions, created: %u"
        ", recovered: %u, on demand: %d",
        LogTag.c_str(),
        args.Sessions.size(),
        args.CreatedSessionCount,
        args.RecoveredSessionCount,
        args.SessionRecoveryId != 0);

    if (args.SessionRecoveryId) {
        // the session has been requested by this tablet itself, see
        // HandleRecoverShardSessionsResponse
        ReplayPostponedSessionRequests(ctx, args.SessionRecoveryId);
        return;
    }

    RemoveInFlightRequest(*args.RequestInfo);

    using TResponse = TEvIndexTablet::TEvSyncShardSessionsResponse;
    auto response = std::make_unique<TResponse>(args.Error);
    response->Record.SetGeneration(GetGeneration());
    response->Record.SetCreatedSessionCount(args.CreatedSessionCount);
    response->Record.SetRecoveredSessionCount(args.RecoveredSessionCount);

    const auto opCount = static_cast<ui32>(
        args.SessionsToReset.size() + args.SessionsToDestroy.size());

    if (HasError(args.Error) || !opCount) {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    //
    // ResetSession and DestroySession requests of the main tablet are
    // processed the regular way. The sessions they refer to have the same
    // subsessions as in the main tablet now (see ExecuteTx_SyncShardSessions),
    // so this tablet makes the same checks against the same data and comes
    // to the same decisions as the main tablet. TShardSessionOpsActor waits
    // for all these requests and replies to the sender of SyncShardSessions.
    //

    auto actor = std::make_unique<TShardSessionOpsActor>(
        LogTag,
        SelfId(),
        args.RequestInfo,
        opCount,
        std::move(response->Record));

    const auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);

    ui64 cookie = 0;

    for (const auto& session: args.SessionsToDestroy) {
        auto requestInfo = CreateRequestInfo(
            actorId,
            cookie++,
            args.RequestInfo->CallContext);
        requestInfo->StartedTs = ctx.Now();

        AddInFlightRequest<TEvIndexTablet::TDestroySessionMethod>(
            *requestInfo);

        ExecuteTx<TDestroySession>(
            ctx,
            std::move(requestInfo),
            session.GetSessionId(),
            session.GetSessionSeqNo(),
            Config->GetMaxDeleteSessionHandlesPerTx(),
            false,      // isContinuation
            NProtoPrivate::TDestroySessionRequest());
    }

    for (const auto& session: args.SessionsToReset) {
        auto requestInfo = CreateRequestInfo(
            actorId,
            cookie++,
            args.RequestInfo->CallContext);
        requestInfo->StartedTs = ctx.Now();

        AddInFlightRequest<TEvService::TResetSessionMethod>(*requestInfo);

        ExecuteTx<TResetSession>(
            ctx,
            std::move(requestInfo),
            session.GetSessionId(),
            session.GetSessionSeqNo(),
            Config->GetMaxDeleteSessionHandlesPerTx(),
            false,      // isContinuation
            NProto::TResetSessionRequest());
    }
}

////////////////////////////////////////////////////////////////////////////////
// On-demand session recovery: a shard requests a session from the main tablet.

bool TIndexTabletActor::ShouldRecoverSessionsOnDemand() const
{
    return !ReplayingPostponedSessionRequests
        && Config->GetShardSessionRecoveryOnDemandEnabled()
        && !IsMainTablet()
        && !GetFileSystem().GetMainFileSystemId().empty();
}

void TIndexTabletActor::PostponeRequestUntilSessionRecovered(
    const TActorContext& ctx,
    TString clientId,
    TString sessionId,
    ui64 sessionSeqNo,
    IEventHandlePtr request,
    TPostponedSessionRequest::TRejectRoutine* reject)
{
    auto* recovery = SessionRecoveries.FindPtr(sessionId);
    if (!recovery) {
        recovery = &SessionRecoveries[sessionId];
        recovery->Id = ++LastSessionRecoveryId;
        SessionRecoveryIds[recovery->Id] = sessionId;

        Metrics->SessionRecoveriesOnDemand.fetch_add(
            1,
            std::memory_order_relaxed);

        const auto& mainFileSystemId = GetFileSystem().GetMainFileSystemId();

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s No active session c:%s, s:%s, n:%lu - requesting it from the"
            " main tablet %s, recovery id: %lu",
            LogTag.c_str(),
            clientId.Quote().c_str(),
            sessionId.Quote().c_str(),
            sessionSeqNo,
            mainFileSystemId.Quote().c_str(),
            recovery->Id);

        auto sessionsRequest =
            std::make_unique<TEvIndexTablet::TEvRecoverShardSessionsRequest>();
        sessionsRequest->Record.SetFileSystemId(mainFileSystemId);
        sessionsRequest->Record.SetShardId(GetFileSystemId());
        sessionsRequest->Record.AddSessionIds(sessionId);

        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            sessionsRequest.release(),
            {}, // flags
            recovery->Id);
    }

    TPostponedSessionRequest postponedRequest;
    postponedRequest.Request = std::move(request);
    postponedRequest.Reject = reject;
    postponedRequest.ClientId = std::move(clientId);
    postponedRequest.SessionSeqNo = sessionSeqNo;
    recovery->Requests.push_back(std::move(postponedRequest));
}

void TIndexTabletActor::HandleRecoverShardSessionsResponse(
    const TEvIndexTablet::TEvRecoverShardSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 sessionRecoveryId = ev->Cookie;
    const auto* sessionId = SessionRecoveryIds.FindPtr(sessionRecoveryId);
    if (!sessionId) {
        // the requests have already been rejected
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s Got a session for an unknown recovery, recovery id: %lu",
            LogTag.c_str(),
            sessionRecoveryId);
        return;
    }

    if (HasError(msg->GetError())) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s Failed to get session s:%s from the main tablet: %s"
            ", recovery id: %lu",
            LogTag.c_str(),
            sessionId->Quote().c_str(),
            FormatError(msg->GetError()).c_str(),
            sessionRecoveryId);

        // The main tablet is unavailable - we don't know whether the session
        // is valid, so the requests should be retried.
        RejectPostponedSessionRequests(
            ctx,
            sessionRecoveryId,
            MakeError(E_REJECTED, TStringBuilder()
                << "failed to get session from the main tablet: "
                << FormatError(msg->GetError())));
        return;
    }

    TVector<NProtoPrivate::TShardSessionInfo> sessions;
    for (auto& info: *msg->Record.MutableSessions()) {
        // a session without subsessions is not active - no one is allowed to
        // use it until it gets recovered in the main tablet
        if (info.GetSessionId() == *sessionId
                && !info.GetSubSessions().empty())
        {
            sessions.push_back(std::move(info));
            break;
        }
    }

    if (sessions.empty()) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s Session s:%s is not active in the main tablet"
            ", recovery id: %lu",
            LogTag.c_str(),
            sessionId->Quote().c_str(),
            sessionRecoveryId);

        RejectPostponedSessionRequests(
            ctx,
            sessionRecoveryId,
            {});   // invalid session
        return;
    }

    //
    // The requests which don't match the session (i.e. have other ClientId
    // or SessionSeqNo) will be rejected upon their processing.
    //

    ExecuteTx<TSyncShardSessions>(
        ctx,
        TRequestInfoPtr(),
        sessionRecoveryId,
        std::move(sessions));
}

auto TIndexTabletActor::FinishSessionRecovery(
    ui64 sessionRecoveryId,
    TString* sessionId) -> TDeque<TPostponedSessionRequest>
{
    TDeque<TPostponedSessionRequest> requests;

    auto idIt = SessionRecoveryIds.find(sessionRecoveryId);
    if (idIt == SessionRecoveryIds.end()) {
        return requests;
    }

    *sessionId = idIt->second;
    SessionRecoveryIds.erase(idIt);

    auto it = SessionRecoveries.find(*sessionId);
    if (it != SessionRecoveries.end()) {
        requests = std::move(it->second.Requests);
        SessionRecoveries.erase(it);
    }

    return requests;
}

void TIndexTabletActor::ReplayPostponedSessionRequests(
    const TActorContext& ctx,
    ui64 sessionRecoveryId)
{
    TString sessionId;
    auto requests = FinishSessionRecovery(sessionRecoveryId, &sessionId);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Session s:%s recovered, processing %lu postponed requests"
        ", recovery id: %lu",
        LogTag.c_str(),
        sessionId.Quote().c_str(),
        requests.size(),
        sessionRecoveryId);

    //
    // The requests are processed right here: in the order of their arrival
    // and before any other event is able to change the session. These requests
    // should not be postponed once again - if the session still can't be
    // found for some of them (e.g. because of ClientId or SessionSeqNo
    // mismatch), then the session is really invalid for these requests.
    //

    TABLET_VERIFY(!ReplayingPostponedSessionRequests);
    ReplayingPostponedSessionRequests = true;

    for (auto& request: requests) {
        TAutoPtr<IEventHandle> handle(request.Request.release());
        Receive(handle);
    }

    ReplayingPostponedSessionRequests = false;
}

void TIndexTabletActor::RejectPostponedSessionRequests(
    const TActorContext& ctx,
    ui64 sessionRecoveryId,
    const NProto::TError& error)
{
    TString sessionId;
    auto requests = FinishSessionRecovery(sessionRecoveryId, &sessionId);

    for (auto& request: requests) {
        request.Reject(
            ctx,
            *request.Request,
            HasError(error)
                ? error
                : ErrorInvalidSession(
                    request.ClientId,
                    sessionId,
                    request.SessionSeqNo));
    }
}

void TIndexTabletActor::RejectAllPostponedSessionRequests(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    for (auto& [sessionId, recovery]: SessionRecoveries) {
        for (auto& request: recovery.Requests) {
            request.Reject(ctx, *request.Request, error);
        }
    }

    SessionRecoveries.clear();
    SessionRecoveryIds.clear();
}

}   // namespace NCloud::NFileStore::NStorage
