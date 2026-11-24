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

ui64 TIndexTabletActor::GetShardSessionsSyncedVersion() const
{
    //
    // A shard has processed a request if its sessions have been synced by a
    // round which was started after the arrival of the request: such a round
    // delivers the request to the shard. The shards which work in adapter
    // mode don't take part in it: this tablet sends the request to them
    // itself - after the rest of the shards (CreateSession) or after its own
    // tx (ResetSession and DestroySession).
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

    return syncedVersion;
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

    // no need to wait for the regular sync
    ScheduleOutOfScheduleSyncSessions(ctx);
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
    delta.SessionsToSync = BuildShardSessionsDelta(fromVersion, toVersion);

    // A session which has a pending request is not synced on its own, see
    // AddPendingShardSessionRequests: it gets to the shards together with
    // the request.
    EraseIf(
        delta.SessionsToSync,
        [&] (const NProtoPrivate::TShardSessionInfo& info) {
            const auto& sessionId = info.GetSessionId();
            return PendingShardSessionRequests.contains(sessionId);
        });

    //
    // The shard hasn't been asked for its sessions, so it's unknown whether
    // it has the sessions which the requests are for. Each request which the
    // shard hasn't seen is sent to it: CreateSession makes the shard create
    // or recover the session, ResetSession and DestroySession for a session
    // which the shard doesn't have do nothing there.
    //

    for (const auto& [sessionId, request]: PendingShardSessionRequests) {
        if (request.Applying
                || request.Version <= fromVersion
                || request.Version > toVersion)
        {
            continue;
        }

        switch (request.Type) {
            case EShardSessionRequestType::Create: {
                auto info = BuildCreatedShardSessionInfo(sessionId, request);
                if (info) {
                    delta.SessionsToSync.push_back(std::move(*info));
                }
                break;
            }
            case EShardSessionRequestType::Reset: {
                delta.SessionsToReset.push_back(
                    BuildShardSessionRequest(sessionId, request));
                break;
            }
            case EShardSessionRequestType::Destroy: {
                delta.SessionsToDestroy.push_back(
                    BuildShardSessionRequest(sessionId, request));
                break;
            }
        }
    }

    return SyncSessionsDeltas.emplace(fromVersion, std::move(delta))
        .first->second;
}

////////////////////////////////////////////////////////////////////////////////
// CreateSession, ResetSession and DestroySession requests of the clients:
// none of them is acknowledged until the shards process it. CreateSession is
// processed by this tablet first, ResetSession and DestroySession - last.

void TIndexTabletActor::EnqueueShardSessionRequest(
    const TActorContext& ctx,
    TString sessionId,
    TPendingShardSessionRequest request)
{
    request.Version = ++ShardSessionsVersion;

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s %s s:%s n:%lu waits for the shard sessions sync, version: %lu",
        LogTag.c_str(),
        request.GetName(),
        sessionId.Quote().c_str(),
        request.SessionSeqNo,
        request.Version);

    if (request.Type == EShardSessionRequestType::Create) {
        // the request is what delivers the session to the shards
        if (auto* session = FindSession(sessionId)) {
            session->ShardSyncVersion = request.Version;
        }
    }

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

TMaybe<NProtoPrivate::TShardSessionInfo>
TIndexTabletActor::BuildCreatedShardSessionInfo(
    const TString& sessionId,
    const TPendingShardSessionRequest& request) const
{
    //
    // The shard creates the session if it doesn't have it and makes its copy
    // active with the same subsessions otherwise. A session which has been
    // recovered by the request is a special case: the shard might keep
    // something that its previous incarnation hasn't finished, and the
    // client shouldn't get the session until that's dropped.
    //

    auto info = BuildShardSessionInfo(sessionId);
    if (info && request.Interrupted) {
        info->SetInterrupted(true);
    }

    return info;
}

void TIndexTabletActor::ReplyToPendingShardSessionRequest(
    const TActorContext& ctx,
    const TPendingShardSessionRequest& request,
    const NProto::TError& error)
{
    switch (request.Type) {
        case EShardSessionRequestType::Create: {
            using TResponse = TEvIndexTablet::TEvCreateSessionResponse;
            NCloud::Reply(
                ctx,
                *request.RequestInfo,
                std::make_unique<TResponse>(error));
            break;
        }
        case EShardSessionRequestType::Reset: {
            using TResponse = TEvService::TEvResetSessionResponse;
            NCloud::Reply(
                ctx,
                *request.RequestInfo,
                std::make_unique<TResponse>(error));
            break;
        }
        case EShardSessionRequestType::Destroy: {
            using TResponse = TEvIndexTablet::TEvDestroySessionResponse;
            NCloud::Reply(
                ctx,
                *request.RequestInfo,
                std::make_unique<TResponse>(error));
            break;
        }
    }
}

void TIndexTabletActor::ReplyToReplacedShardSessionRequest(
    const TActorContext& ctx,
    const TPendingShardSessionRequest& request,
    const TRequestInfo& newRequestInfo,
    const NProto::TError& error)
{
    if (request.RequestInfo->Sender == newRequestInfo.Sender
            && request.RequestInfo->Cookie == newRequestInfo.Cookie)
    {
        //
        // The requester has repeated the request itself: that's what the
        // session actor of the service does when it reconnects to this
        // tablet. Such a requester doesn't tell the responses to its
        // attempts from each other - a response to the previous attempt
        // would look like the response to the new one. So the previous
        // attempt is just forgotten.
        //
        return;
    }

    ReplyToPendingShardSessionRequest(ctx, request, error);
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
    // CreateSession requests which this shard hasn't got yet. The session
    // is sent whatever the shard has: the comparison of the sessions doesn't
    // tell everything - a session which has been recovered here may look
    // exactly the same in the shard, but the shard still has to know about
    // it, see BuildCreatedShardSessionInfo.
    //

    THashSet<TString> createdSessionIds;
    for (const auto& [sessionId, request]: PendingShardSessionRequests) {
        if (request.Type == EShardSessionRequestType::Create
                && request.Version <= syncVersion
                && !(syncState && request.Version <= syncState->Version))
        {
            createdSessionIds.insert(sessionId);
        }
    }

    //
    // A session which is being reset or destroyed is not synced on its own.
    // The shards which don't have it don't get it: there's nothing to reset
    // or to destroy there. The shards which have it get its subsessions
    // only together with the request.
    //

    EraseIf(
        diff.SessionsToSync,
        [&] (const NProtoPrivate::TShardSessionInfo& info) {
            const auto& sessionId = info.GetSessionId();
            if (createdSessionIds.contains(sessionId)) {
                // it's added below - the way the request describes it
                return true;
            }

            const auto* request =
                PendingShardSessionRequests.FindPtr(sessionId);
            return request
                && request->Type != EShardSessionRequestType::Create;
        });

    for (const auto& sessionId: createdSessionIds) {
        const auto* request = PendingShardSessionRequests.FindPtr(sessionId);
        if (!request) {
            continue;
        }

        if (auto info = BuildCreatedShardSessionInfo(sessionId, *request)) {
            diff.SessionsToSync.push_back(std::move(*info));
        }
    }

    THashSet<TString> processedSessionIds;

    auto addRequest = [&] (const TString& sessionId, bool recovering) {
        const auto* request = PendingShardSessionRequests.FindPtr(sessionId);
        if (!request || request->Type == EShardSessionRequestType::Create) {
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

        const bool reset = request->Type == EShardSessionRequestType::Reset;

        if (recovering && reset) {
            // the shard is about to get a new copy of the session - there's
            // nothing to reset there
            return;
        }

        if (!processedSessionIds.insert(sessionId).second) {
            return;
        }

        auto shardRequest = BuildShardSessionRequest(sessionId, *request);
        if (reset) {
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

    const ui64 syncedVersion = GetShardSessionsSyncedVersion();

    // a tx may complete (and remove its request) right away
    TVector<TString> sessionIds;
    for (const auto& [sessionId, request]: PendingShardSessionRequests) {
        if (!request.Applying && request.Version <= syncedVersion) {
            sessionIds.push_back(sessionId);
        }
    }

    //
    // The rest of the requests keep waiting, there's no deadline for them.
    // A request has to reach each shard only once: a shard which has been
    // synced by a round is not waited for anymore, whatever happens to that
    // shard in the next rounds. So a request makes progress even if each
    // round fails for some of the shards. Failing it upon a deadline would
    // throw this progress away: the retry of the client would have to start
    // over with all the shards - and that's something which might never
    // succeed if there are lots of shards. So a shard which is unavailable
    // blocks the requests until it's back: the rounds keep trying to reach
    // it, see HandleSyncSessionsCompleted. A client which doesn't want to
    // wait that long may retry the request at any moment: the retry takes
    // the place of the request and keeps what has been done for it, see
    // TPendingShardSessionRequest::RequestId.
    //

    for (const auto& sessionId: sessionIds) {
        auto* request = PendingShardSessionRequests.FindPtr(sessionId);
        if (!request || request->Applying) {
            continue;
        }

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s %s s:%s n:%lu is not waiting for the shards anymore"
            ", version: %lu",
            LogTag.c_str(),
            request->GetName(),
            sessionId.Quote().c_str(),
            request->SessionSeqNo,
            request->Version);

        if (request->Type == EShardSessionRequestType::Create) {
            // this tablet has already processed the request - the response
            // was waiting for the shards
            auto completedRequest = std::move(*request);
            PendingShardSessionRequests.erase(sessionId);

            CompletePendingCreateSession(
                ctx,
                sessionId,
                std::move(completedRequest));
            continue;
        }

        request->Applying = true;

        auto requestInfo = request->RequestInfo;
        const ui64 sessionSeqNo = request->SessionSeqNo;

        if (request->Type == EShardSessionRequestType::Reset) {
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
    const TRequestInfoPtr& requestInfo,
    const NProto::TError& error)
{
    auto it = PendingShardSessionRequests.find(sessionId);
    if (it == PendingShardSessionRequests.end()
            || it->second.RequestInfo != requestInfo)
    {
        // the request didn't have to wait for the shards
        return;
    }

    const auto& request = it->second;

    // A retry of this request shouldn't be processed once again, see
    // HandleResetSession and HandleDestroySession. There's nothing to
    // remember if the request has destroyed the session: a retry finds no
    // session and succeeds as well.
    auto* session = FindSession(sessionId);
    if (session && !HasError(error)) {
        if (request.Type == EShardSessionRequestType::Reset) {
            session->CompletedResetRequestId = request.RequestId;
        } else {
            session->CompletedDestroyRequestId = request.RequestId;
        }
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

        ReplyToPendingShardSessionRequest(ctx, request, error);

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
        if (request && request->Type == EShardSessionRequestType::Destroy) {
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

    //
    // The main tablet has recovered these sessions upon CreateSession
    // requests, so their clients have started anew. This tablet does what
    // it does when it recovers a session upon a CreateSession request
    // itself (see CompleteTx_CreateSession): an unconfirmed write of the
    // previous incarnation of a session shouldn't be applied after this
    // moment - the new incarnation may be already overwriting that range.
    // The cleanup is started before the response is sent, so no later write
    // gets ahead of it, and the main tablet lets the client use the session
    // only after the response. It doesn't depend on what the tx has done to
    // the session: the data is found by the session id.
    //

    for (const auto& info: args.Sessions) {
        if (info.GetInterrupted()) {
            DeleteUnconfirmedDataForSession(info.GetSessionId(), ctx);
        }
    }

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
