#include "tablet_actor.h"
#include "shard_request_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TDestroyShardSessionsActor = TShardRequestActor<
    TEvIndexTablet::TEvDestroySessionRequest,
    TEvIndexTablet::TEvDestroySessionResponse>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDestroySession(
    const TEvIndexTablet::TEvDestroySessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>(
                std::move(error)));

        return;
    }

    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const auto sessionSeqNo = GetSessionSeqNo(msg->Record);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s DestroySession c:%s, s:%s n:%lu",
        LogTag.c_str(),
        clientId.c_str(),
        sessionId.c_str(),
        sessionSeqNo);

    auto* session = FindSession(sessionId);
    if (!session) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>();

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (session->GetClientId() != clientId) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>(
                ErrorInvalidSession(clientId, sessionId, sessionSeqNo));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (ShouldSyncShardSessionRequests()) {
        //
        // The shards go first: the request is kept until the shard sessions
        // sync delivers it to all the shards whose sessions it manages -
        // each of them processes the request right after getting the
        // up-to-date subsessions of this session. The session is not touched
        // here until then, see ProcessPendingShardSessionRequests. So if
        // the request can't be delivered to some of the shards, then this
        // tablet neither processes nor acknowledges it, and the next sync
        // round makes one more attempt.
        //

        using TResponse = TEvIndexTablet::TEvDestroySessionResponse;

        // see TPendingShardSessionRequest::RequestId
        const ui64 requestId = msg->Record.GetHeaders().GetRequestId();

        auto requestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            msg->CallContext);
        requestInfo->StartedTs = ctx.Now();

        if (auto* pending = PendingShardSessionRequests.FindPtr(sessionId)) {
            const bool retry = requestId
                && pending->Type == EShardSessionRequestType::Destroy
                && pending->RequestId == requestId
                && pending->SessionSeqNo == sessionSeqNo
                && !pending->Applying;

            if (!retry) {
                auto response = std::make_unique<TResponse>(MakeError(
                    E_REJECTED,
                    "another CreateSession, ResetSession or DestroySession"
                    " request for this session is in progress"));

                NCloud::Reply(ctx, *ev, std::move(response));
                return;
            }

            //
            // A retry of the request which is waiting for the shards, see
            // HandleResetSession: it takes the place of its previous
            // attempt and doesn't start over.
            //

            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s DestroySession s:%s n:%lu is a retry of request %lu"
                ", it keeps waiting for the shard sessions sync"
                ", version: %lu",
                LogTag.c_str(),
                sessionId.Quote().c_str(),
                sessionSeqNo,
                requestId,
                pending->Version);

            ReplyToReplacedShardSessionRequest(
                ctx,
                *pending,
                *requestInfo,
                MakeError(
                    E_REJECTED,
                    "replaced by a retry of the same request"));

            pending->RequestInfo = std::move(requestInfo);
            // sessionId refers to this record: it's not used below
            pending->DestroyRequest = std::move(msg->Record);
            return;
        }

        if (requestId
                && session->CompletedDestroyRequestId == requestId
                && GetAdapterModeShardIds().empty())
        {
            //
            // A retry of a request which this tablet and all the shards
            // have completed. The session is still here, so the request
            // has destroyed just a subsession - and the client might have
            // established it anew since then. A retry which finds no
            // session at all succeeds above.
            //

            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s DestroySession s:%s n:%lu is a retry of request %lu"
                " which has been completed",
                LogTag.c_str(),
                sessionId.Quote().c_str(),
                sessionSeqNo,
                requestId);

            NCloud::Reply(ctx, *ev, std::make_unique<TResponse>());
            return;
        }

        TPendingShardSessionRequest request;
        request.Type = EShardSessionRequestType::Destroy;
        request.RequestId = requestId;
        request.SessionSeqNo = sessionSeqNo;
        request.RequestInfo = std::move(requestInfo);

        // sessionId refers to the record which is about to be moved
        TString pendingSessionId = sessionId;
        request.DestroyRequest = std::move(msg->Record);

        EnqueueShardSessionRequest(
            ctx,
            std::move(pendingSessionId),
            std::move(request));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TEvIndexTablet::TDestroySessionMethod>(*requestInfo);

    ExecuteTx<TDestroySession>(
        ctx,
        std::move(requestInfo),
        sessionId,
        sessionSeqNo,
        Config->GetMaxDeleteSessionHandlesPerTx(),
        false /* isContinuation */,
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DestroySession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroySession& args)
{
    Y_UNUSED(ctx);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        return true;
    }

    if (!CheckSessionForDestroy(session, args.SessionSeqNo)) {
        return true;
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Wipe session s:%s n:%lu",
        LogTag.c_str(),
        args.SessionId.c_str(),
        args.SessionSeqNo);

    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    return ReadNodesToRemoveForSessionHandles(
        *db,
        *session,
        args.MaxHandlesPerTx,
        args.Nodes);
}

void TIndexTabletActor::ExecuteTx_DestroySession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroySession& args)
{
    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        if (args.IsContinuation) {
            args.Error = MakeError(
                E_REJECTED,
                "session destroy interrupted: session destroyed");
            ReportDestroySessionInterrupted(TStringBuilder()
                << LogTag << " DestroySession s:" << args.SessionId
                << " n:" << args.SessionSeqNo
                << " interrupted: session destroyed");
        }
        args.Completed = true;
        return;
    }

    if (!CheckSessionForDestroy(session, args.SessionSeqNo)) {
        if (args.IsContinuation) {
            args.Error = MakeError(
                E_REJECTED,
                "session destroy interrupted: session recovered");
            ReportDestroySessionInterrupted(TStringBuilder()
                << LogTag << " DestroySession s:" << args.SessionId
                << " n:" << args.SessionSeqNo
                << " interrupted: session recovered");
            args.Completed = true;
            return;
        }

        if (session->DeleteSubSession(args.SessionSeqNo)) {
            db->WriteSession(*session);
            args.Completed = true;
            return;
        }
    }

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        args.Completed = true;
        return;
    }

    DestroySessionHandlesAndRemoveNodes(
        *db,
        args.FileSystemEvents,
        ctx,
        session,
        args.CommitId,
        args.MaxHandlesPerTx,
        args.IsContinuation,
        args.Nodes,
        "session destroy");

    if (session->Handles.Empty()) {
        RemoveSession(*db, args.SessionId);
        args.Completed = true;
    }

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_DestroySession(
    const TActorContext& ctx,
    TTxIndexTablet::TDestroySession& args)
{
    if (!args.Completed) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s Destroy session s:%s n:%lu continues in the next tx",
            LogTag.c_str(),
            args.SessionId.c_str(),
            args.SessionSeqNo);

        ExecuteTx<TDestroySession>(
            ctx,
            std::move(args.RequestInfo),
            args.SessionId,
            args.SessionSeqNo,
            args.MaxHandlesPerTx,
            true /* isContinuation */,
            std::move(args.Request));
        return;
    }

    RemoveInFlightRequest(*args.RequestInfo);

    // the request might have been waiting for the shards, see
    // HandleDestroySession
    OnShardSessionRequestCompleted(
        ctx,
        args.SessionId,
        args.RequestInfo,
        args.Error);

    auto response =
        std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>(args.Error);

    if (HasError(args.Error)) {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    DeleteUnconfirmedDataForSession(args.SessionId, ctx);

    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    // session will be deleted in other shards via the code in the main tablet
    if (!IsMainTablet() || shardIds.empty()) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s DestroySession completed",
            LogTag.c_str());

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    TVector<TString> targetShardIds(shardIds.begin(), shardIds.end());

    if (Config->GetLazyShardSessionCreationEnabled()) {
        //
        // The shards whose sessions are managed by the shard sessions sync
        // have already processed this request, see HandleDestroySession. The
        // shards which work in adapter mode are the exception: their
        // sessions are managed the old way.
        //

        targetShardIds = GetAdapterModeShardIds();

        if (targetShardIds.empty()) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s DestroySession completed",
                LogTag.c_str());

            NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
            return;
        }
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s DestroySession completed - local"
        ", destroying shard sessions (%s)",
        LogTag.c_str(),
        JoinSeq(",", targetShardIds).c_str());

    auto actor = std::make_unique<TDestroyShardSessionsActor>(
        LogTag,
        SelfId(),
        std::move(args.RequestInfo),
        std::move(args.Request),
        std::move(targetShardIds),
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
