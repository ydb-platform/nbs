#include "tablet_actor.h"
#include "shard_request_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TResetShardSessionsActor = TShardRequestActor<
    TEvService::TEvResetSessionRequest,
    TEvService::TEvResetSessionResponse>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleResetSession(
    const TEvService::TEvResetSessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const auto seqNo = GetSessionSeqNo(msg->Record);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s ResetSession c:%s, s:%s n:%lu",
        LogTag.c_str(),
        clientId.c_str(),
        sessionId.c_str(),
        seqNo);

    auto* session = FindSession(sessionId);
    if (!session || session->GetClientId() != clientId) {
        auto response = std::make_unique<TEvService::TEvResetSessionResponse>(
            ErrorInvalidSession(
                clientId,
                sessionId,
                seqNo));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (ShouldSyncShardSessionRequests()) {
        //
        // The shards go first, see HandleDestroySession: the request is
        // kept until the shard sessions sync delivers it to all the shards
        // whose sessions it manages, this tablet processes it after that.
        //

        using TResponse = TEvService::TEvResetSessionResponse;

        if (PendingShardSessionRequests.contains(sessionId)) {
            auto response = std::make_unique<TResponse>(MakeError(
                E_REJECTED,
                "another ResetSession or DestroySession request for this"
                " session is in progress"));

            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        TPendingShardSessionRequest request;
        request.SessionSeqNo = seqNo;
        request.Reset = true;
        request.RequestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            msg->CallContext);
        request.RequestInfo->StartedTs = ctx.Now();

        // sessionId refers to the record which is about to be moved
        TString pendingSessionId = sessionId;
        request.ResetRequest = std::move(msg->Record);

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

    AddInFlightRequest<TEvService::TResetSessionMethod>(*requestInfo);

    ExecuteTx<TResetSession>(
        ctx,
        std::move(requestInfo),
        sessionId,
        seqNo,
        Config->GetMaxDeleteSessionHandlesPerTx(),
        false /* isContinuation */,
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_ResetSession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TResetSession& args)
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
        "%s Reset session s:%s n:%lu l:%lu",
        LogTag.c_str(),
        args.SessionId.c_str(),
        args.SessionSeqNo,
        args.Request.GetSessionState().size());

    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    return ReadNodesToRemoveForSessionHandles(
        *db,
        *session,
        args.MaxHandlesPerTx,
        args.Nodes);
}

void TIndexTabletActor::ExecuteTx_ResetSession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TResetSession& args)
{
    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        if (args.IsContinuation) {
            args.Error = MakeError(
                E_REJECTED,
                "session reset interrupted: session destroyed");
            ReportResetSessionInterrupted(TStringBuilder()
                << LogTag << " ResetSession s:" << args.SessionId
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
                "session reset interrupted: session recovered");
            ReportResetSessionInterrupted(TStringBuilder()
                << LogTag << " ResetSession s:" << args.SessionId
                << " n:" << args.SessionSeqNo
                << " interrupted: session recovered");
        }
        args.Completed = true;
        return;
    }

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        args.Completed = true;
        return;
    }

    DestroySessionHandlesAndRemoveNodes(
        *db,
        args.FileSystemEvents,
        ctx,
        session,
        commitId,
        args.MaxHandlesPerTx,
        args.IsContinuation,
        args.Nodes,
        "session reset");

    if (session->Handles.Empty()) {
        ResetSession(*db, session, args.Request.GetSessionState());
        args.Completed = true;
    }

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_ResetSession(
    const TActorContext& ctx,
    TTxIndexTablet::TResetSession& args)
{
    if (!args.Completed) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s Reset session s:%s n:%lu continues in the next tx",
            LogTag.c_str(),
            args.SessionId.c_str(),
            args.SessionSeqNo);

        ExecuteTx<TResetSession>(
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
    // HandleResetSession
    OnShardSessionRequestCompleted(ctx, args.SessionId, args.RequestInfo);

    auto response =
        std::make_unique<TEvService::TEvResetSessionResponse>(args.Error);

    if (HasError(args.Error)) {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    // session will be reset in other shards via the code in the main tablet
    if (!IsMainTablet() || shardIds.empty()) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s ResetSession completed",
            LogTag.c_str());

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    TVector<TString> targetShardIds(shardIds.begin(), shardIds.end());

    if (Config->GetLazyShardSessionCreationEnabled()) {
        //
        // The shards whose sessions are managed by the shard sessions sync
        // have already processed this request, see HandleResetSession. The
        // shards which work in adapter mode are the exception: their
        // sessions are managed the old way.
        //

        targetShardIds = GetAdapterModeShardIds();

        if (targetShardIds.empty()) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s ResetSession completed",
                LogTag.c_str());

            NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
            return;
        }
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s ResetSession completed - local"
        ", resetting shard sessions (%s)",
        LogTag.c_str(),
        JoinSeq(",", targetShardIds).c_str());

    auto actor = std::make_unique<TResetShardSessionsActor>(
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
