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

    auto response =
        std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>(args.Error);

    if (HasError(args.Error)) {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    UnregisterSessionByPipeServer(args.SessionId);
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

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s DestroySession completed - local"
        ", destroying shard sessions (%s)",
        LogTag.c_str(),
        JoinSeq(",", GetFileSystem().GetShardFileSystemIds()).c_str());

    auto actor = std::make_unique<TDestroyShardSessionsActor>(
        LogTag,
        SelfId(),
        std::move(args.RequestInfo),
        std::move(args.Request),
        TVector<TString>(shardIds.begin(), shardIds.end()),
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
