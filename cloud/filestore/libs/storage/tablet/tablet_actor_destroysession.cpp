#include "tablet_actor.h"
#include "shard_request_actor.h"

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

    AddTransaction<TEvIndexTablet::TDestroySessionMethod>(*requestInfo);

    ExecuteTx<TDestroySession>(
        ctx,
        std::move(requestInfo),
        sessionId,
        sessionSeqNo,
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

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    bool ready = true;
    auto commitId = GetCurrentCommitId();
    args.Nodes.reserve(session->Handles.Size());
    for (const auto& handle: session->Handles) {
        if (args.Nodes.contains(handle.GetNodeId())) {
            continue;
        }

        TMaybe<IIndexTabletDatabase::TNode> node;
        if (!ReadNode(db, handle.GetNodeId(), commitId, node)) {
            ready = false;
        } else {
            TABLET_VERIFY(node);
            if (node->Attrs.GetLinks() == 0) {
                // candidate to be removed
                args.Nodes.insert(*node);
            }
        }
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_DestroySession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroySession& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        return;
    }

    if (!CheckSessionForDestroy(session, args.SessionSeqNo) &&
        session->DeleteSubSession(args.SessionSeqNo))
    {
        db.WriteSession(*session);
        return;
    }

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "DestroySession");
    }

    auto handle = session->Handles.begin();
    while (handle != session->Handles.end()) {
        auto nodeId = handle->GetNodeId();
        DestroyHandle(db, &*(handle++));

        auto it = args.Nodes.find(nodeId);
        if (it != args.Nodes.end() && !HasOpenHandles(nodeId)) {
            auto e = RemoveNode(
                db,
                *it,
                it->MinCommitId,
                commitId);

            if (HasError(e)) {
                WriteOrphanNode(db, TStringBuilder()
                    << "DestroySession: " << args.SessionId
                    << ", RemoveNode: " << nodeId
                    << ", Error: " << FormatError(e), nodeId);
            }
        }
    }

    RemoveSession(db, args.SessionId);

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_DestroySession(
    const TActorContext& ctx,
    TTxIndexTablet::TDestroySession& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response =
        std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>();

    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    if (shardIds.empty()) {
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
        std::move(args.RequestInfo),
        std::move(args.Request),
        TVector<TString>(shardIds.begin(), shardIds.end()),
        std::move(response));

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
