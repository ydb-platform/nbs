#include "tablet_actor.h"
#include "shard_request_actor.h"

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

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TResetSessionMethod>(*requestInfo);

    ExecuteTx<TResetSession>(
        ctx,
        std::move(requestInfo),
        sessionId,
        seqNo,
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

    TIndexTabletDatabase db(tx.DB);

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

void TIndexTabletActor::ExecuteTx_ResetSession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TResetSession& args)
{
    TIndexTabletDatabase db(tx.DB);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        return;
    }

    if (!CheckSessionForDestroy(session, args.SessionSeqNo)) {
        return;
    }

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "ResetSession");
    }

    auto handle = session->Handles.begin();
    while (handle != session->Handles.end()) {
        auto nodeId = handle->GetNodeId();
        DestroyHandle(db, &*(handle++));

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s Removing handle upon session reset s:%s n:%lu",
            LogTag.c_str(),
            args.SessionId.c_str(),
            nodeId);

        auto it = args.Nodes.find(nodeId);
        if (it != args.Nodes.end() && !HasOpenHandles(nodeId)) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s Removing node upon session reset s:%s n:%lu (size %lu)",
                LogTag.c_str(),
                args.SessionId.c_str(),
                nodeId,
                it->Attrs.GetSize());

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

    ResetSession(db, session, args.Request.GetSessionState());

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_ResetSession(
    const TActorContext& ctx,
    TTxIndexTablet::TResetSession& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvResetSessionResponse>();

    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    if (shardIds.empty()) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s ResetSession completed",
            LogTag.c_str());

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s ResetSession completed - local"
        ", resetting shard sessions (%s)",
        LogTag.c_str(),
        JoinSeq(",", GetFileSystem().GetShardFileSystemIds()).c_str());

    auto actor = std::make_unique<TResetShardSessionsActor>(
        LogTag,
        std::move(args.RequestInfo),
        std::move(args.Request),
        TVector<TString>(shardIds.begin(), shardIds.end()),
        std::move(response));

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
