#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TAccessNodeRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidTarget(request.GetNodeId());
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAccessNode(
    const TEvService::TEvAccessNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequest<TEvService::TAccessNodeMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo<TEvService::TAccessNodeMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction(*requestInfo);

    ExecuteTx<TAccessNode>(
        ctx,
        requestInfo,
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AccessNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAccessNode& args)
{
    Y_UNUSED(ctx);

    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);

    if (!session) {
        args.Error = ErrorInvalidSession(
            args.ClientId,
            args.SessionId,
            args.SessionSeqNo);
        return true;
    }

    args.CommitId = GetReadCommitId(session->GetCheckpointId());
    if (args.CommitId == InvalidCommitId) {
        args.Error = ErrorInvalidCheckpoint(session->GetCheckpointId());
        return true;
    }

    TIndexTabletDatabase db(tx.DB);

    // validate target node exists
    if (!ReadNode(db, args.NodeId, args.CommitId, args.Node)) {
        return false;   // not ready
    }

    if (!args.Node) {
        args.Error = ErrorInvalidTarget(args.NodeId);
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.Node);

    return true;
}

void TIndexTabletActor::ExecuteTx_AccessNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAccessNode& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TIndexTabletActor::CompleteTx_AccessNode(
    const TActorContext& ctx,
    TTxIndexTablet::TAccessNode& args)
{
    auto response = std::make_unique<TEvService::TEvAccessNodeResponse>(args.Error);
    CompleteResponse<TEvService::TAccessNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    RemoveTransaction(*args.RequestInfo);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
