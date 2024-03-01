#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TReadLinkRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleReadLink(
    const TEvService::TEvReadLinkRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequest<TEvService::TReadLinkMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TReadLinkMethod>(*requestInfo);

    ExecuteTx<TReadLink>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_ReadLink(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TReadLink& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_SESSION(ReadLink, args);

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GetCurrentCommitId();

    // validate parent node exists
    if (!ReadNode(db, args.NodeId, args.CommitId, args.Node)) {
        return false;   // not ready
    }

    if (!args.Node || args.Node->Attrs.GetType() != NProto::E_LINK_NODE) {
        args.Error = ErrorInvalidTarget(args.NodeId);
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.Node);

    return true;
}

void TIndexTabletActor::ExecuteTx_ReadLink(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TReadLink& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TIndexTabletActor::CompleteTx_ReadLink(
    const TActorContext& ctx,
    TTxIndexTablet::TReadLink& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvReadLinkResponse>(args.Error);
    if (SUCCEEDED(args.Error.GetCode())) {
        response->Record.SetSymLink(args.Node->Attrs.GetSymLink());
    }

    CompleteResponse<TEvService::TReadLinkMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
