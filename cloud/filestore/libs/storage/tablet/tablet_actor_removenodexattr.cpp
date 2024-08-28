#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TRemoveNodeXAttrRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    if (auto error = ValidateXAttrName(request.GetName()); HasError(error)) {
        return error;
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleRemoveNodeXAttr(
    const TEvService::TEvRemoveNodeXAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequest<TEvService::TRemoveNodeXAttrMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TRemoveNodeXAttrMethod>(*requestInfo);

    ExecuteTx<TRemoveNodeXAttr>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_RemoveNodeXAttr(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRemoveNodeXAttr& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_SESSION(RemoveNodeXAttr, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    // parse path
    if (!ReadNode(db, args.NodeId, args.CommitId, args.Node)) {
        return false;   // not ready
    }

    if (!args.Node) {
        args.Error = ErrorInvalidTarget(args.NodeId);
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.Node);
    if (!ReadNodeAttr(db, args.NodeId, args.CommitId, args.Name, args.Attr)) {
        return false;   // not ready
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_RemoveNodeXAttr(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRemoveNodeXAttr& args)
{
    FILESTORE_VALIDATE_TX_ERROR(RemoveNodeXAttr, args);

    if (!args.Attr) {
        args.Error = ErrorAttributeDoesNotExist(args.Name);
        return;
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "RemoveXAttr");
    }

    RemoveNodeAttr(
        db,
        args.NodeId,
        args.Attr->MinCommitId,
        args.CommitId,
        *args.Attr);
}

void TIndexTabletActor::CompleteTx_RemoveNodeXAttr(
    const TActorContext& ctx,
    TTxIndexTablet::TRemoveNodeXAttr& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvRemoveNodeXAttrResponse>(args.Error);
    CompleteResponse<TEvService::TRemoveNodeXAttrMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
