#include "tablet_actor.h"

#include <cloud/storage/core/libs/common/helpers.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TSetNodeXAttrRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    if (auto error = ValidateXAttrName(request.GetName()); HasError(error)) {
        return error;
    }

    auto error = ValidateXAttrValue(request.GetName(), request.GetValue());
    if (HasError(error)) {
        return error;
    }

    return {};
}

}   // namespace
////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleSetNodeXAttr(
    const TEvService::TEvSetNodeXAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequest<TEvService::TSetNodeXAttrMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TSetNodeXAttrMethod>(*requestInfo);

    ExecuteTx<TSetNodeXAttr>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_SetNodeXAttr(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetNodeXAttr& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_SESSION(SetNodeXAttr, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

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
    // fetch previous version
    if (!ReadNodeAttr(db, args.NodeId, args.CommitId, args.Name, args.Attr)) {
        return false;   // not ready
    }

    const auto flags = args.Request.GetFlags();
    if (args.Attr && HasProtoFlag(flags, NProto::TSetNodeXAttrRequest::F_CREATE)) {
        args.Error = ErrorAttributeAlreadyExists(args.Name);
        return true;
    }
    if (!args.Attr && HasProtoFlag(flags, NProto::TSetNodeXAttrRequest::F_REPLACE)) {
        args.Error = ErrorAttributeDoesNotExist(args.Name);
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_SetNodeXAttr(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetNodeXAttr& args)
{
    FILESTORE_VALIDATE_TX_ERROR(SetNodeXAttr, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "SetXAttr");
    }

    if (args.Attr) {
        args.Version = UpdateNodeAttr(
            db,
            args.NodeId,
            args.Attr->MinCommitId,
            args.CommitId,
            *args.Attr,
            args.Value);
    } else {
        args.Version = CreateNodeAttr(
            db,
            args.NodeId,
            args.CommitId,
            args.Name,
            args.Value);
    }
}

void TIndexTabletActor::CompleteTx_SetNodeXAttr(
    const TActorContext& ctx,
    TTxIndexTablet::TSetNodeXAttr& args)
{
    UpdateInMemoryIndexState(std::move(args.NodeUpdates));
    RemoveTransaction(*args.RequestInfo);

    if (SUCCEEDED(args.Error.GetCode())) {
        NProto::TSessionEvent sessionEvent;
        {
            auto* changed = sessionEvent.AddNodeChanged();
            changed->SetNodeId(args.NodeId);
            changed->SetKind(NProto::TSessionEvent::NODE_XATTR_CHANGED);
        }
        NotifySessionEvent(ctx, sessionEvent);
    }

    auto response = std::make_unique<TEvService::TEvSetNodeXAttrResponse>(args.Error);
    response->Record.SetVersion(args.Version);
    CompleteResponse<TEvService::TSetNodeXAttrMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
