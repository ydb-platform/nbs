#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TGetNodeXAttrRequest& request)
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

void TIndexTabletActor::HandleGetNodeXAttr(
    const TEvService::TEvGetNodeXAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    RegisterFileSystemEventClient(ev->Recipient, ev->Sender);

    if (!AcceptRequest<TEvService::TGetNodeXAttrMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();
    requestInfo->PipeServerId = ev->Recipient;

    AddInFlightRequest<TEvService::TGetNodeXAttrMethod>(*requestInfo);

    ExecuteTx<TGetNodeXAttr>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_GetNodeXAttr(
    const TActorContext& ctx,
    TTxIndexTablet::TGetNodeXAttr& args)
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
        return false;
    }

    args.CommitId = GetReadCommitId(session->GetCheckpointId());
    if (args.CommitId == InvalidCommitId) {
        args.Error = ErrorInvalidCheckpoint(session->GetCheckpointId());
        return false;
    }

    return true;
}

bool TIndexTabletActor::PrepareTx_GetNodeXAttr(
    const TActorContext& ctx,
    INodeIndexTabletDatabase& db,
    TTxIndexTablet::TGetNodeXAttr& args)
{
    Y_UNUSED(ctx);

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
    // validate target attribute exists
    if (!ReadNodeAttr(db, args.NodeId, args.CommitId, args.Name, args.Attr)) {
        return false;   // not ready
    }

    if (!args.Attr) {
        args.Error = ErrorAttributeDoesNotExist(args.Name);
        return true;
    }

    return true;
}

void TIndexTabletActor::CompleteTx_GetNodeXAttr(
    const TActorContext& ctx,
    TTxIndexTablet::TGetNodeXAttr& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    //
    // The client may cache the attribute value as well as its absence.
    //

    if (!HasError(args.Error) ||
        STATUS_FROM_CODE(args.Error.GetCode()) == NProto::E_FS_NOXATTR)
    {
        AddFileSystemEventNodeXAttrSubscriber(
            ctx,
            args.RequestInfo->PipeServerId,
            args.NodeId,
            args.Name);
    }

    auto response = std::make_unique<TEvService::TEvGetNodeXAttrResponse>(args.Error);
    if (!HasError(args.Error)) {
        TABLET_VERIFY(args.Attr);
        response->Record.SetValue(args.Attr->Value);
        response->Record.SetVersion(args.Attr->Version);
    }

    CompleteResponse<TEvService::TGetNodeXAttrMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);
    Metrics->GetNodeXAttr.Update(1, 0, ctx.Now() - args.RequestInfo->StartedTs);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
