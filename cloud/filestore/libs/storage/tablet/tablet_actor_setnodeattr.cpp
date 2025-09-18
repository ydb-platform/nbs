#include "tablet_actor.h"

#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(
    const NProto::TSetNodeAttrRequest& request,
    ui32 blockSize,
    ui32 maxFileBlocks)
{
    if (request.GetNodeId() == InvalidNodeId || request.GetFlags() == 0) {
        return ErrorInvalidArgument();
    }

    const auto setSize = NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE;
    if (HasFlag(request.GetFlags(), setSize) &&
        request.GetUpdate().GetSize() > 0)
    {
        TByteRange range(0, request.GetUpdate().GetSize(), blockSize);
        if (range.BlockCount() > maxFileBlocks) {
            return ErrorFileTooBig();
        }
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleSetNodeAttr(
    const TEvService::TEvSetNodeAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto flags = msg->Record.GetFlags();
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) {
        if (auto error = IsDataOperationAllowed(); HasError(error)) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvSetNodeAttrResponse>(
                    std::move(error)));

            return;
        }
    }

    auto validator = [&] (const NProto::TSetNodeAttrRequest& request) {
        return ValidateRequest(
            request,
            GetBlockSize(),
            Config->GetMaxFileBlocks());
    };

    if (!AcceptRequest<TEvService::TSetNodeAttrMethod>(ev, ctx, validator)) {
        return;
    }
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvService::TSetNodeAttrMethod>(*requestInfo);

    ExecuteTx<TSetNodeAttr>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_SetNodeAttr(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetNodeAttr& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_SESSION(SetNodeAttr, args);

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

    auto flags = args.Request.GetFlags();
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) {
        const auto& update = args.Request.GetUpdate();
        if (!HasSpaceLeft(args.Node->Attrs.GetSize(), update.GetSize())) {
            args.Error = ErrorNoSpaceLeft();
            return true;
        }
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.Node);

    return true;
}

void TIndexTabletActor::ExecuteTx_SetNodeAttr(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetNodeAttr& args)
{
    FILESTORE_VALIDATE_TX_ERROR(SetNodeAttr, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "SetAttr");
    }

    auto flags = args.Request.GetFlags();
    const auto& update = args.Request.GetUpdate();

    ECopyAttrsMode mode;
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) {
        // upon size change e.g. truncate we should also update mtime
        mode = E_CM_CMTIME;
    } else {
        // other attributes trigger only ctime change
        mode = E_CM_CTIME;
    }

    NProto::TNode attrs = CopyAttrs(args.Node->Attrs, mode);
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MODE)) {
        attrs.SetMode(update.GetMode());
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_UID)) {
        attrs.SetUid(update.GetUid());
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_GID)) {
        attrs.SetGid(update.GetGid());
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_ATIME)) {
        attrs.SetATime(update.GetATime());
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MTIME)) {
        attrs.SetMTime(update.GetMTime());
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_CTIME)) {
        attrs.SetCTime(update.GetCTime());
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) {
        auto e = Truncate(
            db,
            args.NodeId,
            args.CommitId,
            attrs.GetSize(),
            update.GetSize());

        if (HasError(e)) {
            args.Error = e;
            return;
        }

        attrs.SetSize(update.GetSize());
    }

    UpdateNode(
        db,
        args.NodeId,
        args.Node->MinCommitId,
        args.CommitId,
        attrs,
        args.Node->Attrs);

    args.Node->Attrs = std::move(attrs);

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_SetNodeAttr(
    const TActorContext& ctx,
    TTxIndexTablet::TSetNodeAttr& args)
{
    InvalidateNodeCaches(args.NodeId);

    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvSetNodeAttrResponse>(args.Error);
    if (SUCCEEDED(args.Error.GetCode())) {
        TABLET_VERIFY(args.Node);

        auto* node = response->Record.MutableNode();
        ConvertNodeFromAttrs(*node, args.NodeId, args.Node->Attrs);

        NProto::TSessionEvent sessionEvent;
        {
            auto* changed = sessionEvent.AddNodeChanged();
            changed->SetNodeId(args.NodeId);
            changed->SetKind(NProto::TSessionEvent::NODE_ATTR_CHANGED);
        }
        NotifySessionEvent(ctx, sessionEvent);
    }

    CompleteResponse<TEvService::TSetNodeAttrMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    EnqueueBlobIndexOpIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
