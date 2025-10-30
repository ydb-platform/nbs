#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(
    const NProto::TAllocateDataRequest& request,
    ui32 blockSize,
    ui32 maxFileBlocks)
{
    if (!request.GetHandle()) {
        return ErrorInvalidHandle();
    }

    if (request.GetLength() == 0) {
        return ErrorInvalidArgument();
    }

    if (request.GetOffset() > Max<ui64>() - request.GetLength()) {
        return ErrorFileTooBig();
    }

    TByteRange range(request.GetOffset(), request.GetLength(), blockSize);
    if (range.BlockCount() > maxFileBlocks) {
        return ErrorFileTooBig();
    }

    // F_PUNCH_HOLE must be OR'ed with F_KEEP_SIZE
    if (HasFlag(request.GetFlags(), NProto::TAllocateDataRequest::F_PUNCH_HOLE) &&
        !HasFlag(request.GetFlags(), NProto::TAllocateDataRequest::F_KEEP_SIZE))
    {
        return ErrorNotSupported(
            "FALLOC_FL_PUNCH_HOLE flag must be ORed with FALLOC_FL_KEEP_SIZE");
    }

    // TODO: Support later after NBS-3095 and
    // add errors from https://man7.org/linux/man-pages/man2/fallocate.2.html
    if (HasFlag(request.GetFlags(), NProto::TAllocateDataRequest::F_INSERT_RANGE) ||
        HasFlag(request.GetFlags(), NProto::TAllocateDataRequest::F_COLLAPSE_RANGE))
    {
        return ErrorNotSupported(
            "FALLOC_FL_INSERT_RANGE and FALLOC_FL_COLLAPSE_RANGE flags are not supported");
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAllocateData(
    const TEvService::TEvAllocateDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvAllocateDataResponse>(
                std::move(error)));

        return;
    }

    auto validator = [&] (const NProto::TAllocateDataRequest& request) {
        return ValidateRequest(
            request,
            GetBlockSize(),
            Config->GetMaxFileBlocks());
    };

    if (!AcceptRequest<TEvService::TAllocateDataMethod>(ev, ctx, validator)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvService::TAllocateDataMethod>(*requestInfo);

    ExecuteTx<TAllocateData>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AllocateData(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAllocateData& args)
{
    Y_UNUSED(ctx);

    auto* session = FindSession(args.ClientId, args.SessionId, args.SessionSeqNo);
    if (!session) {
        args.Error = ErrorInvalidSession(
            args.ClientId,
            args.SessionId,
            args.SessionSeqNo);
        return true;
    }

    auto* handle = FindHandle(args.Handle);
    if (!handle || handle->Session != session) {
        args.Error = ErrorInvalidHandle(args.Handle);
        return true;
    }

    if (!HasFlag(handle->GetFlags(), NProto::TCreateHandleRequest::E_WRITE)) {
        args.Error = ErrorInvalidHandle(args.Handle);
        return true;
    }

    args.NodeId = handle->GetNodeId();
    args.CommitId = GetCurrentCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    if (!ReadNode(db, args.NodeId, args.CommitId, args.Node)) {
        return false;
    }

    if (!args.Node) {
        args.Error = ErrorInvalidTarget(args.NodeId);
        return true;
    }

    // Based on https://man7.org/linux/man-pages/man2/fallocate.2.html
    // fallocate can be applied to directory to preallocate space for it.
    // Right now we don't support this feature.
    const auto nodeType = args.Node.GetRef().Attrs.GetType();
    if (nodeType == NProto::ENodeType::E_DIRECTORY_NODE) {
        args.Error = ErrorIsDirectory(args.NodeId);
        return true;
    }

    // Based on https://man7.org/linux/man-pages/man2/fallocate.2.html if node
    // is neither regular file nor directory we must return ENODEV. But if
    // it is pipe or FIFO, we must return ESPIPE. Right now we does not support
    // pipes and FIFO, so always return ENODEV.
    if (nodeType != NProto::ENodeType::E_REGULAR_NODE) {
        args.Error = ErrorInvalidNodeType(args.NodeId);
        return true;
    }

    // TODO: When directory handle will be supported we should add check on
    // invalid argument.
    // https://man7.org/linux/man-pages/man2/fallocate.2.html#:~:text=or%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20FALLOC_FL_INSERT_RANGE.-,EINVAL,-mode%20is%20FALLOC_FL_COLLAPSE_RANGE

    if (!HasSpaceLeft(args.Node->Attrs.GetSize(), args.Offset + args.Length)) {
        args.Error = ErrorNoSpaceLeft();
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.Node);

    return true;
}

void TIndexTabletActor::ExecuteTx_AllocateData(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAllocateData& args)
{
    FILESTORE_VALIDATE_TX_ERROR(AllocateData, args);

    const ui64 size = args.Offset + args.Length;
    const ui64 minBorder = Min(size, args.Node->Attrs.GetSize());
    const bool needExtend = args.Node->Attrs.GetSize() < size &&
        !HasFlag(args.Flags, NProto::TAllocateDataRequest::F_KEEP_SIZE);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    // Here we should not check F_KEEP_SIZE OR'ed with F_PUNCH_HOLE, because
    // we did it in validation stage.
    const bool shouldTruncateExistentRange =
        (HasFlag(args.Flags, NProto::TAllocateDataRequest::F_PUNCH_HOLE) ||
         HasFlag(args.Flags, NProto::TAllocateDataRequest::F_ZERO_RANGE)) &&
        minBorder > args.Offset;

    if (shouldTruncateExistentRange) {
        // Here we should zero range in current file
        args.CommitId = GenerateCommitId();
        if (args.CommitId == InvalidCommitId) {
            return RebootTabletOnCommitOverflow(ctx, "AllocateData");
        }
        auto e = ZeroRange(
            db,
            args.NodeId,
            args.CommitId,
            TByteRange(args.Offset, minBorder - args.Offset, GetBlockSize()));
        if (HasError(e)) {
            args.Error = std::move(e);
            return;
        }
    }

    if (!needExtend) {
        // TODO: Right now we cannot preallocate blocks, but in the future we
        // would do it when F_KEEP_SIZE is set
        // (probably after: NBS-3095)
        return;
    }

    if (!shouldTruncateExistentRange) {
        args.CommitId = GenerateCommitId();
        if (args.CommitId == InvalidCommitId) {
            return RebootTabletOnCommitOverflow(ctx, "AllocateData");
        }
    }

    auto attrs = CopyAttrs(args.Node->Attrs, E_CM_CMTIME);
    attrs.SetSize(size);

    UpdateNode(
        db,
        args.NodeId,
        args.Node->MinCommitId,
        args.CommitId,
        attrs,
        args.Node->Attrs);
}

void TIndexTabletActor::CompleteTx_AllocateData(
    const TActorContext& ctx,
    TTxIndexTablet::TAllocateData& args)
{
    InvalidateNodeCaches(args.NodeId);

    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvAllocateDataResponse>(args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
