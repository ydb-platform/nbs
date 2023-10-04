#include "tablet_actor.h"

#include <cloud/filestore/libs/service/public.h>

#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TAcquireLockRequest& request, ui32 blockSize)
{
    if (request.GetHandle() == InvalidHandle) {
        return ErrorInvalidHandle();
    }

    TByteRange range(request.GetOffset(), request.GetLength(), blockSize);
    if (range.BlockCount() > MaxFileBlocks) {
        return ErrorFileTooBig();
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAcquireLock(
    const TEvService::TEvAcquireLockRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto validator = [&] (const NProto::TAcquireLockRequest& request) {
        return ValidateRequest(request, GetBlockSize());
    };

    if (!AcceptRequest<TEvService::TAcquireLockMethod>(ev, ctx, validator)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TAcquireLock>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AcquireLock(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAcquireLock& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    FILESTORE_VALIDATE_TX_SESSION(AcquireLock, args);

    return true;
}

void TIndexTabletActor::ExecuteTx_AcquireLock(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAcquireLock& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_ERROR(AcquireLock, args);

    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);
    TABLET_VERIFY(session);

    auto* handle = FindHandle(args.Request.GetHandle());
    if (!handle || handle->GetSessionId() != session->GetSessionId()) {
        args.Error = ErrorInvalidHandle();
        return;
    }

    ELockMode mode = GetLockMode(args.Request.GetLockType());
    // FIXME: NBS-2933 validate handle mode for fcntl locks

    TLockRange range = {
        .NodeId = handle->GetNodeId(),
        .OwnerId = args.Request.GetOwner(),
        .Offset = args.Request.GetOffset(),
        .Length = args.Request.GetLength()
    };

    if (!TestLock(session, range, mode, nullptr)) {
        args.Error = ErrorIncompatibleLocks();
        return;
    }

    TIndexTabletDatabase db(tx.DB);

    // TODO: access check
    AcquireLock(db, session, handle->GetHandle(), range, mode);
}

void TIndexTabletActor::CompleteTx_AcquireLock(
    const TActorContext& ctx,
    TTxIndexTablet::TAcquireLock& args)
{
    auto response = std::make_unique<TEvService::TEvAcquireLockResponse>(args.Error);
    CompleteResponse<TEvService::TAcquireLockMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
