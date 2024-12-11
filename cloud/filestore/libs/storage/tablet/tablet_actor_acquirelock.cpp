#include "tablet_actor.h"

#include <cloud/filestore/libs/service/public.h>

#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(
    const NProto::TAcquireLockRequest& request,
    ui32 blockSize,
    ui32 maxFileBlocks)
{
    if (request.GetHandle() == InvalidHandle) {
        return ErrorInvalidHandle();
    }

    TByteRange range(request.GetOffset(), request.GetLength(), blockSize);
    if (range.BlockCount() > maxFileBlocks) {
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
        return ValidateRequest(
            request,
            GetBlockSize(),
            Config->GetMaxFileBlocks());
    };

    if (!AcceptRequest<TEvService::TAcquireLockMethod>(ev, ctx, validator)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvService::TAcquireLockMethod>(*requestInfo);

    ExecuteTx<TAcquireLock>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AcquireLock(
    const TActorContext&  /*ctx*/,
    TTransactionContext&  /*tx*/,
    TTxIndexTablet::TAcquireLock& args)
{
    FILESTORE_VALIDATE_TX_SESSION(AcquireLock, args);

    return true;
}

void TIndexTabletActor::ExecuteTx_AcquireLock(
    const TActorContext&  /*ctx*/,
    TTransactionContext& tx,
    TTxIndexTablet::TAcquireLock& args)
{
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

    TLockRange range = MakeLockRange(args.Request, handle->GetNodeId());

    auto result = TestLock(session, handle, range);
    if (result.Succeeded()) {
        TIndexTabletDatabase db(tx.DB);
        result = AcquireLock(db, session, handle->GetHandle(), range);
    }
    args.Error = std::move(result.Error);
}

void TIndexTabletActor::CompleteTx_AcquireLock(
    const TActorContext& ctx,
    TTxIndexTablet::TAcquireLock& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvAcquireLockResponse>(args.Error);
    CompleteResponse<TEvService::TAcquireLockMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
