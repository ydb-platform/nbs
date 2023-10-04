#include "tablet_actor.h"

#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleTestLock(
    const TEvService::TEvTestLockRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequest<TEvService::TTestLockMethod>(ev, ctx)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TTestLock>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_TestLock(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TTestLock& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    FILESTORE_VALIDATE_TX_SESSION(TestLock, args);

    return true;
}

void TIndexTabletActor::ExecuteTx_TestLock(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TTestLock& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    FILESTORE_VALIDATE_TX_ERROR(TestLock, args);

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
        .Length = args.Request.GetLength(),
    };

    TLockRange conflicting;
    if (!TestLock(session, range, mode, &conflicting)) {
        args.Error = ErrorIncompatibleLocks();
        args.Conflicting = conflicting;
        return;
    }
}

void TIndexTabletActor::CompleteTx_TestLock(
    const TActorContext& ctx,
    TTxIndexTablet::TTestLock& args)
{
    auto response = std::make_unique<TEvService::TEvTestLockResponse>(args.Error);
    if (args.Conflicting) {
        response->Record.SetOwner(args.Conflicting->OwnerId);
        response->Record.SetOffset(args.Conflicting->Offset);
        response->Record.SetLength(args.Conflicting->Length);
    }

    CompleteResponse<TEvService::TTestLockMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
