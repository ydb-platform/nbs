#include "tablet_actor.h"

#include "helpers.h"

#include <util/generic/overloaded.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

void SetResponseDetails(
    NProto::TTestLockResponse& response,
    TLockIncompatibleInfo incompatible)
{
    std::visit(
        TOverloaded{
            [&](TLockRange&& c)
            {
                response.SetOwner(c.OwnerId);
                response.SetOffset(c.Offset);
                response.SetLength(c.Length);
                response.SetPid(c.Pid);
                response.SetLockType(ConvertTo<NProto::ELockType>(c.LockMode));
            },
            [&](ELockOrigin origin) {
                response.SetIncompatibleLockOrigin(
                    ConvertTo<NProto::ELockOrigin>(origin));
            },
            [&](ELockMode mode)
            { response.SetLockType(ConvertTo<NProto::ELockType>(mode)); },
        },
        std::move(incompatible));
}

}   // namespace

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
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvService::TTestLockMethod>(*requestInfo);

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

    auto range = MakeLockRange(args.Request, handle->GetNodeId());

    auto result = TestLock(session, handle, range);
    if (result.Failed()) {
        args.Incompatible = std::move(result.Incompatible());
    }
    args.Error = std::move(result.Error);
}

void TIndexTabletActor::CompleteTx_TestLock(
    const TActorContext& ctx,
    TTxIndexTablet::TTestLock& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvTestLockResponse>(args.Error);
    if (args.Incompatible.has_value()) {
        SetResponseDetails(response->Record, std::move(*args.Incompatible));
    }

    CompleteResponse<TEvService::TTestLockMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
