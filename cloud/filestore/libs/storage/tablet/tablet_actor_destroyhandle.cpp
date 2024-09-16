#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDestroyHandle(
    const TEvService::TEvDestroyHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvDestroyHandleResponse>(
                std::move(error)));

        return;
    }

    if (!AcceptRequest<TEvService::TDestroyHandleMethod>(ev, ctx)) {
        return;
    }

    auto* msg = ev->Get();

    auto& request = msg->Record;
    auto* handle = FindHandle(request.GetHandle());
    if (!handle || handle->GetSessionId() != GetSessionId(request)) {
        auto response = std::make_unique<TEvService::TEvDestroyHandleResponse>(
            MakeError(S_FALSE, "Invalid handle"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TDestroyHandleMethod>(*requestInfo);

    ExecuteTx<TDestroyHandle>(
        ctx,
        std::move(requestInfo),
        request);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DestroyHandle(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroyHandle& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_SESSION(DestroyHandle, args);

    auto* handle = FindHandle(args.Request.GetHandle());
    if (!handle) {
        // could be already deleted by a concurrently started tx
        args.Error = MakeError(S_ALREADY, "handle already destroyed");
        return true;
    }

    auto commitId = GetCurrentCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);
    if (!ReadNode(db, handle->GetNodeId(), commitId, args.Node)) {
        return false;
    }

    TABLET_VERIFY(args.Node);

    return true;
}

void TIndexTabletActor::ExecuteTx_DestroyHandle(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroyHandle& args)
{
    if (args.Error.GetCode() == S_ALREADY || HasError(args.Error)) {
        return;
    }

    auto* handle = FindHandle(args.Request.GetHandle());
    TABLET_VERIFY(handle);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);
    DestroyHandle(db, handle);

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "DestroyHandle");
    }

    if (args.Node->Attrs.GetLinks() == 0 &&
        !HasOpenHandles(args.Node->NodeId))
    {
        RemoveNode(
            db,
            *args.Node,
            args.Node->MinCommitId,
            commitId);
    }

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_DestroyHandle(
    const TActorContext& ctx,
    TTxIndexTablet::TDestroyHandle& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvDestroyHandleResponse>(args.Error);
    CompleteResponse<TEvService::TDestroyHandleMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
