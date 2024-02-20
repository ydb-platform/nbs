#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TResolvePathRequest& request)
{
    const auto& path = request.GetPath();
    if (path.empty()) {
        return ErrorInvalidArgument();
    }

    if (path.size() > MaxPath) {
        return ErrorNameTooLong(path);
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleResolvePath(
    const TEvService::TEvResolvePathRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequest<TEvService::TResolvePathMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo<TEvService::TResolvePathMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction(*requestInfo);

    ExecuteTx<TResolvePath>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_ResolvePath(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TResolvePath& args)
{
    // TODO
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_ResolvePath(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TResolvePath& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TIndexTabletActor::CompleteTx_ResolvePath(
    const TActorContext& ctx,
    TTxIndexTablet::TResolvePath& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvResolvePathResponse>(args.Error);
    CompleteResponse<TEvService::TResolvePathMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
