#include "tablet_actor.h"

#include "tablet_database.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_SetHasXAttrs(
    const TActorContext& /* ctx */,
    TTransactionContext& /* tx */,
    TTxIndexTablet::TSetHasXAttrs& args)
{
    args.IsToBeChanged = (GetHasXAttrs() != args.Request.GetValue());
    return true;
}

void TIndexTabletActor::ExecuteTx_SetHasXAttrs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetHasXAttrs& args)
{
    TIndexTabletDatabase db(tx.DB);

    // write only the value is changed
    if (args.IsToBeChanged) {
        WriteHasXAttrs(db, args.Request.GetValue());
    }
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "Set: %lu HasXAttrs: %lu",
        args.IsToBeChanged,
        GetHasXAttrs());
}

void TIndexTabletActor::CompleteTx_SetHasXAttrs(
    const TActorContext& ctx,
    TTxIndexTablet::TSetHasXAttrs& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvIndexTablet::TEvSetHasXAttrsResponse>();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    // when the value is changed, suicide in order all ssesions to be recreated
    if (args.IsToBeChanged) {
        TIndexTabletActor::Suicide(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleSetHasXAttrs(
    const TEvIndexTablet::TEvSetHasXAttrsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TSetHasXAttrsMethod>(*requestInfo);

    const auto* msg = ev->Get();
    ExecuteTx<TSetHasXAttrs>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

}   // namespace NCloud::NFileStore::NStorage
