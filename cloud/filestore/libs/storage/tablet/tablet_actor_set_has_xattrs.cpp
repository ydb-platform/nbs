#include "tablet_actor.h"

#include "tablet_database.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

inline TIndexTabletActor::EHasXAttrs EHasXAttrsFromBool(bool value)
{
    return value ? TIndexTabletActor::EHasXAttrs::True
                 : TIndexTabletActor::EHasXAttrs::False;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_SetHasXAttrs(
    const TActorContext&,
    TTransactionContext&,
    TTxIndexTablet::TSetHasXAttrs& args)
{
    const ui64 newValue =
        static_cast<ui64>(EHasXAttrsFromBool(args.Request.GetValue()));
    args.IsToBeChanged = (GetHasXAttrs() != newValue);
    return true;
}

void TIndexTabletActor::ExecuteTx_SetHasXAttrs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TSetHasXAttrs& args)
{
    TIndexTabletDatabase db(tx.DB);

    // Write only if the value is changed
    if (args.IsToBeChanged) {
        WriteHasXAttrs(db, EHasXAttrsFromBool(args.Request.GetValue()));
    }
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "SetHasXAttrs: %lu, HasXAttrs is changed: %s",
        GetHasXAttrs(),
        args.IsToBeChanged ? "true" : "false");
}

void TIndexTabletActor::CompleteTx_SetHasXAttrs(
    const TActorContext& ctx,
    TTxIndexTablet::TSetHasXAttrs& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvIndexTablet::TEvSetHasXAttrsResponse>();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    // Suiciding in order to force clients to recreate sessions
    if (args.IsToBeChanged) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Suiciding after HasXAttrs is changed to force clients to fetch"
            " a new value",
            LogTag.c_str());

        Suicide(ctx);
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
