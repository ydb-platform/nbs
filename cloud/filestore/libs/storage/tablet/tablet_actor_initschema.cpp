#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_InitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_InitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TInitSchema& args)
{
    Y_UNUSED(ctx, args);

    TIndexTabletDatabase db(tx.DB);

    db.InitSchema();
}

void TIndexTabletActor::CompleteTx_InitSchema(
    const TActorContext& ctx,
    TTxIndexTablet::TInitSchema& args)
{
    Y_UNUSED(args);

    ExecuteTx<TLoadState>(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
