#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    db.DeleteOpLogEntry(args.EntryId);
}

void TIndexTabletActor::CompleteTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DeleteOpLogEntry completed (%lu)",
        LogTag.c_str(),
        args.EntryId);
}

}   // namespace NCloud::NFileStore::NStorage
