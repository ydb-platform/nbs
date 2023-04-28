#include "volume_actor.h"

#include "volume_database.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareWriteThrottlerState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TWriteThrottlerState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteWriteThrottlerState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TWriteThrottlerState& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    db.WriteThrottlerState(args.StateInfo);
}

void TVolumeActor::CompleteWriteThrottlerState(
    const TActorContext& ctx,
    TTxVolume::TWriteThrottlerState& args)
{
    Y_UNUSED(args);

    LastThrottlerStateWrite = ctx.Now();
}

}   // namespace NCloud::NBlockStore::NStorage
