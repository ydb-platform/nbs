#include "volume_actor.h"

#include "volume_database.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteInitSchema(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TVolumeDatabase db(tx.DB);
    db.InitSchema();
}

void TVolumeActor::CompleteInitSchema(
    const TActorContext& ctx,
    TTxVolume::TInitSchema& args)
{
    Y_UNUSED(args);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Schema initialized",
        LogTitle.Get(TLogTitle::EDetails::WithTime).c_str());

    ExecuteTx<TLoadState>(
        ctx,
        ctx.Now() - Config->GetVolumeHistoryDuration());
}

}   // namespace NCloud::NBlockStore::NStorage
