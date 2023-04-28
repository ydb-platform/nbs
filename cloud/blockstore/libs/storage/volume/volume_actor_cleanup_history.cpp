#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareCleanupHistory(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TCleanupHistory& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    return db.ReadOutdatedHistory(args.OutdatedHistory, args.Key);
}

void TVolumeActor::ExecuteCleanupHistory(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TCleanupHistory& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);

    for (const auto& o : args.OutdatedHistory) {
        db.DeleteHistoryEntry(o);
    }
}

void TVolumeActor::CompleteCleanupHistory(
    const TActorContext& ctx,
    TTxVolume::TCleanupHistory& args)
{
    Y_UNUSED(args);
    LastHistoryCleanup = ctx.Now();
}

}   // namespace NCloud::NBlockStore::NStorage
