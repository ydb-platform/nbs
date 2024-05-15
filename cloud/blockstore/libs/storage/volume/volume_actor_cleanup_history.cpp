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
    return db.ReadOutdatedHistory(
        args.OutdatedHistory,
        args.Key,
        args.ItemsToRemove);
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

    if (args.OutdatedHistory.size()) {
        LOG_INFO_S(ctx, TBlockStoreComponents::VOLUME,
            "[" << TabletID() << "]"
            << "Deleted " << args.OutdatedHistory.size()
            << " volume history records in range ["
            << args.OutdatedHistory.front().Timestamp
            << "," << args.OutdatedHistory.back().Timestamp << "]");
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::VOLUME,
            "[" << TabletID() << "]"
            << "Nothing to remove in volume history records");
    }
}

void TVolumeActor::CompleteCleanupHistory(
    const TActorContext& ctx,
    TTxVolume::TCleanupHistory& args)
{
    Y_UNUSED(args);
    if (args.OutdatedHistory.empty()) {
        LastHistoryCleanup = ctx.Now();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
