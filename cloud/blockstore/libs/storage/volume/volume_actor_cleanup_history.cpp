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
        args.Key,
        args.ItemsToRemove,
        args.OutdatedHistory);
}

void TVolumeActor::ExecuteCleanupHistory(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TCleanupHistory& args)
{
    TVolumeDatabase db(tx.DB);

    for (const auto& o : args.OutdatedHistory) {
        db.DeleteHistoryEntry(o);
    }

    if (args.OutdatedHistory.size()) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Deleted %d volume history records in range [%s,%s]",
            LogTitle.GetWithTime().c_str(),
            args.OutdatedHistory.size(),
            args.OutdatedHistory.front().Timestamp.ToString().c_str(),
            args.OutdatedHistory.back().Timestamp.ToString().c_str());
    }
}

void TVolumeActor::CompleteCleanupHistory(
    const TActorContext& ctx,
    TTxVolume::TCleanupHistory& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

}   // namespace NCloud::NBlockStore::NStorage
