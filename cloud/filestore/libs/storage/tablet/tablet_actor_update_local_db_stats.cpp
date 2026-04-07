#include "tablet_actor.h"

#include "tablet_schema.h"

#include <cloud/filestore/libs/diagnostics/metrics/operations.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UpdateLocalDbStats(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUpdateLocalDbStats& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TIndexTabletActor::ExecuteTx_UpdateLocalDbStats(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUpdateLocalDbStats& args)
{
    Y_UNUSED(ctx);

    [&]<typename... Tables>(
        NKikimr::NIceDb::Schema::SchemaTables<Tables...>*)
    {
        args.TableStats = {TTxIndexTablet::TLocalDbTableStat{
            .TableId         = Tables::TableId,
            .EstimatedRowSize= tx.DB.EstimateRowSize(Tables::TableId),
            .MemSize         = tx.DB.GetTableMemSize(Tables::TableId),
            .MemRowCount     = tx.DB.GetTableMemRowCount(Tables::TableId),
            .MemOpsCount     = tx.DB.GetTableMemOpsCount(Tables::TableId),
            .IndexSize       = tx.DB.GetTableIndexSize(Tables::TableId),
            .SearchHeight    = tx.DB.GetTableSearchHeight(Tables::TableId),
        }...};
    }(static_cast<TIndexTabletSchema::TTables*>(nullptr));
}

void TIndexTabletActor::CompleteTx_UpdateLocalDbStats(
    const TActorContext& ctx,
    TTxIndexTablet::TUpdateLocalDbStats& args)
{
    Y_UNUSED(ctx);

    for (const auto& stat : args.TableStats) {
        Y_DEBUG_ABORT_UNLESS(stat.TableId < TTabletMetrics::LocalDbTableCount);
        auto& dst = Metrics.LocalDbTableStats[stat.TableId];
        NMetrics::Store(dst.EstimatedRowSize, stat.EstimatedRowSize);
        NMetrics::Store(dst.MemSize,          stat.MemSize);
        NMetrics::Store(dst.MemRowCount,      stat.MemRowCount);
        NMetrics::Store(dst.MemOpsCount,      stat.MemOpsCount);
        NMetrics::Store(dst.IndexSize,        stat.IndexSize);
        NMetrics::Store(dst.SearchHeight,     stat.SearchHeight);
    }
}

}   // namespace NCloud::NFileStore::NStorage
