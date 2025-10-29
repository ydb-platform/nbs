#include "part2_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo_ForceCompaction(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    using namespace NMonitoringUtils;

    if (State->IsForcedCompactionRunning()) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            "Compaction is already running",
            EAlertLevel::SUCCESS);
        return;
    }

    ui64 blockIndex = 0;
    if (const auto& param = params.Get("BlockIndex");
        param && !TryFromString(param, blockIndex))
    {
        blockIndex = 0;
    }

    ui32 blocksCount = 0;
    if (const auto& param = params.Get("BlocksCount");
        param && !TryFromString(param, blocksCount))
    {
        blocksCount = 0;
    }

    auto& compactionMap = State->GetCompactionMap();

    TVector<ui32> rangesToCompact;
    if (blockIndex || blocksCount) {
        auto startIndex = Min(State->GetBlockCount(), blockIndex);
        auto endIndex = Min(State->GetBlockCount(), blockIndex + blocksCount);

        rangesToCompact = TVector<ui32>(
            ::xrange(
                compactionMap.GetRangeStart(startIndex),
                compactionMap.GetRangeStart(endIndex) + compactionMap.GetRangeSize(),
                compactionMap.GetRangeSize())
        );
    } else {
        rangesToCompact = compactionMap.GetNonEmptyRanges();
    }

    if (!rangesToCompact) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            "Nothing to compact",
            EAlertLevel::SUCCESS);
        return;
    }

    AddForcedCompaction(ctx, std::move(rangesToCompact), "partition-monitoring-compaction");
    EnqueueForcedCompaction(ctx);

    SendHttpResponse(
        ctx,
        *requestInfo,
        "Compaction has been started",
        EAlertLevel::SUCCESS);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
