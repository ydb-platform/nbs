#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/public.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

// Calculates the time during which a 4MB block should migrate.
class TMigrationTimeoutCalculator
{
private:
    const ui32 MaxMigrationBandwidthMiBs = 0;
    const ui32 ExpectedDiskAgentSize = 0;
    TNonreplicatedPartitionConfigPtr PartitionConfig;
    ui32 LimitedBandwidthMiBs = 0;
    ui64 RecommendedBandwidth = 0;

public:
    TMigrationTimeoutCalculator(
        ui32 maxMigrationBandwidthMiBs,
        ui32 expectedDiskAgentSize,
        TNonreplicatedPartitionConfigPtr partitionConfig);

    [[nodiscard]] TDuration CalculateTimeout(
        TBlockRange64 nextProcessingRange) const;

    void RegisterTrafficSource(const NActors::TActorContext& ctx);

    void HandleUpdateBandwidthLimit(
        const TEvStatsServicePrivate::
            TEvRegisterTrafficSourceResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void SetRecommendedBandwidth(ui64 bandwidth);
};

}   // namespace NCloud::NBlockStore::NStorage
