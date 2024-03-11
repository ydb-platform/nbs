#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/public.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

// Calculates the time during which a 4MB block should migrate.
class TMigrationTimeoutCalculator
{
private:
    const ui32 MaxMigrationBandwidthMiBs = 0;
    const ui32 ExpectedDiskAgentSize = 0;
    TNonreplicatedPartitionConfigPtr PartitionConfig;

public:
    TMigrationTimeoutCalculator(
        ui32 maxMigrationBandwidthMiBs,
        ui32 expectedDiskAgentSize,
        TNonreplicatedPartitionConfigPtr partitionConfig);

    [[nodiscard]] TDuration
    CalculateTimeout(TBlockRange64 nextProcessingRange) const;
};

}   // namespace NCloud::NBlockStore::NStorage
