#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/public.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

class TMigrationTimeoutCalculator
{
private:
    const TStorageConfigPtr Config;
    TNonreplicatedPartitionConfigPtr PartitionConfig;

public:
    TMigrationTimeoutCalculator(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partitionConfig);

    [[nodiscard]] TDuration CalculateTimeout(
        TBlockRange64 nextProcessingRange) const;
};

}   // namespace NCloud::NBlockStore::NStorage
