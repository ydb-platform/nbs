#include "migration_timeout_calculator.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TMigrationTimeoutCalculator::TMigrationTimeoutCalculator(
        ui32 maxMigrationBandwidthMiBs,
        ui32 expectedDiskAgentSize,
        TNonreplicatedPartitionConfigPtr partitionConfig)
    : MaxMigrationBandwidthMiBs(maxMigrationBandwidthMiBs)
    , ExpectedDiskAgentSize(expectedDiskAgentSize)
    , PartitionConfig(std::move(partitionConfig))
{}

TDuration TMigrationTimeoutCalculator::CalculateTimeout(
    TBlockRange64 nextProcessingRange) const
{
    // migration range is 4_MB
    const double processingRangeSizeMiBs =
        static_cast<double>(ProcessingRangeSize) / (1024 * 1024);
    const double migrationFactorPerAgent =
        MaxMigrationBandwidthMiBs / processingRangeSizeMiBs;

    if (PartitionConfig->GetUseSimpleMigrationBandwidthLimiter()) {
        return TDuration::Seconds(1) / migrationFactorPerAgent;
    }

    const auto& sourceDevices = PartitionConfig->GetDevices();
    const auto requests =
        PartitionConfig->ToDeviceRequests(nextProcessingRange);

    ui32 agentDeviceCount = 0;
    if (!requests.empty()) {
        agentDeviceCount = CountIf(
            sourceDevices,
            [&](const auto& d)
            { return d.GetAgentId() == requests.front().Device.GetAgentId(); });
    }

    const auto factor =
        Max(migrationFactorPerAgent * agentDeviceCount / ExpectedDiskAgentSize,
            1.0);

    return TDuration::Seconds(1) / factor;
}

}   // namespace NCloud::NBlockStore::NStorage
