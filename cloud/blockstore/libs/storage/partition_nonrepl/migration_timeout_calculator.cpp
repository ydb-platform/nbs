#include "migration_timeout_calculator.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TMigrationTimeoutCalculator::TMigrationTimeoutCalculator(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partitionConfig)
    : Config(std::move(config))
    , PartitionConfig(std::move(partitionConfig))
{}

TDuration TMigrationTimeoutCalculator::CalculateTimeout(
    TBlockRange64 nextProcessingRange) const
{
    const ui32 maxMigrationBandwidthMiBs = Config->GetMaxMigrationBandwidth();
    const ui32 expectedDiskAgentSize = Config->GetExpectedDiskAgentSize();

    // migration range is 4_MB
    const auto migrationFactorPerAgent = maxMigrationBandwidthMiBs / 4;

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
        Max(migrationFactorPerAgent * agentDeviceCount / expectedDiskAgentSize,
            1U);

    return TDuration::Seconds(1) / factor;
}

}   // namespace NCloud::NBlockStore::NStorage
