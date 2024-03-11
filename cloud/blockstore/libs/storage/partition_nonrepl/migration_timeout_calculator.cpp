#include "migration_timeout_calculator.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>

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
    const auto migrationFactorPerAgent = MaxMigrationBandwidthMiBs / 4;

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
            1U);

    return TDuration::Seconds(1) / factor;
}

}   // namespace NCloud::NBlockStore::NStorage
