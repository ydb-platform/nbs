#include "part_nonrepl_migration_state.h"

#include "config.h"

#include <cloud/blockstore/libs/storage/core/config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TNonreplicatedPartitionMigrationState::TNonreplicatedPartitionMigrationState(
        TStorageConfigPtr config,
        ui64 initialMigrationIndex,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr srcPartitionActorConfig)
    : Config(std::move(config))
    , RWClientId(std::move(rwClientId))
    , SrcPartitionActorConfig(std::move(srcPartitionActorConfig))
    , ProcessingBlocks(
        SrcPartitionActorConfig->GetBlockCount(),
        SrcPartitionActorConfig->GetBlockSize(),
        initialMigrationIndex)
{
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationState::SetupDstPartition(
    TNonreplicatedPartitionConfigPtr config)
{
    DstPartitionActorConfig = std::move(config);
}

void TNonreplicatedPartitionMigrationState::AbortMigration()
{
    ProcessingBlocks.AbortProcessing();
}

bool TNonreplicatedPartitionMigrationState::IsMigrationStarted() const
{
    return ProcessingBlocks.IsProcessingStarted();
}

void TNonreplicatedPartitionMigrationState::MarkMigrated(TBlockRange64 range)
{
    ProcessingBlocks.MarkProcessed(range);
}

bool TNonreplicatedPartitionMigrationState::AdvanceMigrationIndex()
{
    return ProcessingBlocks.AdvanceProcessingIndex();
}

bool TNonreplicatedPartitionMigrationState::SkipMigratedRanges()
{
    return ProcessingBlocks.SkipProcessedRanges();
}

TBlockRange64 TNonreplicatedPartitionMigrationState::BuildMigrationRange() const
{
    return ProcessingBlocks.BuildProcessingRange();
}

ui64 TNonreplicatedPartitionMigrationState::GetMigratedBlockCount() const
{
    return ProcessingBlocks.GetProcessedBlockCount();
}

TDuration TNonreplicatedPartitionMigrationState::CalculateMigrationTimeout(
    ui32 maxMigrationBandwidthMiBs,
    ui32 expectedDiskAgentSize) const
{
    // migration range is 4_MB
    const auto migrationFactorPerAgent = maxMigrationBandwidthMiBs / 4;

    if (SrcPartitionActorConfig->GetUseSimpleMigrationBandwidthLimiter()) {
        return TDuration::Seconds(1) / migrationFactorPerAgent;
    }

    const auto& sourceDevices = SrcPartitionActorConfig->GetDevices();
    const auto requests =
        SrcPartitionActorConfig->ToDeviceRequests(BuildMigrationRange());

    ui32 agentDeviceCount = 0;
    if (requests.size()) {
        agentDeviceCount = CountIf(sourceDevices, [&] (const auto& d) {
            return d.GetAgentId() == requests.front().Device.GetAgentId();
        });
    }

    const auto factor = Max(
        migrationFactorPerAgent * agentDeviceCount / expectedDiskAgentSize,
        1U);

    return TDuration::Seconds(1) / factor;
}

}   // namespace NCloud::NBlockStore::NStorage
