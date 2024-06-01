#include "migration_timeout_calculator.h"

#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/storage/core/libs/actors/helpers.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TMigrationTimeoutCalculator::TMigrationTimeoutCalculator(
        ui32 maxMigrationBandwidthMiBs,
        ui32 expectedDiskAgentSize,
        TNonreplicatedPartitionConfigPtr partitionConfig)
    : MaxMigrationBandwidthMiBs(maxMigrationBandwidthMiBs)
    , ExpectedDiskAgentSize(expectedDiskAgentSize)
    , PartitionConfig(std::move(partitionConfig))
    , LimitedBandwidthMiBs(maxMigrationBandwidthMiBs)
{}

TDuration TMigrationTimeoutCalculator::CalculateTimeout(
    TBlockRange64 nextProcessingRange) const
{
    // migration range is 4_MB
    const double processingRangeSizeMiBs =
        static_cast<double>(ProcessingRangeSize) / (1024 * 1024);

    const ui32 limitedBandwidthMiBs =
        Min(MaxMigrationBandwidthMiBs, LimitedBandwidthMiBs);
    const double migrationFactorPerAgent =
        limitedBandwidthMiBs / processingRangeSizeMiBs;

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

void TMigrationTimeoutCalculator::RegisterTrafficSource(
    const NActors::TActorContext& ctx)
{
    auto request = std::make_unique<
        TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest>();
    request->SourceId = PartitionConfig->GetName();
    request->BandwidthMiBs = MaxMigrationBandwidthMiBs;
    NCloud::Send(ctx, MakeStorageStatsServiceId(), std::move(request));
}

void TMigrationTimeoutCalculator::HandleUpdateBandwidthLimit(
    const TEvStatsServicePrivate::TEvRegisterTrafficSourceResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        return;
    }

    LimitedBandwidthMiBs = msg->LimitedBandwidthMiBs;
}

}   // namespace NCloud::NBlockStore::NStorage
