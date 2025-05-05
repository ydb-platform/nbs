#include "lagging_agent_migration_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TLaggingAgentMigrationActor::TLaggingAgentMigrationActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId parentActorId,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString rwClientId,
        TActorId targetActorId,
        TActorId sourceActorId,
        TCompressedBitmap migrationBlockMap,
        TString agentId)
    : TNonreplicatedPartitionMigrationCommonActor(
          this,
          config,
          std::move(diagnosticsConfig),
          partConfig->GetName(),
          partConfig->GetBlockCount(),
          partConfig->GetBlockSize(),
          std::move(profileLog),
          std::move(blockDigestGenerator),
          std::move(migrationBlockMap),
          std::move(rwClientId),
          // Since this actor doesn't own source or destination actors, it won't
          // receive any stats and shouldn't send any either.
          TActorId(),   //  statActorId
          config->GetLaggingDeviceMaxMigrationIoDepth(),
          partConfig->GetParentActorId())
    , Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , ParentActorId(parentActorId)
    , TargetActorId(targetActorId)
    , SourceActorId(sourceActorId)
    , AgentId(std::move(agentId))
    , ProcessedBlockCount(GetProcessedBlockCount())
    , BlockCountNeedToBeProcessed(GetBlockCountNeedToBeProcessed())
{}

TLaggingAgentMigrationActor::~TLaggingAgentMigrationActor() = default;

void TLaggingAgentMigrationActor::OnBootstrap(const TActorContext& ctx)
{
    InitWork(
        ctx,
        SourceActorId,
        SourceActorId,
        TargetActorId,
        false,   // takeOwnershipOverActors
        std::make_unique<TMigrationTimeoutCalculator>(
            Config->GetLaggingDeviceMaxMigrationBandwidth(),
            Config->GetExpectedDiskAgentSize(),
            PartConfig));
}

bool TLaggingAgentMigrationActor::OnMessage(
    const TActorContext& ctx,
    TAutoPtr<IEventHandle>& ev)
{
    Y_UNUSED(ctx);
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvNonreplPartitionPrivate::TEvStartLaggingAgentMigration,
            HandleStartLaggingAgentMigration);

        default:
            // Message processing by the base class is required.
            return false;
    }

    // We get here if we have processed an incoming message. And its processing
    // by the base class is not required.
    return true;
}

void TLaggingAgentMigrationActor::HandleStartLaggingAgentMigration(
    const TEvNonreplPartitionPrivate::TEvStartLaggingAgentMigration::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_ABORT_IF(IsMigrationAllowed());
    StartWork(ctx);
}

void TLaggingAgentMigrationActor::OnRangeMigrated(
    const NActors::TActorContext& ctx,
    const TBlockRange64& blockRange)

{
    BlocksMigratedSinceLastReport += blockRange.Size();
    ProcessedBlockCount += blockRange.Size();
    BlockCountNeedToBeProcessed -= blockRange.Size();
    if (Config->GetMigrationIndexCachingInterval() <=
        BlocksMigratedSinceLastReport)
    {
        ctx.Send(
            PartConfig->GetParentActorId(),
            std::make_unique<
                TEvVolumePrivate::TEvUpdateLaggingAgentMigrationState>(
                AgentId,
                ProcessedBlockCount,
                BlockCountNeedToBeProcessed));
        BlocksMigratedSinceLastReport = 0;
    }
}

void TLaggingAgentMigrationActor::OnMigrationProgress(
    const TActorContext& ctx,
    ui64 migrationIndex)
{
    Y_UNUSED(migrationIndex);
    Y_UNUSED(ctx);
}

void TLaggingAgentMigrationActor::OnMigrationFinished(const TActorContext& ctx)
{
    ctx.Send(
        ParentActorId,
        std::make_unique<TEvVolumePrivate::TEvLaggingAgentMigrationFinished>(
            AgentId));
}

void TLaggingAgentMigrationActor::OnMigrationError(const TActorContext& ctx)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Lagging agent %s migration failed",
        PartConfig->GetName().c_str(),
        AgentId.c_str());
}

NActors::TActorId
TLaggingAgentMigrationActor::GetActorToLockAndDrainRange() const
{
    return SourceActorId;
}

}   // namespace NCloud::NBlockStore::NStorage
