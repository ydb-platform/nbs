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
        NActors::TActorId parentActorId,
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
          config->GetMaxMigrationIoDepth())
    , Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , ParentActorId(parentActorId)
    , TargetActorId(targetActorId)
    , SourceActorId(sourceActorId)
    , AgentId(std::move(agentId))
{}

TLaggingAgentMigrationActor::~TLaggingAgentMigrationActor() = default;

void TLaggingAgentMigrationActor::OnBootstrap(const TActorContext& ctx)
{
    InitWork(
        ctx,
        SourceActorId,
        TargetActorId,
        false,   // takeOwnershipOverActors
        std::make_unique<TMigrationTimeoutCalculator>(
            Config->GetMaxMigrationBandwidth(),
            Config->GetExpectedDiskAgentSize(),
            PartConfig));
    StartWork(ctx);
}

bool TLaggingAgentMigrationActor::OnMessage(
    const TActorContext& ctx,
    TAutoPtr<IEventHandle>& ev)
{
    Y_UNUSED(ctx);
    Y_UNUSED(ev);
    return false;
}

void TLaggingAgentMigrationActor::OnMigrationProgress(
    const TActorContext& ctx,
    ui64 migrationIndex)
{
    Y_UNUSED(migrationIndex);

    ctx.Send(
        PartConfig->GetParentActorId(),
        std::make_unique<TEvVolumePrivate::TEvUpdateLaggingAgentMigrationState>(
            AgentId,
            GetProcessedBlockCount(),
            GetBlockCountNeedToBeProcessed()));
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

}   // namespace NCloud::NBlockStore::NStorage
