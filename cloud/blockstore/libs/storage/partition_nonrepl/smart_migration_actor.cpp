#include "smart_migration_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using TEvPartition = NPartition::TEvPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

TSmartMigrationActor::TSmartMigrationActor(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString rwClientId,
        NActors::TActorId targetActorId,
        NActors::TActorId sourceActorId,
        NActors::TActorId statActorId,
        TCompressedBitmap migrationBlockMap,
        TString agentId)
    : TNonreplicatedPartitionMigrationCommonActor(
          this,
          config,
          partConfig->GetName(),
          partConfig->GetBlockCount(),
          partConfig->GetBlockSize(),
          profileLog,
          blockDigestGenerator,
          std::move(migrationBlockMap),
          std::move(rwClientId),
          statActorId,
          config->GetMaxMigrationIoDepth())
    , Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , TargetActorId(targetActorId)
    , SourceActorId(sourceActorId)
    , AgentId(std::move(agentId))
{}

TSmartMigrationActor::~TSmartMigrationActor() = default;

void TSmartMigrationActor::OnBootstrap(const TActorContext& ctx)
{
    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "xxxxx TSmartMigrationActor::OnBootstrap: AID[%s]",
        ctx.SelfID.ToString().c_str());

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

bool TSmartMigrationActor::OnMessage(
    const NActors::TActorContext& ctx,
    TAutoPtr<NActors::IEventHandle>& ev)
{
    Y_UNUSED(ctx);
    switch (ev->GetTypeRewrite()) {
        // HFunc(
        //     TEvVolumePrivate::TEvShadowDiskAcquired,
        //     HandleShadowDiskAcquired);


        // Write/zero request.
        // case TEvService::TEvWriteBlocksRequest::EventType: {
        //     return HandleWriteZeroBlocks<TEvService::TWriteBlocksMethod>(
        //         *reinterpret_cast<TEvService::TEvWriteBlocksRequest::TPtr*>(
        //             &ev),
        //         ctx);
        // }
        // case TEvService::TEvWriteBlocksLocalRequest::EventType: {
        //     return HandleWriteZeroBlocks<TEvService::TWriteBlocksLocalMethod>(
        //         *reinterpret_cast<
        //             TEvService::TEvWriteBlocksLocalRequest::TPtr*>(&ev),
        //         ctx);
        // }
        // case TEvService::TEvZeroBlocksRequest::EventType: {
        //     return HandleWriteZeroBlocks<TEvService::TZeroBlocksMethod>(
        //         *reinterpret_cast<TEvService::TEvZeroBlocksRequest::TPtr*>(&ev),
        //         ctx);
        // }

        default:
            // Message processing by the base class is required.
            return false;
    }

    // We get here if we have processed an incoming message. And its processing
    // by the base class is not required.
    return true;
}

void TSmartMigrationActor::OnMigrationProgress(
    const NActors::TActorContext& ctx,
    ui64 migrationIndex)
{
    Y_UNUSED(migrationIndex);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "TSmartMigrationActor::OnMigrationProgress %s, GetProcessedBlockCount = %lu, "
        "GetBlockCountNeedToBeProcessed = %lu",
        AgentId.c_str(),
        GetProcessedBlockCount(),
        GetBlockCountNeedToBeProcessed());

    // Straight to volume!
    ctx.Send(
        PartConfig->GetParentActorId(),
        std::make_unique<TEvVolumePrivate::TEvUpdateSmartMigrationState>(
            AgentId,
            GetProcessedBlockCount(),
            GetBlockCountNeedToBeProcessed()));
}

void TSmartMigrationActor::OnMigrationFinished(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "TSmartMigrationActor::OnMigrationFinished %s",
        AgentId.c_str());

    // Straight to volume mb?
    ctx.Send(
        PartConfig->GetParentActorId(),
        std::make_unique<TEvVolumePrivate::TEvSmartMigrationFinished>(AgentId));
}

void TSmartMigrationActor::OnMigrationError(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "TSmartMigrationActor::OnMigrationError %s",
        AgentId.c_str());

    // Abort this and start real resync?
}

}   // namespace NCloud::NBlockStore::NStorage
