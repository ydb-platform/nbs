#include "smart_resync_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using TEvPartition = NPartition::TEvPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

TSmartResyncActor::TSmartResyncActor(
        ISmartResyncDelegate* delegate,
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString rwClientId,
        NActors::TActorId partNonreplActorId,
        NActors::TActorId statActorId,
        NActors::TActorId mirrorPartitionActor,
        NActors::TActorId parentActor,
        std::shared_ptr<TCompressedBitmap> migrationBlockMap,
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
    , Delegate(delegate)
    , Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , PartNonreplActorId(partNonreplActorId)
    , MirrorPartitionActor(mirrorPartitionActor)
    // , StatActorId(statActorId)
    , ParentActor(parentActor)
    , AgentId(std::move(agentId))
{
    Y_DEBUG_ABORT_UNLESS(Delegate);
}

TSmartResyncActor::~TSmartResyncActor() = default;

void TSmartResyncActor::OnBootstrap(const TActorContext& ctx)
{
    InitWork(
        ctx,
        MirrorPartitionActor,
        PartNonreplActorId,
        /*takeOwnershipOverActors=*/false,
        std::make_unique<TMigrationTimeoutCalculator>(
            Config->GetMaxMigrationBandwidth(),
            Config->GetExpectedDiskAgentSize(),
            PartConfig));
    StartWork(ctx);
}

bool TSmartResyncActor::OnMessage(
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

void TSmartResyncActor::OnMigrationProgress(
    const NActors::TActorContext& ctx,
    ui64 migrationIndex)
{
    Y_UNUSED(migrationIndex);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "TSmartResyncActor::OnMigrationProgress %s, GetProcessedBlockCount = %lu, "
        "GetBlockCountNeedToBeProcessed = %lu",
        AgentId.c_str(),
        GetProcessedBlockCount(),
        GetBlockCountNeedToBeProcessed());

    // Straight to volume!
    ctx.Send(
        PartConfig->GetParentActorId(),
        std::make_unique<TEvVolume::TEvUpdateSmartResyncState>(
            AgentId,
            GetProcessedBlockCount(),
            GetBlockCountNeedToBeProcessed()));
}

void TSmartResyncActor::OnMigrationFinished(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "TSmartResyncActor::OnMigrationFinished %s",
        AgentId.c_str());

    // Straight to volume mb?
    ctx.Send(
        PartConfig->GetParentActorId(),
        std::make_unique<TEvVolume::TEvSmartResyncFinished>(AgentId));

    // Delegate->OnMigrationFinished(AgentId);
}

void TSmartResyncActor::OnMigrationError(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "TSmartResyncActor::OnMigrationError %s",
        AgentId.c_str());

    // Abort this and start real resync?


    // Delegate->OnMigrationError(AgentId);
}

}   // namespace NCloud::NBlockStore::NStorage
