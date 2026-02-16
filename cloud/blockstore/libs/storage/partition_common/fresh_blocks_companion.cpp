#include "fresh_blocks_companion.h"


namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TFreshBlocksCompanion::TFreshBlocksCompanion(
        TStorageConfigPtr config,
        EStorageAccessMode storageAccessMode,
        NProto::TPartitionConfig partitionConfig,
        NKikimr::TTabletStorageInfo* tabletStorageInfo,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        IFreshBlocksCompanionClient& client,
        TPartitionChannelsState& channelsState,
        TPartitionFreshBlobState& freshBlobState,
        TPartitionFlushState& flushState,
        TPartitionTrimFreshLogState& trimFreshLogState,
        TPartitionFreshBlocksState& freshBlocksState,
        TLogTitle logTitle)
    : Config(std::move(config))
    , StorageAccessMode(storageAccessMode)
    , PartitionConfig(std::move(partitionConfig))
    , TabletStorageInfo(tabletStorageInfo)
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , Client(client)
    , ChannelsState(channelsState)
    , FreshBlobState(freshBlobState)
    , FlushState(flushState)
    , TrimFreshLogState(trimFreshLogState)
    , FreshBlocksState(freshBlocksState)
    , LogTitle(std::move(logTitle))
{}

void TFreshBlocksCompanion::KillActors(const NActors::TActorContext& ctx)
{
    for (const auto& actor: Actors.GetActors()) {
        NCloud::Send<NActors::TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TFreshBlocksCompanion::RebootOnCommitIdOverflow(
    const NActors::TActorContext& ctx,
    const TStringBuf& requestName)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s CommitId overflow in %s. Restarting partition",
        LogTitle.GetWithTime().c_str(),
        ToString(requestName).c_str());
    ReportTabletCommitIdOverflow({{"disk", PartitionConfig.GetDiskId()}});
    Client.Poison(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
