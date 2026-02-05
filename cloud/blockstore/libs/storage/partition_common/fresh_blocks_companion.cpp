#include "fresh_blocks_companion.h"


namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TFreshBlocksCompanion::TFreshBlocksCompanion(
        EStorageAccessMode storageAccessMode,
        NProto::TPartitionConfig partitionConfig,
        NKikimr::TTabletStorageInfo* tabletStorageInfo,
        IFreshBlocksCompanionClient& client,
        TPartitionChannelsState& channelsState,
        TPartitionFreshBlobState& freshBlobState,
        TPartitionFlushState& flushState,
        TPartitionTrimFreshLogState& trimFreshLogState,
        TPartitionFreshBlocksState& freshBlocksState,
        TLogTitle logTitle)
    : StorageAccessMode(storageAccessMode)
    , PartitionConfig(std::move(partitionConfig))
    , TabletStorageInfo(tabletStorageInfo)
    , Client(client)
    , ChannelsState(channelsState)
    , FreshBlobState(freshBlobState)
    , FlushState(flushState)
    , TrimFreshLogState(trimFreshLogState)
    , FreshBlocksState(freshBlocksState)
    , LogTitle(std::move(logTitle))
{}

}   // namespace NCloud::NBlockStore::NStorage
