#include "fresh_blocks_writer_actor.h"

#include <cloud/blockstore/libs/storage/core/forward_helpers.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_FORWARD_TO_PARTITION(name, ns)                              \
    void TFreshBlocksWriterActor::Handle##name(                                \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const NActors::TActorContext& ctx)                                     \
    {                                                                          \
        ForwardMessageToActor(ev, ctx, PartitionActorId);                      \
    }                                                                          \
// BLOCKSTORE_FORWARD_TO_PARTITION

    BLOCKSTORE_FORWARD_TO_PARTITION(ReadBlocks,               TEvService)
    BLOCKSTORE_FORWARD_TO_PARTITION(WriteBlocks,              TEvService)
    BLOCKSTORE_FORWARD_TO_PARTITION(ZeroBlocks,               TEvService)
    BLOCKSTORE_FORWARD_TO_PARTITION(CreateCheckpoint,         TEvService)
    BLOCKSTORE_FORWARD_TO_PARTITION(DeleteCheckpoint,         TEvService)
    BLOCKSTORE_FORWARD_TO_PARTITION(GetChangedBlocks,         TEvService)
    BLOCKSTORE_FORWARD_TO_PARTITION(GetCheckpointStatus,      TEvService)
    BLOCKSTORE_FORWARD_TO_PARTITION(ReadBlocksLocal,          TEvService)
    BLOCKSTORE_FORWARD_TO_PARTITION(WriteBlocksLocal,         TEvService)


    BLOCKSTORE_FORWARD_TO_PARTITION(DescribeBlocks,           TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(GetUsedBlocks,            TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(GetPartitionInfo,         TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(CompactRange,             TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(GetCompactionStatus,      TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(DeleteCheckpointData,     TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(RebuildMetadata,          TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(GetRebuildMetadataStatus, TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(ScanDisk,                 TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(GetScanDiskStatus,        TEvVolume)
    BLOCKSTORE_FORWARD_TO_PARTITION(CheckRange,               TEvVolume)

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
