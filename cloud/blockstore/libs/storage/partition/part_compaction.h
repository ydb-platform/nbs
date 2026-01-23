#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/protos/blobstorage.pb.h>
#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/array_ref.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TRangeCompactionInfo
{
    const TBlockRange32 BlockRange;
    const NCloud::TPartialBlobId OriginalBlobId;
    const NCloud::TPartialBlobId DataBlobId;
    const TBlockMask DataBlobSkipMask;
    const NCloud::TPartialBlobId ZeroBlobId;
    const TBlockMask ZeroBlobSkipMask;
    const ui32 BlobsSkippedByCompaction;
    const ui32 BlocksSkippedByCompaction;
    const TVector<ui32> BlockChecksums;
    const EChannelDataKind ChannelDataKind;

    TGuardedBuffer<TBlockBuffer> BlobContent;
    TVector<ui32> ZeroBlocks;
    TAffectedBlobs AffectedBlobs;
    TAffectedBlocks AffectedBlocks;
    TVector<ui16> UnchangedBlobOffsets;
    TArrayHolder<NKikimr::TEvBlobStorage::TEvPatch::TDiff> Diffs;
    ui32 DiffCount = 0;

    TRangeCompactionInfo(
        TBlockRange32 blockRange,
        NCloud::TPartialBlobId originalBlobId,
        NCloud::TPartialBlobId dataBlobId,
        TBlockMask dataBlobSkipMask,
        NCloud::TPartialBlobId zeroBlobId,
        TBlockMask zeroBlobSkipMask,
        ui32 blobsSkippedByCompaction,
        ui32 blocksSkippedByCompaction,
        TVector<ui32> blockChecksums,
        EChannelDataKind channelDataKind,
        TBlockBuffer blobContent,
        TVector<ui32> zeroBlocks,
        TAffectedBlobs affectedBlobs,
        TAffectedBlocks affectedBlocks)
        : BlockRange(blockRange)
        , OriginalBlobId(originalBlobId)
        , DataBlobId(dataBlobId)
        , DataBlobSkipMask(dataBlobSkipMask)
        , ZeroBlobId(zeroBlobId)
        , ZeroBlobSkipMask(zeroBlobSkipMask)
        , BlobsSkippedByCompaction(blobsSkippedByCompaction)
        , BlocksSkippedByCompaction(blocksSkippedByCompaction)
        , BlockChecksums(std::move(blockChecksums))
        , ChannelDataKind(channelDataKind)
        , BlobContent(std::move(blobContent))
        , ZeroBlocks(std::move(zeroBlocks))
        , AffectedBlobs(std::move(affectedBlobs))
        , AffectedBlocks(std::move(affectedBlocks))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TCompactionRequest
{
    NCloud::TPartialBlobId BlobId;
    NActors::TActorId Proxy;
    ui16 BlobOffset;
    ui32 BlockIndex;
    size_t IndexInBlobContent;
    ui32 GroupId;
    ui32 RangeCompactionIndex;

    TCompactionRequest(
        const NCloud::TPartialBlobId& blobId,
        const NActors::TActorId& proxy,
        ui16 blobOffset,
        ui32 blockIndex,
        size_t indexInBlobContent,
        ui32 groupId,
        ui32 rangeCompactionIndex)
        : BlobId(blobId)
        , Proxy(proxy)
        , BlobOffset(blobOffset)
        , BlockIndex(blockIndex)
        , IndexInBlobContent(indexInBlobContent)
        , GroupId(groupId)
        , RangeCompactionIndex(rangeCompactionIndex)
    {}
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
