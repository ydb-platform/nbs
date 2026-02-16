#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>  // TODO:_ ???
#include <cloud/blockstore/libs/storage/partition/model/affected.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/protos/blobstorage.pb.h>  // TODO:_ ???
#include <contrib/ydb/library/actors/core/actorid.h>  // TODO:_ ???

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
    const ui32 RangeCompactionIndex;

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
        ui32 rangeCompactionIndex,
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
        , RangeCompactionIndex(rangeCompactionIndex)
        , BlobContent(std::move(blobContent))
        , ZeroBlocks(std::move(zeroBlocks))
        , AffectedBlobs(std::move(affectedBlobs))
        , AffectedBlocks(std::move(affectedBlocks))
    {}
};

struct TCompactionReadRequest
{
    NCloud::TPartialBlobId BlobId;
    NActors::TActorId Proxy;
    ui16 BlobOffset;
    ui32 BlockIndex;
    size_t IndexInBlobContent;
    ui32 GroupId;
    ui32 RangeCompactionIndex;

    TCompactionReadRequest(
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

struct TCompactionRange {
    const ui32 RangeIdx;
    TBlockRange32 BlockRange;
    const ui32 RangeCompactionIndex;   // Index of the range in its batch for
                                       // batch compaction.

    TCompactionRange(
        ui32 rangeIdx,
        TBlockRange32 blockRange,
        ui32 rangeCompactionIndex)
        : RangeIdx(rangeIdx)
        , BlockRange(blockRange)
        , RangeCompactionIndex(rangeCompactionIndex)
    {}
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
