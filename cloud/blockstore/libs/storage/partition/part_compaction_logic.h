#pragma once

#include "part_events_private.h"
#include "part_tx.h"

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
    class TTabletStorageInfo;
}

namespace NCloud::NBlockStore::NStorage::NPartition {

class TPartitionState;
class TPartitionDatabase;

////////////////////////////////////////////////////////////////////////////////

struct TChecksumFixup
{
    // Index into TRangeCompactionInfo::BlockChecksums that must be filled
    // with the checksum from AffectedBlobs[BlobId].BlobMeta after the
    // CompactionReadBlobInfo TX response arrives.
    size_t ChecksumIndex = 0;
    TPartialBlobId BlobId;
    ui16 BlobOffset = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRangeCompactionInfo
{
    const TBlockRange32 BlockRange;
    const TPartialBlobId OriginalBlobId;
    const TPartialBlobId DataBlobId;
    const TBlockMask DataBlobSkipMask;
    const TPartialBlobId ZeroBlobId;
    const TBlockMask ZeroBlobSkipMask;
    const ui32 BlobsSkippedByCompaction;
    const ui32 BlocksSkippedByCompaction;
    TVector<std::optional<ui32>> BlockChecksums;
    const EChannelDataKind ChannelDataKind;

    TGuardedBuffer<TBlockBuffer> BlobContent;
    TVector<ui32> ZeroBlocks;
    TAffectedBlobs AffectedBlobs;
    TAffectedBlocks AffectedBlocks;
    TVector<ui16> UnchangedBlobOffsets;
    TVector<TChecksumFixup> ChecksumFixups;
    TArrayHolder<NKikimr::TEvBlobStorage::TEvPatch::TDiff> Diffs;
    ui32 DiffCount = 0;

    TRangeCompactionInfo(
            TBlockRange32 blockRange,
            TPartialBlobId originalBlobId,
            TPartialBlobId dataBlobId,
            TBlockMask dataBlobSkipMask,
            TPartialBlobId zeroBlobId,
            TBlockMask zeroBlobSkipMask,
            ui32 blobsSkippedByCompaction,
            ui32 blocksSkippedByCompaction,
            TVector<std::optional<ui32>> blockChecksums,
            EChannelDataKind channelDataKind,
            TBlockBuffer blobContent,
            TVector<ui32> zeroBlocks,
            TAffectedBlobs affectedBlobs,
            TAffectedBlocks affectedBlocks,
            TVector<TChecksumFixup> checksumFixups);
};

void ApplyChecksumFixups(TRangeCompactionInfo& rc);

////////////////////////////////////////////////////////////////////////////////

struct TBlobCompactionRequest
{
    TPartialBlobId BlobId;
    NActors::TActorId Proxy;
    ui16 BlobOffset;
    ui32 BlockIndex;
    size_t IndexInBlobContent;
    ui32 GroupId;
    ui32 RangeCompactionIndex;

    TBlobCompactionRequest(
            const TPartialBlobId& blobId,
            const NActors::TActorId& proxy,
            ui16 blobOffset,
            ui32 blockIndex,
            size_t indexInBlobContent,
            ui32 groupId,
            ui32 rangeCompactionIndex);
};

////////////////////////////////////////////////////////////////////////////////

void RecreateBlobMetas(TTxPartition::TRangeCompaction& args, ui64 commitId);

////////////////////////////////////////////////////////////////////////////////

void PrepareRangeCompaction(
    const TStorageConfig& config,
    const ui32 maxSkippedBlobs,
    const ui64 commitId,
    const ui64 tabletId,
    const bool readBlockMaskOnCompactionOptimizationEnabled,
    bool& ready,
    TPartitionDatabase& db,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& blobsToReadBlockMasks,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& blobsToReadBlobMetas);

void CompleteRangeCompaction(
    const bool blobPatchingEnabled,
    const ui32 mergedBlobThreshold,
    const ui64 commitId,
    const ui64 tabletId,
    const bool recreateBlobMetasEnabled,
    NKikimr::TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    TVector<TBlobCompactionRequest>& requests,
    TVector<TRangeCompactionInfo>& rangeCompactionInfos,
    ui32 maxDiffPercentageForBlobPatching);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
