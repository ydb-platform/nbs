#pragma once

#include "public.h"

#include "part_events_private.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/compaction_map.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition/model/blob_to_confirm.h>
#include <cloud/blockstore/libs/storage/partition/model/block.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>
#include <cloud/blockstore/libs/storage/partition/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition/model/cleanup_queue.h>
#include <cloud/blockstore/libs/storage/partition/model/garbage_queue.h>
#include <cloud/blockstore/libs/storage/partition_common/model/blob_markers.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/compressed_bitmap.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_TRANSACTIONS(xxx, ...)                            \
    xxx(InitSchema,                 __VA_ARGS__)                               \
    xxx(LoadState,                  __VA_ARGS__)                               \
    xxx(WriteBlocks,                __VA_ARGS__)                               \
    xxx(ZeroBlocks,                 __VA_ARGS__)                               \
    xxx(ReadBlocks,                 __VA_ARGS__)                               \
    xxx(AddBlobs,                   __VA_ARGS__)                               \
    xxx(Compaction,                 __VA_ARGS__)                               \
    xxx(Cleanup,                    __VA_ARGS__)                               \
    xxx(CollectGarbage,             __VA_ARGS__)                               \
    xxx(AddGarbage,                 __VA_ARGS__)                               \
    xxx(DeleteGarbage,              __VA_ARGS__)                               \
    xxx(CreateCheckpoint,           __VA_ARGS__)                               \
    xxx(DeleteCheckpoint,           __VA_ARGS__)                               \
    xxx(DescribeRange,              __VA_ARGS__)                               \
    xxx(DescribeBlob,               __VA_ARGS__)                               \
    xxx(CheckIndex,                 __VA_ARGS__)                               \
    xxx(GetChangedBlocks,           __VA_ARGS__)                               \
    xxx(DescribeBlocks,             __VA_ARGS__)                               \
    xxx(FlushToDevNull,             __VA_ARGS__)                               \
    xxx(GetUsedBlocks,              __VA_ARGS__)                               \
    xxx(UpdateLogicalUsedBlocks,    __VA_ARGS__)                               \
    xxx(MetadataRebuildUsedBlocks,  __VA_ARGS__)                               \
    xxx(MetadataRebuildBlockCount,  __VA_ARGS__)                               \
    xxx(ScanDiskBatch,              __VA_ARGS__)                               \
    xxx(AddUnconfirmedBlobs,        __VA_ARGS__)                               \
    xxx(ConfirmBlobs,               __VA_ARGS__)                               \
// BLOCKSTORE_PARTITION_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxPartition
{
    using TBlockMark = NBlobMarkers::TBlockMark;
    using TBlockMarks = NBlobMarkers::TBlockMarks;

    //
    // InitSchema
    //

    struct TInitSchema
    {
        ui64 BlocksCount;
        const TRequestInfoPtr RequestInfo;

        explicit TInitSchema(ui64 blocksCount)
            : BlocksCount(blocksCount)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // LoadState
    //

    struct TLoadState
    {
        const TRequestInfoPtr RequestInfo;

        TMaybe<NProto::TPartitionMeta> Meta;

        TVector<TOwningFreshBlock> FreshBlocks;
        TVector<TCompactionCounter> CompactionMap;
        TCompressedBitmap UsedBlocks;
        TCompressedBitmap LogicalUsedBlocks;
        bool ReadLogicalUsedBlocks = false;
        TVector<TCheckpoint> Checkpoints;
        THashMap<TString, ui64> CheckpointId2CommitId;
        TVector<TCleanupQueueItem> CleanupQueue;
        TVector<TPartialBlobId> NewBlobs;
        TVector<TPartialBlobId> GarbageBlobs;
        TCommitIdToBlobsToConfirm UnconfirmedBlobs;

        explicit TLoadState(ui64 blocksCount)
            : UsedBlocks(blocksCount)
            , LogicalUsedBlocks(blocksCount)
        {}

        void Clear()
        {
            Meta.Clear();

            FreshBlocks.clear();
            CompactionMap.clear();
            UsedBlocks.Clear();
            LogicalUsedBlocks.Clear();
            ReadLogicalUsedBlocks = false;
            Checkpoints.clear();
            CheckpointId2CommitId.clear();
            CleanupQueue.clear();
            NewBlobs.clear();
            GarbageBlobs.clear();
            UnconfirmedBlobs.clear();
        }
    };

    //
    // WriteBlocks
    //

    struct TWriteBlocks
    {
        struct TSubRequestInfo
        {
            TRequestInfoPtr RequestInfo;
            TBlockRange32 Range;
            const IWriteBlocksHandlerPtr WriteHandler;
            bool Empty = false;
            bool ReplyLocal = false;

            TSubRequestInfo() = default;

            TSubRequestInfo(
                    TRequestInfoPtr requestInfo,
                    TBlockRange32 range,
                    IWriteBlocksHandlerPtr writeHandler,
                    bool empty,
                    bool replyLocal)
                : RequestInfo(std::move(requestInfo))
                , Range(range)
                , WriteHandler(std::move(writeHandler))
                , Empty(empty)
                , ReplyLocal(replyLocal)
            {
            }
        };

        const ui64 CommitId = 0;
        bool Interrupted = false;

        const TVector<TSubRequestInfo> Requests;

        TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

        TWriteBlocks(
                ui64 commitId,
                TSubRequestInfo request)
            : CommitId(commitId)
            , Requests(1, std::move(request))
        {}

        TWriteBlocks(
                ui64 commitId,
                TVector<TSubRequestInfo> requests)
            : CommitId(commitId)
            , Requests(std::move(requests))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // ZeroBlocks
    //

    struct TZeroBlocks
    {
        const TRequestInfoPtr RequestInfo;

        const ui64 CommitId;
        const TBlockRange32 WriteRange;

        TZeroBlocks(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                const TBlockRange32& writeRange)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , WriteRange(writeRange)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // ReadBlocks
    //

    struct TReadBlocks
    {
        const TRequestInfoPtr RequestInfo;

        const ui64 CommitId;
        const TBlockRange32 ReadRange;
        const IReadBlocksHandlerPtr ReadHandler;
        const bool ReplyLocal;
        const bool ShouldReportBlobIdsOnFailure;
        bool ChecksumsEnabled = false;
        bool Interrupted = false;

        TBlockMarks BlockMarks;
        TVector<ui64> BlockMarkCommitIds;
        THashMap<TPartialBlobId, NProto::TBlobMeta, TPartialBlobIdHash> BlobId2Meta;

        TVector<IProfileLog::TBlockInfo> BlockInfos;

        TReadBlocks(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                const TBlockRange32& readRange,
                IReadBlocksHandlerPtr readHandler,
                bool replyLocal,
                bool shouldReportBlobIdsOnFailure)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , ReadRange(readRange)
            , ReadHandler(std::move(readHandler))
            , ReplyLocal(replyLocal)
            , ShouldReportBlobIdsOnFailure(shouldReportBlobIdsOnFailure)
            , BlockMarks(ReadRange.Size())
            , BlockMarkCommitIds(ReadRange.Size(), 0)
        {}

        void Clear()
        {
            ReadHandler->Clear();
            ChecksumsEnabled = false;
            std::fill(BlockMarks.begin(), BlockMarks.end(), NBlobMarkers::TEmptyMark());
            std::fill(BlockMarkCommitIds.begin(), BlockMarkCommitIds.end(), 0);
            BlobId2Meta.clear();
            BlockInfos.clear();
        }

        ui64& GetBlockMarkCommitId(ui32 blockIndex)
        {
            Y_DEBUG_ABORT_UNLESS(ReadRange.Contains(blockIndex));
            return BlockMarkCommitIds[blockIndex - ReadRange.Start];
        }

        TBlockMark& GetBlockMark(ui32 blockIndex)
        {
            Y_DEBUG_ABORT_UNLESS(ReadRange.Contains(blockIndex));
            return BlockMarks[blockIndex - ReadRange.Start];
        }

        bool MarkBlock(ui32 blockIndex, ui64 commitId, TBlockMark mark)
        {
            auto& outputMark = GetBlockMark(blockIndex);
            auto& outputMarkCommitId = GetBlockMarkCommitId(blockIndex);

            if (outputMarkCommitId < commitId) {
                outputMarkCommitId = commitId;
                outputMark = std::move(mark);
                return true;
            }

            return false;
        }
    };

    //
    // AddBlobs
    //

    struct TAddBlobs
    {
        const TRequestInfoPtr RequestInfo;

        const ui64 CommitId;
        const TVector<TAddMixedBlob> MixedBlobs;
        const TVector<TAddMergedBlob> MergedBlobs;
        const TVector<TAddFreshBlob> FreshBlobs;
        const EAddBlobMode Mode;

        // compaction
        const TAffectedBlobs AffectedBlobs;
        const TAffectedBlocks AffectedBlocks;
        const TVector<TBlobCompactionInfo> MixedBlobCompactionInfos;
        const TVector<TBlobCompactionInfo> MergedBlobCompactionInfos;

        ui64 DeletionCommitId = 0;

        TAddBlobs(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                TVector<TAddMixedBlob> mixedBlobs,
                TVector<TAddMergedBlob> mergedBlobs,
                TVector<TAddFreshBlob> freshBlobs,
                EAddBlobMode mode,
                TAffectedBlobs affectedBlobs,
                TAffectedBlocks affectedBlocks,
                TVector<TBlobCompactionInfo> mixedBlobCompactionInfos,
                TVector<TBlobCompactionInfo> mergedBlobCompactionInfos)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , MixedBlobs(std::move(mixedBlobs))
            , MergedBlobs(std::move(mergedBlobs))
            , FreshBlobs(std::move(freshBlobs))
            , Mode(mode)
            , AffectedBlobs(std::move(affectedBlobs))
            , AffectedBlocks(std::move(affectedBlocks))
            , MixedBlobCompactionInfos(std::move(mixedBlobCompactionInfos))
            , MergedBlobCompactionInfos(std::move(mergedBlobCompactionInfos))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // Compaction
    //

    struct TRangeCompaction
    {
        const ui32 RangeIdx;
        const TBlockRange32 BlockRange;

        struct TBlockMark
        {
            TPartialBlobId BlobId;
            ui64 CommitId = 0;
            ui16 BlobOffset = 0;
            TString BlockContent;
        };

        TVector<TBlockMark> BlockMarks;
        TAffectedBlobs AffectedBlobs;
        TAffectedBlocks AffectedBlocks;
        ui32 BlobsSkipped = 0;
        ui32 BlocksSkipped = 0;
        bool ChecksumsEnabled = false;

        TRangeCompaction(ui32 rangeIdx, const TBlockRange32& blockRange)
            : RangeIdx(rangeIdx)
            , BlockRange(blockRange)
            , BlockMarks(blockRange.Size())
        {}

        void Clear()
        {
            std::fill(BlockMarks.begin(), BlockMarks.end(), TBlockMark());
            AffectedBlobs.clear();
            AffectedBlocks.clear();
            BlobsSkipped = 0;
            BlocksSkipped = 0;
            ChecksumsEnabled = false;
        }

        TBlockMark& GetBlockMark(ui32 blockIndex)
        {
            Y_DEBUG_ABORT_UNLESS(BlockRange.Contains(blockIndex));
            return BlockMarks[blockIndex - BlockRange.Start];
        }

        void MarkBlock(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset,
            bool keepTrackOfAffectedBlocks)
        {
            auto& mark = GetBlockMark(blockIndex);
            if (mark.CommitId < commitId) {
                mark.CommitId = commitId;
                mark.BlobId = blobId;
                mark.BlobOffset = blobOffset;
                mark.BlockContent.clear();
            }

            auto& ab = AffectedBlobs[blobId];
            ab.Offsets.push_back(blobOffset);

            if (keepTrackOfAffectedBlocks) {
                ab.AffectedBlockIndices.push_back(blockIndex);
                AffectedBlocks.push_back({ blockIndex, commitId });
            }
        }

        void MarkBlock(
            ui32 blockIndex,
            ui64 commitId,
            TStringBuf blockContent)
        {
            auto& mark = GetBlockMark(blockIndex);
            if (mark.CommitId < commitId) {
                mark.CommitId = commitId;
                mark.BlobId = {};
                mark.BlobOffset = 0;
                mark.BlockContent.assign(blockContent);
            }
        }
    };

    struct TCompaction
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 CommitId;
        const TCompactionOptions CompactionOptions;

        TVector<TRangeCompaction> RangeCompactions;

        TCompaction(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                TCompactionOptions compactionOptions,
                const TVector<std::pair<ui32, TBlockRange32>>& ranges)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , CompactionOptions(compactionOptions)
        {
            RangeCompactions.reserve(ranges.size());
            for (const auto& range: ranges) {
                RangeCompactions.emplace_back(range.first, range.second);
            }
        }

        void Clear()
        {
            for (auto& range: RangeCompactions) {
                range.Clear();
            }
        }
    };

    //
    // MetadataRebuildUsedBlocks
    //

    struct TMetadataRebuildUsedBlocks
    {
        const TRequestInfoPtr RequestInfo;

        const TBlockRange32 BlockRange;
        struct TBlockInfo
        {
            bool Filled = false;
            ui64 MaxCommitId = 0;
        };
        TVector<TBlockInfo> BlockInfos;
        ui32 FilledBlockCount = 0;

        TMetadataRebuildUsedBlocks(
                TRequestInfoPtr requestInfo,
                const TBlockRange32& blockRange)
            : RequestInfo(std::move(requestInfo))
            , BlockRange(blockRange)
            , BlockInfos(BlockRange.Size())
        {}

        void Clear()
        {
            std::fill(BlockInfos.begin(), BlockInfos.end(), TBlockInfo());
            FilledBlockCount = 0;
        }
    };

    //
    // MetadataRebuildBlockCount
    //

    struct TMetadataRebuildBlockCount
    {
        const TRequestInfoPtr RequestInfo;
        const TPartialBlobId StartBlobId;
        const ui32 BlobCountToRead;
        const TPartialBlobId FinalBlobId;

        ui32 ReadCount = 0;

        ui64 MixedBlockCount = 0;
        ui64 MergedBlockCount = 0;

        TPartialBlobId LastReadBlobId;

        TBlockCountRebuildState RebuildState;

        TMetadataRebuildBlockCount(
                TRequestInfoPtr requestInfo,
                TPartialBlobId startBlobId,
                ui32 blobCountToRead,
                TPartialBlobId finalBlobId,
                const TBlockCountRebuildState& rebuildState)
            : RequestInfo(std::move(requestInfo))
            , StartBlobId(startBlobId)
            , BlobCountToRead(blobCountToRead)
            , FinalBlobId(finalBlobId)
            , RebuildState(rebuildState)
        {}

        void Clear()
        {
            ReadCount = 0;
            MixedBlockCount = 0;
            MergedBlockCount = 0;
            LastReadBlobId = {};
        }
    };

    //
    // ScanDiskBatch
    //

    struct TScanDiskBatch
    {
        const TRequestInfoPtr RequestInfo;
        const TPartialBlobId StartBlobId;
        const ui32 BlobCountToVisit = 0;
        const TPartialBlobId FinalBlobId;

        using TBlobMark =
            TEvPartitionPrivate::TScanDiskBatchResponse::TBlobMark;

        TVector<TBlobMark> BlobsToReadInCurrentBatch;
        ui32 VisitCount = 0;
        TPartialBlobId LastVisitedBlobId;

        TScanDiskBatch(
                TRequestInfoPtr requestInfo,
                TPartialBlobId startBlobId,
                ui32 blobCountToRead,
                TPartialBlobId finalBlobId)
            : RequestInfo(std::move(requestInfo))
            , StartBlobId(startBlobId)
            , BlobCountToVisit(blobCountToRead)
            , FinalBlobId(finalBlobId)
        {}

        void Clear()
        {
            BlobsToReadInCurrentBatch.clear();
            VisitCount = 0;
            LastVisitedBlobId = TPartialBlobId();
        }
    };

    //
    // Cleanup
    //

    struct TCleanup
    {
        const TRequestInfoPtr RequestInfo;

        const ui64 CommitId;
        const TVector<TCleanupQueueItem> CleanupQueue;

        TVector<NProto::TBlobMeta> BlobsMeta;

        TCleanup(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                TVector<TCleanupQueueItem> cleanupQueue)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , CleanupQueue(std::move(cleanupQueue))
        {}

        void Clear()
        {
            BlobsMeta.clear();
        }
    };

    //
    // CollectGarbage
    //

    struct TCollectGarbage
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 CollectCommitId;

        TVector<TPartialBlobId> KnownBlobIds;

        TCollectGarbage(TRequestInfoPtr requestInfo, ui64 collectCommitId)
            : RequestInfo(std::move(requestInfo))
            , CollectCommitId(collectCommitId)
        {}

        void Clear()
        {
            KnownBlobIds.clear();
        }
    };

    //
    // AddGarbage
    //

    struct TAddGarbage
    {
        const TRequestInfoPtr RequestInfo;

        const TVector<TPartialBlobId> BlobIds;

        TVector<TPartialBlobId> KnownBlobIds;

        TAddGarbage(
                TRequestInfoPtr requestInfo,
                TVector<TPartialBlobId> blobIds)
            : RequestInfo(std::move(requestInfo))
            , BlobIds(std::move(blobIds))
        {}

        void Clear()
        {
            KnownBlobIds.clear();
        }
    };

    //
    // DeleteGarbage
    //

    struct TDeleteGarbage
    {
        const TRequestInfoPtr RequestInfo;

        const ui64 CommitId;
        const TVector<TPartialBlobId> NewBlobs;
        const TVector<TPartialBlobId> GarbageBlobs;

        TDeleteGarbage(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                TVector<TPartialBlobId> newBlobs,
                TVector<TPartialBlobId> garbageBlobs)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , NewBlobs(std::move(newBlobs))
            , GarbageBlobs(std::move(garbageBlobs))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // CreateCheckpoint
    //

    struct TCreateCheckpoint
    {
        const TRequestInfoPtr RequestInfo;

        const TCheckpoint Checkpoint;

        bool WithoutData;

        NProto::TError Error;

        TCreateCheckpoint(
                TRequestInfoPtr requestInfo,
                TCheckpoint checkpoint,
                bool withoutData)
            : RequestInfo(std::move(requestInfo))
            , Checkpoint(std::move(checkpoint))
            , WithoutData(withoutData)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // DeleteCheckpoint
    //

    struct TDeleteCheckpoint
    {
        using TReplyCallback = std::function<void(
            const NActors::TActorContext& ctx,
            TRequestInfoPtr requestInfo,
            const NProto::TError& error)>;

        const TRequestInfoPtr RequestInfo;

        const TString CheckpointId;

        const TReplyCallback ReplyCallback;

        const bool DeleteOnlyData;

        NProto::TError Error;

        TDeleteCheckpoint(
                TRequestInfoPtr requestInfo,
                TString checkpointId,
                TReplyCallback replyCallback,
                bool deleteOnlyData)
            : RequestInfo(std::move(requestInfo))
            , CheckpointId(std::move(checkpointId))
            , ReplyCallback(std::move(replyCallback))
            , DeleteOnlyData(deleteOnlyData)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // DescribeRange
    //

    struct TDescribeRange
    {
        const TRequestInfoPtr RequestInfo;

        const TBlockRange32 BlockRange;
        const TString BlockFilter;

        struct TBlockMark
        {
            TPartialBlobId BlobId;
            ui64 CommitId = 0;
            ui32 BlockIndex = 0;
            ui16 BlobOffset = 0;
        };

        TVector<TBlockMark> BlockMarks;

        TDescribeRange(
                TRequestInfoPtr requestInfo,
                const TBlockRange32& blockRange,
                TString blockFilter)
            : RequestInfo(std::move(requestInfo))
            , BlockRange(blockRange)
            , BlockFilter(std::move(blockFilter))
        {}

        void Clear()
        {
            BlockMarks.clear();
        }

        void MarkBlock(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset)
        {
            Y_DEBUG_ABORT_UNLESS(BlockRange.Contains(blockIndex));
            BlockMarks.push_back({
                blobId,
                commitId,
                blockIndex,
                blobOffset,
            });
        }
    };

    //
    // DescribeBlob
    //

    struct TDescribeBlob
    {
        const TRequestInfoPtr RequestInfo;

        const TPartialBlobId BlobId;

        struct TBlockMark
        {
            ui64 CommitId = 0;
            ui32 BlockIndex = 0;
            ui16 BlobOffset = 0;
            ui32 Checksum = 0;
        };

        TVector<TBlockMark> BlockMarks;

        TDescribeBlob(
                TRequestInfoPtr requestInfo,
                const TPartialBlobId& blobId)
            : RequestInfo(std::move(requestInfo))
            , BlobId(blobId)
        {}

        void Clear()
        {
            BlockMarks.clear();
        }

        void MarkBlock(
            ui32 blockIndex,
            ui64 commitId,
            ui16 blobOffset,
            ui32 checksum)
        {
            BlockMarks.push_back({
                commitId,
                blockIndex,
                blobOffset,
                checksum,
            });
        }
    };

    //
    // CheckIndex
    //

    struct TCheckIndex
    {
        const TRequestInfoPtr RequestInfo;

        const TBlockRange32 BlockRange;

        struct TBlockMark
        {
            TPartialBlobId BlobId;
            ui64 CommitId = 0;
            ui32 BlockIndex = 0;
            ui16 BlobOffset = 0;
        };

        TVector<TBlockMark> BlockMarks_Index;
        TVector<TBlockMark> BlockMarks_Blobs;

        TCheckIndex(
                TRequestInfoPtr requestInfo,
                const TBlockRange32& blockRange)
            : RequestInfo(std::move(requestInfo))
            , BlockRange(blockRange)
        {}

        void Clear()
        {
            BlockMarks_Index.clear();
            BlockMarks_Blobs.clear();
        }

        void MarkBlock_Index(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset)
        {
            Y_DEBUG_ABORT_UNLESS(BlockRange.Contains(blockIndex));
            BlockMarks_Index.push_back({ blobId, commitId, blockIndex, blobOffset });
        }

        void MarkBlock_Blobs(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset)
        {
            Y_DEBUG_ABORT_UNLESS(BlockRange.Contains(blockIndex));
            BlockMarks_Blobs.push_back({ blobId, commitId, blockIndex, blobOffset });
        }
    };

    //
    // ChanedBlocks
    //

    struct TGetChangedBlocks
    {
        const TRequestInfoPtr RequestInfo;

        const TBlockRange32 ReadRange;
        const ui64 LowCommitId;
        const ui64 HighCommitId;
        const bool IgnoreBaseDisk;

        TVector<ui8> ChangedBlocks;
        bool Interrupted = false;

        TGetChangedBlocks(
                TRequestInfoPtr requestInfo,
                const TBlockRange32& readRange,
                ui64 lowCommitId,
                ui64 highCommitId,
                bool ignoreBaseDisk)
            : RequestInfo(std::move(requestInfo))
            , ReadRange(readRange)
            , LowCommitId(lowCommitId)
            , HighCommitId(highCommitId)
            , IgnoreBaseDisk(ignoreBaseDisk)
            , ChangedBlocks(GetArraySize(ReadRange.Size()))
        {}

        void Clear()
        {
            std::fill(ChangedBlocks.begin(), ChangedBlocks.end(), 0);
        }

        static ui32 GetArraySize(ui32 count)
        {
            return (count + 7) >> 3;
        }

        std::pair<ui32, ui32> GetBitPosition(ui32 blockIndex)
        {
            auto blockOffset = blockIndex - ReadRange.Start;
            return {blockOffset / 8, blockOffset % 8};
        }

        void MarkBlock(ui32 blockIndex, ui64 commitId)
        {
            if (commitId > LowCommitId) {
                auto p = GetBitPosition(blockIndex);
                ChangedBlocks[p.first] |= 1 << p.second;
            }
        }
    };

    //
    // DescribeBlocks
    //

    struct TDescribeBlocks
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 CommitId;
        const TBlockRange32 DescribeRange;

        struct TBlockMark
        {
            TBlockMark() = default;

            TBlockMark(
                    ui32 blockIndex,
                    ui64 commitId,
                    TString content)
                : BlockIndex(blockIndex)
                , CommitId(commitId)
                , Content(std::move(content))
            {}

            TBlockMark(
                    ui32 blockIndex,
                    ui64 commitId,
                    const TPartialBlobId& blobId,
                    ui16 blobOffset)
                : BlockIndex(blockIndex)
                , CommitId(commitId)
                , BlobId(blobId)
                , BlobOffset(blobOffset)
            {}

            bool operator <(const TBlockMark& other) const
            {
                return BlobId < other.BlobId ||
                    (BlobId == other.BlobId && BlobOffset < other.BlobOffset);
            }

            ui32 BlockIndex = 0;
            ui64 CommitId = 0;
            TPartialBlobId BlobId;
            ui16 BlobOffset = 0;
            TString Content;
        };

        TVector<TBlockMark> Marks;
        bool Interrupted = false;

        TDescribeBlocks(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                const TBlockRange32& describeRange)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , DescribeRange(describeRange)
            , Marks(DescribeRange.Size())
        {}

        void Clear()
        {
            std::fill(Marks.begin(), Marks.end(), TBlockMark());
        }

        ui32 GetBlockMarkIndex(ui32 blockIndex)
        {
            Y_DEBUG_ABORT_UNLESS(DescribeRange.Contains(blockIndex));
            return blockIndex - DescribeRange.Start;
        }

        void MarkBlock(
            ui32 blockIndex,
            ui64 commitId,
            TStringBuf content)
        {
            auto& mark = Marks[GetBlockMarkIndex(blockIndex)];

            if (mark.CommitId < commitId) {
                mark = TBlockMark(blockIndex, commitId, TString{content});
            }
        }

        void MarkBlock(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset)
        {
            auto& mark = Marks[GetBlockMarkIndex(blockIndex)];

            if (mark.CommitId < commitId) {
                mark = TBlockMark(blockIndex, commitId, blobId, blobOffset);
            }
        }
    };

    //
    // FlushToDevNull
    //

    struct TFlushToDevNull
    {
        const TRequestInfoPtr RequestInfo;
        TVector<TBlock> FreshBlocks;

        TFlushToDevNull(
                TRequestInfoPtr requestInfo,
                TVector<TBlock> freshBlocks)
            : RequestInfo(std::move(requestInfo))
            , FreshBlocks(std::move(freshBlocks))
        {
        }

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // GetUsedBlocks
    //

    struct TGetUsedBlocks
    {
        const TRequestInfoPtr RequestInfo;
        google::protobuf::RepeatedPtrField<NProto::TUsedBlockData> UsedBlocks;

        TGetUsedBlocks(TRequestInfoPtr requestInfo)
            : RequestInfo(std::move(requestInfo))
        {}

        void Clear()
        {
            UsedBlocks.Clear();
        }
    };

    //
    // UpdateLogicalUsedBlocks
    //

    struct TUpdateLogicalUsedBlocks
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 UpdateFromIdx = 0;

        ui64 UpdatedToIdx = 0;

        TUpdateLogicalUsedBlocks(ui32 updateFromIdx)
            : UpdateFromIdx(updateFromIdx)
        {}

        void Clear()
        {
            UpdatedToIdx = 0;
        }
    };

    //
    // AddUnconfirmedBlobs
    //

    struct TAddUnconfirmedBlobs
    {
        const TRequestInfoPtr RequestInfo;
        ui64 CommitId = 0;
        TVector<TBlobToConfirm> Blobs;

        TAddUnconfirmedBlobs(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                TVector<TBlobToConfirm> blobs)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , Blobs(std::move(blobs))
        {}

        void Clear()
        {
            // Nothing to do.
        }
    };

    //
    // ConfirmBlobs
    //

    struct TConfirmBlobs
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 StartCycleCount;
        TVector<TPartialBlobId> UnrecoverableBlobs;

        TConfirmBlobs(
                ui64 startCycleCount,
                TVector<TPartialBlobId> unrecoverableBlobs)
            : StartCycleCount(startCycleCount)
            , UnrecoverableBlobs(std::move(unrecoverableBlobs))
        {}

        void Clear()
        {
            // Nothing to do.
        }
    };
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
