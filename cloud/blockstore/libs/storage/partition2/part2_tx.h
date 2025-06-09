#pragma once

#include "public.h"

#include "part2_database.h" // XXX

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition2/model/blob.h>
#include <cloud/blockstore/libs/storage/partition2/model/blob_index.h>
#include <cloud/blockstore/libs/storage/partition2/model/block.h>
#include <cloud/blockstore/libs/storage/partition2/model/block_list.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION2_TRANSACTIONS(xxx, ...)                           \
    xxx(InitSchema,             __VA_ARGS__)                                   \
    xxx(LoadState,              __VA_ARGS__)                                   \
    xxx(ZeroBlocks,             __VA_ARGS__)                                   \
    xxx(InitIndex,              __VA_ARGS__)                                   \
    xxx(ReadBlocks,             __VA_ARGS__)                                   \
    xxx(AddBlobs,               __VA_ARGS__)                                   \
    xxx(Compaction,             __VA_ARGS__)                                   \
    xxx(Cleanup,                __VA_ARGS__)                                   \
    xxx(UpdateIndexStructures,  __VA_ARGS__)                                   \
    xxx(CollectGarbage,         __VA_ARGS__)                                   \
    xxx(AddGarbage,             __VA_ARGS__)                                   \
    xxx(DeleteGarbage,          __VA_ARGS__)                                   \
    xxx(CreateCheckpoint,       __VA_ARGS__)                                   \
    xxx(DeleteCheckpoint,       __VA_ARGS__)                                   \
    xxx(DescribeRange,          __VA_ARGS__)                                   \
    xxx(DescribeBlob,           __VA_ARGS__)                                   \
    xxx(GetChangedBlocks,       __VA_ARGS__)                                   \
    xxx(DescribeBlocks,         __VA_ARGS__)                                   \
// BLOCKSTORE_PARTITION2_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxPartition
{
    //
    // InitSchema
    //

    struct TInitSchema
    {
        const TRequestInfoPtr RequestInfo;

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
        TFreshBlockUpdates FreshBlockUpdates;
        TVector<TPartitionDatabase::TBlobMeta> Blobs;
        TVector<TBlobUpdate> BlobUpdates;
        TVector<TPartitionDatabase::TBlobGarbage> BlobGarbage;
        TVector<NProto::TCheckpointMeta> Checkpoints;
        TVector<TVector<TPartialBlobId>> DeletedCheckpointBlobIds;
        TVector<TCompactionCounter> CompactionMap;
        TVector<TPartialBlobId> GarbageBlobs;
        TVector<TPartialBlobId> ZoneBlobIds;

        void Clear()
        {
            Meta.Clear();
            FreshBlockUpdates.clear();
            Blobs.clear();
            BlobUpdates.clear();
            BlobGarbage.clear();
            Checkpoints.clear();
            DeletedCheckpointBlobIds.clear();
            CompactionMap.clear();
            GarbageBlobs.clear();
            ZoneBlobIds.clear();
        }
    };

    //
    // ZeroBlocks
    //

    struct TZeroBlocks
    {
        const TRequestInfoPtr RequestInfo;
        const TBlockRange32 WriteRange;

        ui64 CommitId = 0;

        TZeroBlocks(
                TRequestInfoPtr requestInfo,
                const TBlockRange32& writeRange)
            : RequestInfo(std::move(requestInfo))
            , WriteRange(writeRange)
        {}

        void Clear()
        {
            CommitId = 0;
        }
    };

    struct TInitIndex
    {
        const TRequestInfoPtr RequestInfo;
        const NActors::TActorId ActorId;
        const TVector<TBlockRange32> BlockRanges;

        TInitIndex(
                const NActors::TActorId& actorId,
                TVector<TBlockRange32> blockRanges)
            : ActorId(actorId)
            , BlockRanges(std::move(blockRanges))
        {
        }

        void Clear()
        {
        }
    };

    //
    // ReadBlocks
    //

    struct TReadBlocks
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 CheckpointId;
        const TBlockRange32 ReadRange;
        const IReadBlocksHandlerPtr ReadHandler;
        const bool ReplyLocal;
        const bool ShouldReportBlobIdsOnFailure;

        struct TBlockMark
        {
            ui64 MinCommitId = 0;
            ui64 MaxCommitId = 0;
            NKikimr::TLogoBlobID BlobId;
            ui32 BSGroupId = 0;
            ui16 BlobOffset = 0;
            bool Empty = true;
        };

        ui64 CommitId = 0;
        size_t BlocksToRead = 0;
        TVector<TBlockMark> Blocks;

        TVector<IProfileLog::TBlockInfo> BlockInfos;

        TReadBlocks(
                TRequestInfoPtr requestInfo,
                ui64 checkpointId,
                const TBlockRange32& readRange,
                IReadBlocksHandlerPtr readHandler,
                bool replyLocal,
                bool shouldReportBlobIdsOnFailure)
            : RequestInfo(std::move(requestInfo))
            , CheckpointId(checkpointId)
            , ReadRange(readRange)
            , ReadHandler(std::move(readHandler))
            , ReplyLocal(replyLocal)
            , ShouldReportBlobIdsOnFailure(shouldReportBlobIdsOnFailure)
            , Blocks(ReadRange.Size())
        {}

        void Clear()
        {
            CommitId = 0;
            BlocksToRead = 0;
            std::fill(Blocks.begin(), Blocks.end(), TBlockMark());
            ReadHandler->Clear();
            BlockInfos.clear();
        }
    };

    //
    // AddBlobs
    //

    struct TAddBlobs
    {
        const TRequestInfoPtr RequestInfo;
        const EAddBlobMode Mode;
        TVector<TAddBlob> NewBlobs;
        const TGarbageInfo GarbageInfo;
        TAffectedBlobInfos AffectedBlobInfos;
        const ui32 BlobsSkippedByCompaction;
        const ui32 BlocksSkippedByCompaction;
        TVector<IProfileLog::TBlockCommitId> BlockCommitIds;

        bool Success = true;

        TAddBlobs(
                TRequestInfoPtr requestInfo,
                EAddBlobMode mode,
                TVector<TAddBlob> newBlobs,
                TGarbageInfo garbageInfo,
                TAffectedBlobInfos affectedBlobInfos,
                ui32 blobsSkippedByCompaction,
                ui32 blocksSkippedByCompaction)
            : RequestInfo(std::move(requestInfo))
            , Mode(mode)
            , NewBlobs(std::move(newBlobs))
            , GarbageInfo(std::move(garbageInfo))
            , AffectedBlobInfos(std::move(affectedBlobInfos))
            , BlobsSkippedByCompaction(blobsSkippedByCompaction)
            , BlocksSkippedByCompaction(blocksSkippedByCompaction)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // Compaction
    //

    struct TCompaction
    {
        const TRequestInfoPtr RequestInfo;
        const TBlockRange32 BlockRange;
        const TCompactionOptions CompactionOptions;
        TGarbageInfo GarbageInfo;

        ui64 CommitId = 0;
        TBlobRefsList Blobs;
        TAffectedBlobInfos AffectedBlobInfos;
        ui32 BlobsSkipped = 0;
        ui32 BlocksSkipped = 0;

        TCompaction(
                TRequestInfoPtr requestInfo,
                const TBlockRange32& blockRange,
                TCompactionOptions compactionOptions)
            : RequestInfo(std::move(requestInfo))
            , BlockRange(blockRange)
            , CompactionOptions(compactionOptions)
        {}

        TCompaction(
                TRequestInfoPtr requestInfo,
                TGarbageInfo garbageInfo)
            : RequestInfo(std::move(requestInfo))
            , GarbageInfo(std::move(garbageInfo))
        {}

        void Clear()
        {
            CommitId = 0;
            Blobs.clear();
            AffectedBlobInfos.clear();
            BlobsSkipped = 0;
            BlocksSkipped = 0;
        }
    };

    //
    // Cleanup
    //

    struct TCleanup
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 CommitId;
        const TEvPartitionPrivate::ECleanupMode Mode;

        using TBlockList = TVector<TBlock>;

        ui64 CollectBarrierCommitId = 0;

        TVector<TPartialBlobId> ExistingBlobIds;
        TVector<TBlockList> BlockLists;
        TVector<IProfileLog::TBlockCommitId> BlockCommitIds;
        TVector<TBlobUpdate> BlobUpdates;

        TCleanup(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                TEvPartitionPrivate::ECleanupMode mode)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , Mode(mode)
        {}

        void Clear()
        {
            ExistingBlobIds.clear();
            BlockLists.clear();
        }
    };

    //
    // UpdateIndexStructures
    //

    struct TUpdateIndexStructures
    {
        const TRequestInfoPtr RequestInfo;
        const TBlockRange32 BlockRange;

        TVector<TBlockRange64> ConvertedToMixedIndex;
        TVector<TBlockRange64> ConvertedToRangeMap;

        TUpdateIndexStructures(
                TRequestInfoPtr requestInfo,
                const TBlockRange32& blockRange)
            : RequestInfo(std::move(requestInfo))
            , BlockRange(blockRange)
        {}

        void Clear()
        {
            ConvertedToMixedIndex.clear();
            ConvertedToRangeMap.clear();
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
        NProto::TCheckpointMeta Meta;

        ui64 CommitId = 0;

        TCreateCheckpoint(
                TRequestInfoPtr requestInfo,
                NProto::TCheckpointMeta meta)
            : RequestInfo(std::move(requestInfo))
            , Meta(std::move(meta))
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
        const TRequestInfoPtr RequestInfo;
        const ui64 CommitId;
        const TString CheckpointId;
        NProto::TError Error;
        TVector<TPartialBlobId> BlobIds;

        TDeleteCheckpoint(
                TRequestInfoPtr requestInfo,
                ui64 commitId,
                TString checkpointId)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
            , CheckpointId(std::move(checkpointId))
        {}

        void Clear()
        {
            BlobIds.clear();
        }
    };

    //
    // DescribeRange
    //

    struct TDescribeRange
    {
        const TRequestInfoPtr RequestInfo;
        const TBlockRange32 BlockRange;

        TVector<TBlockRef> Blocks;

        TDescribeRange(
                TRequestInfoPtr requestInfo,
                const TBlockRange32& blockRange)
            : RequestInfo(std::move(requestInfo))
            , BlockRange(blockRange)
        {}

        void Clear()
        {
            Blocks.clear();
        }
    };

    //
    // DescribeBlob
    //

    struct TDescribeBlob
    {
        const TRequestInfoPtr RequestInfo;
        const TPartialBlobId BlobId;

        TVector<TBlockRef> Blocks;

        TDescribeBlob(
                TRequestInfoPtr requestInfo,
                const TPartialBlobId& blobId)
            : RequestInfo(std::move(requestInfo))
            , BlobId(blobId)
        {}

        void Clear()
        {
            Blocks.clear();
        }
    };

    //
    // GetChangedBlocks
    //

    struct TGetChangedBlocks
    {
        const TRequestInfoPtr RequestInfo;
        const TBlockRange32 ReadRange;
        const ui64 LowCommitId;
        const ui64 HighCommitId;
        const bool IgnoreBaseDisk;

        struct TBlockMeta
        {
            ui64 LowCommitId = InvalidCommitId;
            ui64 HighCommitId = InvalidCommitId;
        };
        TVector<TBlockMeta> Blocks;

        TVector<ui8> ChangedBlocks;

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
            , Blocks(ReadRange.Size())
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

        void MarkBlock(const TBlock& block, bool low)
        {
            const auto i = block.BlockIndex - ReadRange.Start;
            if (low) {
                Blocks[i].LowCommitId = block.MinCommitId;
            } else {
                Blocks[i].HighCommitId = block.MinCommitId;
            }
        }

        void Finish()
        {
            for (ui32 i = 0; i < Blocks.size(); ++i) {
                if (Blocks[i].LowCommitId != Blocks[i].HighCommitId) {
                    ChangedBlocks[i / 8] |= 1 << (i % 8);
                }
            }
        }
    };

    //
    // DescribeBlocks
    //

    struct TDescribeBlocks
    {
        const TRequestInfoPtr RequestInfo;
        ui64 CommitId;
        const TBlockRange32 DescribeRange;

        struct TBlockMark
        {
            TBlockMark() = default;

            TBlockMark(
                    ui32 blockIndex,
                    ui64 minCommitId,
                    TString content)
                : BlockIndex(blockIndex)
                , MinCommitId(minCommitId)
                , Content(std::move(content))
            {}

            TBlockMark(
                    ui32 blockIndex,
                    ui64 minCommitId,
                    const TPartialBlobId& blobId,
                    ui16 blobOffset)
                : BlockIndex(blockIndex)
                , MinCommitId(minCommitId)
                , BlobId(blobId)
                , BlobOffset(blobOffset)
            {}

            bool operator <(const TBlockMark& other) const
            {
                return BlobId < other.BlobId ||
                    (BlobId == other.BlobId && BlobOffset < other.BlobOffset);
            }

            ui32 BlockIndex = 0;
            ui64 MinCommitId = 0;
            TPartialBlobId BlobId;
            ui16 BlobOffset = 0;
            TString Content;
        };

        TVector<TBlockMark> Marks;

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
            ui64 minCommitId,
            TStringBuf content)
        {
            auto& mark = Marks[GetBlockMarkIndex(blockIndex)];

            if (mark.MinCommitId < minCommitId) {
                mark = TBlockMark(blockIndex, minCommitId, TString{content});
            }
        }

        void MarkBlock(
            ui32 blockIndex,
            ui64 minCommitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset)
        {
            auto& mark = Marks[GetBlockMarkIndex(blockIndex)];

            if (mark.MinCommitId < minCommitId) {
                mark = TBlockMark(blockIndex, minCommitId, blobId, blobOffset);
            }
        }
    };
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
