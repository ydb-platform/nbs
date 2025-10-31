#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/core/compaction_type.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/model/channel_permissions.h>
#include <cloud/blockstore/libs/storage/partition/model/blob_to_confirm.h>
#include <cloud/blockstore/libs/storage/partition/model/block.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/lwtrace/shuttle.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

enum EAddBlobMode
{
    ADD_WRITE_RESULT,
    ADD_FLUSH_RESULT,
    ADD_COMPACTION_RESULT,
};

////////////////////////////////////////////////////////////////////////////////

struct TAddMixedBlob
{
    const TPartialBlobId BlobId;
    const TVector<ui32> Blocks;
    const TVector<ui32> Checksums;

    TAddMixedBlob(
            const TPartialBlobId& blobId,
            TVector<ui32> blocks,
            TVector<ui32> checksums)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
        , Checksums(std::move(checksums))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TAddMergedBlob
{
    const TPartialBlobId BlobId;
    const TBlockRange32 BlockRange;
    const TBlockMask SkipMask;
    const TVector<ui32> Checksums;

    TAddMergedBlob(
            const TPartialBlobId& blobId,
            const TBlockRange32& blockRange,
            const TBlockMask& skipMask,
            TVector<ui32> checksums)
        : BlobId(blobId)
        , BlockRange(blockRange)
        , SkipMask(skipMask)
        , Checksums(std::move(checksums))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TAddFreshBlob
{
    const TPartialBlobId BlobId;
    const TVector<TBlock> Blocks;
    const TVector<ui32> Checksums;

    TAddFreshBlob(
            const TPartialBlobId& blobId,
            TVector<TBlock> blocks,
            TVector<ui32> checksums)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
        , Checksums(std::move(checksums))
    {}
};

struct TWriteFreshBlocksRequest
{
    TBlockRange32 BlockRange;
    IWriteBlocksHandlerPtr WriteHandler;

    TWriteFreshBlocksRequest(
            TBlockRange32 blockRange,
            IWriteBlocksHandlerPtr writeHandler)
        : BlockRange(blockRange)
        , WriteHandler(std::move(writeHandler))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TAffectedBlob
{
    TVector<ui16> Offsets;
    TMaybe<TBlockMask> BlockMask;
    TVector<ui32> AffectedBlockIndices;

    // Filled only if a flag is set. BlobMeta is needed only to do some extra
    // consistency checks.
    TMaybe<NProto::TBlobMeta> BlobMeta;
};

using TAffectedBlobs = THashMap<TPartialBlobId, TAffectedBlob, TPartialBlobIdHash>;

////////////////////////////////////////////////////////////////////////////////

struct TAffectedBlock
{
    ui32 BlockIndex = 0;
    ui64 CommitId = 0;
};

using TAffectedBlocks = TVector<TAffectedBlock>;

////////////////////////////////////////////////////////////////////////////////

struct TBlobCompactionInfo
{
    const ui32 BlobsSkippedByCompaction = 0;
    const ui32 BlocksSkippedByCompaction = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushedCommitId
{
    ui64 CommitId;
    ui32 BlockCount;

    TFlushedCommitId(ui64 commitId, ui32 blockCount)
        : CommitId(commitId)
        , BlockCount(blockCount)
    {}
};

using TFlushedCommitIds = TVector<TFlushedCommitId>;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_REQUESTS_PRIVATE(xxx, ...)                        \
    xxx(WriteBlob,                 __VA_ARGS__)                                \
    xxx(AddBlobs,                  __VA_ARGS__)                                \
    xxx(AddFreshBlocks,            __VA_ARGS__)                                \
    xxx(Flush,                     __VA_ARGS__)                                \
    xxx(Compaction,                __VA_ARGS__)                                \
    xxx(MetadataRebuildUsedBlocks, __VA_ARGS__)                                \
    xxx(MetadataRebuildBlockCount, __VA_ARGS__)                                \
    xxx(ScanDiskBatch,             __VA_ARGS__)                                \
    xxx(Cleanup,                   __VA_ARGS__)                                \
    xxx(CollectGarbage,            __VA_ARGS__)                                \
    xxx(AddGarbage,                __VA_ARGS__)                                \
    xxx(DeleteGarbage,             __VA_ARGS__)                                \
    xxx(PatchBlob,                 __VA_ARGS__)                                \
    xxx(AddConfirmedBlobs,         __VA_ARGS__)                                \
    xxx(AddUnconfirmedBlobs,       __VA_ARGS__)                                \
// BLOCKSTORE_PARTITION_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TReadBlocksRequest
{
    NKikimr::TLogoBlobID BlobId;
    NActors::TActorId BSProxy;
    ui16 BlobOffset;
    ui32 BlockIndex;
    ui32 GroupId;
    ui32 BlockChecksum;

    TReadBlocksRequest(
            const NKikimr::TLogoBlobID& blobId,
            NActors::TActorId proxy,
            ui16 blobOffset,
            ui32 blockIndex,
            ui32 groupId,
            ui32 blockChecksum)
        : BlobId(blobId)
        , BSProxy(proxy)
        , BlobOffset(blobOffset)
        , BlockIndex(blockIndex)
        , GroupId(groupId)
        , BlockChecksum(blockChecksum)
    {}
};

using TReadBlocksRequests = TVector<TReadBlocksRequest>;

////////////////////////////////////////////////////////////////////////////////

struct TBlockCountRebuildState
{
    ui64 MixedBlocks = 0;
    ui64 MergedBlocks = 0;

    ui64 InitialMixedBlocks = 0;
    ui64 InitialMergedBlocks = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TEvPartitionPrivate
{
    //
    // WriteBlob
    //

    struct TWriteBlobRequest
    {
        NActors::TActorId Proxy;

        const TPartialBlobId BlobId;
        std::variant<TGuardedSgList, TString> Data;
        // BlockSize is used to calculate checksums. If it's 0, checksums won't
        // be calculated.
        const ui32 BlockSizeForChecksums;
        const bool Async;
        const TInstant Deadline;

        template <typename TData>
        TWriteBlobRequest(
                TPartialBlobId blobId,
                TData data,
                ui32 blockSizeForChecksums,
                bool async,
                TInstant deadline = TInstant::Max())
            : BlobId(blobId)
            , Data(std::move(data))
            , BlockSizeForChecksums(blockSizeForChecksums)
            , Async(async)
            , Deadline(deadline)
        {}
    };

    struct TWriteBlobResponse
    {
        TVector<ui32> BlockChecksums;
        ui64 ExecCycles = 0;
    };

    struct TWriteBlobCompleted
    {
        TPartialBlobId BlobId;
        NKikimr::TStorageStatusFlags StorageStatusFlags;
        double ApproximateFreeSpaceShare = 0;
        TDuration RequestTime;
        ui64 BSGroupOperationId = 0;

        TWriteBlobCompleted() = default;

        TWriteBlobCompleted(
                const TPartialBlobId& blobId,
                NKikimr::TStorageStatusFlags storageStatusFlags,
                double approximateFreeSpaceShare,
                TDuration requestTime)
            : BlobId(blobId)
            , StorageStatusFlags(storageStatusFlags)
            , ApproximateFreeSpaceShare(approximateFreeSpaceShare)
            , RequestTime(requestTime)
        {}
    };

    //
    // PatchBlob
    //

    struct TPatchBlobRequest
    {
        NActors::TActorId Proxy;
        TPartialBlobId OriginalBlobId;
        TPartialBlobId PatchedBlobId;

        TArrayHolder<NKikimr::TEvBlobStorage::TEvPatch::TDiff> Diffs;
        ui32 DiffCount;

        bool Async = false;
        TInstant Deadline;

        TPatchBlobRequest() = default;

        TPatchBlobRequest(
                const TPartialBlobId& originalBlobId,
                const TPartialBlobId& patchedBlobId,
                TArrayHolder<NKikimr::TEvBlobStorage::TEvPatch::TDiff> diffs,
                ui32 diffCount,
                bool async,
                TInstant deadline)
            : OriginalBlobId(originalBlobId)
            , PatchedBlobId(patchedBlobId)
            , Diffs(std::move(diffs))
            , DiffCount(diffCount)
            , Async(async)
            , Deadline(deadline)
        {}
    };

    struct TPatchBlobResponse
    {
        ui64 ExecCycles = 0;
    };

    struct TPatchBlobCompleted
    {
        TPartialBlobId OriginalBlobId;
        TPartialBlobId PatchedBlobId;
        NKikimr::TStorageStatusFlags StorageStatusFlags;
        double ApproximateFreeSpaceShare = 0;
        TDuration RequestTime;
        ui64 BSGroupOperationId = 0;

        TPatchBlobCompleted() = default;

        TPatchBlobCompleted(
                const TPartialBlobId& originalBlobId,
                const TPartialBlobId& patchedBlobId,
                NKikimr::TStorageStatusFlags storageStatusFlags,
                double approximateFreeSpaceShare,
                TDuration requestTime)
            : OriginalBlobId(originalBlobId)
            , PatchedBlobId(patchedBlobId)
            , StorageStatusFlags(storageStatusFlags)
            , ApproximateFreeSpaceShare(approximateFreeSpaceShare)
            , RequestTime(requestTime)
        {}
    };

    //
    // AddBlobs
    //

    struct TAddBlobsRequest
    {
        ui64 CommitId = 0;
        TVector<TAddMixedBlob> MixedBlobs;
        TVector<TAddMergedBlob> MergedBlobs;
        TVector<TAddFreshBlob> FreshBlobs;
        EAddBlobMode Mode = ADD_WRITE_RESULT;

        // compaction
        TAffectedBlobs AffectedBlobs;
        TAffectedBlocks AffectedBlocks;
        TVector<TBlobCompactionInfo> MixedBlobCompactionInfos;
        TVector<TBlobCompactionInfo> MergedBlobCompactionInfos;

        TAddBlobsRequest() = default;

        TAddBlobsRequest(
                ui64 commitId,
                TVector<TAddMixedBlob> mixedBlobs,
                TVector<TAddMergedBlob> mergedBlobs,
                TVector<TAddFreshBlob> freshBlobs,
                EAddBlobMode mode,
                TAffectedBlobs affectedBlobs = {},
                TAffectedBlocks affectedBlocks = {},
                TVector<TBlobCompactionInfo> mixedBlobCompactionInfos = {},
                TVector<TBlobCompactionInfo> mergedBlobCompactionInfos = {})
            : CommitId(commitId)
            , MixedBlobs(std::move(mixedBlobs))
            , MergedBlobs(std::move(mergedBlobs))
            , FreshBlobs(std::move(freshBlobs))
            , Mode(mode)
            , AffectedBlobs(std::move(affectedBlobs))
            , AffectedBlocks(std::move(affectedBlocks))
            , MixedBlobCompactionInfos(std::move(mixedBlobCompactionInfos))
            , MergedBlobCompactionInfos(std::move(mergedBlobCompactionInfos))
        {}
    };

    struct TAddBlobsResponse
    {
        ui64 ExecCycles = 0;
    };

    //
    // AddFreshBlocks
    //

    struct TAddFreshBlocksRequest
    {
        ui64 CommitId;
        ui64 BlobSize;
        TVector<TBlockRange32> BlockRanges;
        TVector<IWriteBlocksHandlerPtr> WriteHandlers;

        TAddFreshBlocksRequest(
                ui64 commitId,
                ui64 blobSize,
                TVector<TBlockRange32> blockRanges,
                TVector<IWriteBlocksHandlerPtr> writeHandlers)
            : CommitId(commitId)
            , BlobSize(blobSize)
            , BlockRanges(std::move(blockRanges))
            , WriteHandlers(std::move(writeHandlers))
        {}
    };

    struct TAddFreshBlocksResponse
    {
    };

    //
    // Flush
    //

    struct TFlushRequest
    {
    };

    struct TFlushResponse
    {
    };

    //
    // Compaction
    //

    enum ECompactionMode
    {
        RangeCompaction,
        GarbageCompaction
    };

    struct TCompactionRequest
    {
        ECompactionMode Mode = RangeCompaction;
        TVector<ui32> RangeBlockIndices;
        TCompactionOptions CompactionOptions;

        TCompactionRequest() = default;

        TCompactionRequest(
                TVector<ui32> rangeBlockIndices,
                TCompactionOptions compactionOptions)
            : RangeBlockIndices(std::move(rangeBlockIndices))
            , CompactionOptions(compactionOptions)
        {}

        TCompactionRequest(
                ui32 blockIndex,
                TCompactionOptions compactionOptions)
            : TCompactionRequest(TVector<ui32>{blockIndex}, compactionOptions)
        {}

        TCompactionRequest(ui32 blockIndex)
            : TCompactionRequest(TVector<ui32>{blockIndex}, {})
        {}

        TCompactionRequest(TVector<ui32> rangeBlockIndices)
            : TCompactionRequest(std::move(rangeBlockIndices), {})
        {}

        TCompactionRequest(ECompactionMode mode)
            : Mode(mode)
        {}
    };

    struct TCompactionResponse
    {
    };

    //
    // MetadataRebuildUsedBlocks
    //

    struct TMetadataRebuildUsedBlocksRequest
    {
        ui32 Begin = 0;
        ui32 End = 0;

        TMetadataRebuildUsedBlocksRequest() = default;

        TMetadataRebuildUsedBlocksRequest(ui32 begin, ui32 end)
            : Begin(begin)
            , End(end)
        {}
    };

    struct TMetadataRebuildUsedBlocksResponse
    {
    };

    //
    // MetadataRebuildBlockCount
    //

    struct TMetadataRebuildBlockCountRequest
    {
        const TPartialBlobId BlobId;
        const ui32 Count = 0;
        const TPartialBlobId FinalBlobId;

        const TBlockCountRebuildState RebuildState;

        TMetadataRebuildBlockCountRequest(
                TPartialBlobId blobId,
                ui32 count,
                TPartialBlobId finalBlobId,
                const TBlockCountRebuildState& rebuildState)
            : BlobId(blobId)
            , Count(count)
            , FinalBlobId(finalBlobId)
            , RebuildState(rebuildState)
        {}
    };

    struct TMetadataRebuildBlockCountResponse
    {
        TPartialBlobId LastReadBlobId;
        TBlockCountRebuildState RebuildState;

        TMetadataRebuildBlockCountResponse() = default;

        TMetadataRebuildBlockCountResponse(
                TPartialBlobId lastReadBlobId,
                const TBlockCountRebuildState& rebuildState)
            : LastReadBlobId(lastReadBlobId)
            , RebuildState(rebuildState)
        {}
    };

    //
    // ScanDiskBatch
    //

    struct TScanDiskBatchRequest
    {
        const TPartialBlobId BlobId;
        const ui32 Count = 0;
        const TPartialBlobId FinalBlobId;

        TScanDiskBatchRequest(
                TPartialBlobId blobId,
                ui32 count,
                TPartialBlobId finalBlobId)
            : BlobId(blobId)
            , Count(count)
            , FinalBlobId(finalBlobId)
        {}
    };

    struct TScanDiskBatchResponse
    {
        struct TBlobMark {
            NKikimr::TLogoBlobID BlobId;
            ui32 BSGroupId;

            TBlobMark(
                    const NKikimr::TLogoBlobID& blobId,
                    ui32 bSGroupId)
                : BlobId(blobId)
                , BSGroupId(bSGroupId)
            {}
        };

        TVector<TBlobMark> BlobsInBatch;
        TPartialBlobId LastVisitedBlobId;
        bool IsScanCompleted = false;

        TScanDiskBatchResponse() = default;

        TScanDiskBatchResponse(
                TVector<TBlobMark> blobs,
                TPartialBlobId lastVisitedBlobId,
                bool isScanCompleted)
            : BlobsInBatch(std::move(blobs))
            , LastVisitedBlobId(lastVisitedBlobId)
            , IsScanCompleted(isScanCompleted)
        {}
    };

    //
    // Cleanup
    //

    struct TCleanupRequest
    {
    };

    struct TCleanupResponse
    {
    };

    //
    // CollectGarbage
    //

    struct TCollectGarbageRequest
    {
    };

    struct TCollectGarbageResponse
    {
    };

    //
    // AddGarbage
    //

    struct TAddGarbageRequest
    {
        TVector<TPartialBlobId> BlobIds;

        TAddGarbageRequest() = default;

        TAddGarbageRequest(TVector<TPartialBlobId> blobIds)
            : BlobIds(std::move(blobIds))
        {}
    };

    struct TAddGarbageResponse
    {
    };

    //
    // DeleteGarbage
    //

    struct TDeleteGarbageRequest
    {
        ui64 CommitId = 0;
        TVector<TPartialBlobId> NewBlobs;
        TVector<TPartialBlobId> GarbageBlobs;

        TDeleteGarbageRequest() = default;

        TDeleteGarbageRequest(
                ui64 commitId,
                TVector<TPartialBlobId> newBlobs,
                TVector<TPartialBlobId> garbageBlobs)
            : CommitId(commitId)
            , NewBlobs(std::move(newBlobs))
            , GarbageBlobs(std::move(garbageBlobs))
        {}
    };

    struct TDeleteGarbageResponse
    {
        ui64 ExecCycles = 0;
    };

    //
    // AddConfirmedBlobs
    //

    struct TAddConfirmedBlobsRequest
    {
    };

    struct TAddConfirmedBlobsResponse
    {
    };

    //
    // AddUnconfirmedBlobs
    //

    struct TAddUnconfirmedBlobsRequest
    {
        ui64 CommitId = 0;
        TVector<TBlobToConfirm> Blobs;

        TAddUnconfirmedBlobsRequest() = default;

        TAddUnconfirmedBlobsRequest(
                ui64 commitId,
                TVector<TBlobToConfirm> blobs)
            : CommitId(commitId)
            , Blobs(std::move(blobs))
        {}
    };

    struct TAddUnconfirmedBlobsResponse
    {
        ui64 ExecCycles = 0;
    };

    //
    // OperationCompleted
    //

    struct TOperationCompleted
    {
        NProto::TPartitionStats Stats;

        ui64 TotalCycles = 0;
        ui64 ExecCycles = 0;

        ui64 CommitId = 0;

        TVector<TBlockRange64> AffectedRanges;
        TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
    };

    //
    // ReadBlocksCompleted
    //

    struct TReadBlocksCompleted
        : TOperationCompleted
    {
        struct TCompactionRangeReadStats
        {
            ui32 BlockIndex;
            ui32 BlobCount;
            ui32 BlockCount;

            TCompactionRangeReadStats(
                    ui32 blockIndex,
                    ui32 blobCount,
                    ui32 blockCount)
                : BlockIndex(blockIndex)
                , BlobCount(blobCount)
                , BlockCount(blockCount)
            {
            }
        };

        TStackVec<TCompactionRangeReadStats, 2> ReadStats;
    };

    //
    // WriteBlocksCompleted
    //

    struct TWriteBlocksCompleted
        : TOperationCompleted
    {
        const bool CollectGarbageBarrierAcquired = false;
        const bool UnconfirmedBlobsAdded = false;
        const bool TrimFreshLogBarrierAcquired = false;
        // needed to pass block checksums to PartState
        TVector<TBlobToConfirm> BlobsToConfirm;

        TWriteBlocksCompleted(
                bool collectGarbageBarrierAcquired,
                bool unconfirmedBlobsAdded,
                bool trimFreshLogBarrierAcquired,
                TVector<TBlobToConfirm> blobsToConfirm)
            : CollectGarbageBarrierAcquired(collectGarbageBarrierAcquired)
            , UnconfirmedBlobsAdded(unconfirmedBlobsAdded)
            , TrimFreshLogBarrierAcquired(trimFreshLogBarrierAcquired)
            , BlobsToConfirm(std::move(blobsToConfirm))
        {
        }

        static TWriteBlocksCompleted CreateFreshBlocksCompleted()
        {
            return TWriteBlocksCompleted(false, false, true, {});
        }

        static TWriteBlocksCompleted CreateMixedBlocksCompleted()
        {
            return TWriteBlocksCompleted(true, false, false, {});
        }

        static TWriteBlocksCompleted CreateMergedBlocksCompleted(
            bool unconfirmedBlobsAdded,
            TVector<TBlobToConfirm>&& blobsToConfirm)
        {
            return TWriteBlocksCompleted(
                true,
                unconfirmedBlobsAdded,
                false,
                std::move(blobsToConfirm));
        }
    };

    //
    // FlushCompleted
    //

    struct TFlushCompleted
        : TOperationCompleted
    {
        ui32 FlushedFreshBlobCount;
        ui64 FlushedFreshBlobByteCount;
        TFlushedCommitIds FlushedCommitIdsFromChannel;

        TFlushCompleted(
                ui32 flushedFreshBlobCount,
                ui64 flushedFreshBlobByteCount,
                TFlushedCommitIds flushedCommitIdsFromChannel)
            : FlushedFreshBlobCount(flushedFreshBlobCount)
            , FlushedFreshBlobByteCount(flushedFreshBlobByteCount)
            , FlushedCommitIdsFromChannel(std::move(flushedCommitIdsFromChannel))
        {
        }
    };

    //
    // ForcedCompactionCompleted
    //

    struct TForcedCompactionCompleted
    {
    };


    //
    // CompactionCompleted
    //

    struct TCompactionCompleted
        : TOperationCompleted
    {
        ECompactionType CompactionType;
    };

    //
    // MetadataRebuildCompleted
    //

    struct TMetadataRebuildCompleted
    {
    };

    //
    // ScanDiskCompleted
    //

    struct TScanDiskCompleted
        : TOperationCompleted
    {
        TVector<NKikimr::TLogoBlobID> BrokenBlobs;

        TScanDiskCompleted(TVector<NKikimr::TLogoBlobID> brokenBlobs)
            : BrokenBlobs(std::move(brokenBlobs))
        {}
    };

    //
    // LoadStateCompleted
    //

    struct TLoadStateCompleted
        : TOperationCompleted
    {
        TVector<NProto::TUsedBlockData> UsedBlocksFromBaseDisk;

        TLoadStateCompleted() = default;

        TLoadStateCompleted(
                const TVector<NProto::TUsedBlockData>& usedBlocksFromBaseDisk)
            : UsedBlocksFromBaseDisk(usedBlocksFromBaseDisk)
        {
        }

    };

    //
    // ConfirmBlobsCompleted
    //

    struct TConfirmBlobsCompleted
    {
        const ui64 StartCycleCount;
        TVector<TPartialBlobId> UnrecoverableBlobs;

        TConfirmBlobsCompleted(
                ui64 startCycleCount,
                TVector<TPartialBlobId> unrecoverableBlobs)
            : StartCycleCount(startCycleCount)
            , UnrecoverableBlobs(std::move(unrecoverableBlobs))
        {}
    };

    //
    // LoadCompactionMapChunkRequest
    //

    struct TLoadCompactionMapChunkRequest
    {
        TBlockRange32 Range;
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::PARTITION_START,

        BLOCKSTORE_PARTITION_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvUpdateCounters,
        EvUpdateYellowState,
        EvSendBackpressureReport,
        EvProcessWriteQueue,

        EvWriteBlobCompleted,
        EvReadBlocksCompleted,
        EvWriteBlocksCompleted,
        EvZeroBlocksCompleted,
        EvFlushCompleted,
        EvCompactionCompleted,
        EvCollectGarbageCompleted,
        EvForcedCompactionCompleted,
        EvMetadataRebuildCompleted,
        EvScanDiskCompleted,
        EvLoadStateCompleted,
        EvGetChangedBlocksCompleted,
        EvPatchBlobCompleted,
        EvAddConfirmedBlobsCompleted,
        EvConfirmBlobsCompleted,
        EvLoadCompactionMapChunkRequest,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::PARTITION_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::PARTITION_END");

    BLOCKSTORE_PARTITION_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvUpdateCounters = TRequestEvent<TEmpty, EvUpdateCounters>;
    using TEvUpdateYellowState = TRequestEvent<TEmpty, EvUpdateYellowState>;
    using TEvSendBackpressureReport = TRequestEvent<TEmpty, EvSendBackpressureReport>;
    using TEvProcessWriteQueue = TRequestEvent<TEmpty, EvProcessWriteQueue>;
    using TEvLoadCompactionMapChunkRequest = TRequestEvent<TLoadCompactionMapChunkRequest, EvLoadCompactionMapChunkRequest>;

    using TEvWriteBlobCompleted = TResponseEvent<TWriteBlobCompleted, EvWriteBlobCompleted>;
    using TEvReadBlocksCompleted = TResponseEvent<TReadBlocksCompleted, EvReadBlocksCompleted>;
    using TEvWriteBlocksCompleted = TResponseEvent<TWriteBlocksCompleted, EvWriteBlocksCompleted>;
    using TEvZeroBlocksCompleted = TResponseEvent<TOperationCompleted, EvZeroBlocksCompleted>;
    using TEvFlushCompleted = TResponseEvent<TFlushCompleted, EvFlushCompleted>;
    using TEvCompactionCompleted = TResponseEvent<TCompactionCompleted, EvCompactionCompleted>;
    using TEvCollectGarbageCompleted = TResponseEvent<TOperationCompleted, EvCollectGarbageCompleted>;
    using TEvForcedCompactionCompleted = TResponseEvent<TForcedCompactionCompleted, EvForcedCompactionCompleted>;
    using TEvMetadataRebuildCompleted = TResponseEvent<TOperationCompleted, EvMetadataRebuildCompleted>;
    using TEvScanDiskCompleted = TResponseEvent<TScanDiskCompleted, EvScanDiskCompleted>;
    using TEvLoadStateCompleted = TResponseEvent<TLoadStateCompleted, EvLoadStateCompleted>;
    using TEvGetChangedBlocksCompleted = TResponseEvent<TOperationCompleted, EvGetChangedBlocksCompleted>;
    using TEvPatchBlobCompleted = TResponseEvent<TPatchBlobCompleted, EvPatchBlobCompleted>;
    using TEvAddConfirmedBlobsCompleted = TResponseEvent<TOperationCompleted, EvAddConfirmedBlobsCompleted>;
    using TEvConfirmBlobsCompleted = TResponseEvent<TConfirmBlobsCompleted, EvConfirmBlobsCompleted>;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
