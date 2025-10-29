#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/core/compaction_type.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/model/channel_permissions.h>
#include <cloud/blockstore/libs/storage/partition2/model/blob.h>
#include <cloud/blockstore/libs/storage/partition2/model/blob_index.h>
#include <cloud/blockstore/libs/storage/partition2/model/block.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/lwtrace/shuttle.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION2_REQUESTS_PRIVATE(xxx, ...)                       \
    xxx(ReadBlob,               __VA_ARGS__)                                   \
    xxx(WriteBlob,              __VA_ARGS__)                                   \
    xxx(AddBlobs,               __VA_ARGS__)                                   \
    xxx(AddFreshBlocks,         __VA_ARGS__)                                   \
    xxx(Flush,                  __VA_ARGS__)                                   \
    xxx(Compaction,             __VA_ARGS__)                                   \
    xxx(Cleanup,                __VA_ARGS__)                                   \
    xxx(InitIndex,              __VA_ARGS__)                                   \
    xxx(UpdateIndexStructures,  __VA_ARGS__)                                   \
    xxx(CollectGarbage,         __VA_ARGS__)                                   \
    xxx(AddGarbage,             __VA_ARGS__)                                   \
    xxx(DeleteGarbage,          __VA_ARGS__)                                   \
// BLOCKSTORE_PARTITION2_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TReadBlocksRequest
{
    NKikimr::TLogoBlobID BlobId;
    NActors::TActorId BSProxy;
    ui16 BlobOffset;
    ui32 BlockIndex;
    ui32 GroupId;

    TReadBlocksRequest(
            const NKikimr::TLogoBlobID& blobId,
            NActors::TActorId proxy,
            ui16 blobOffset,
            ui32 blockIndex,
            ui32 groupId)
        : BlobId(blobId)
        , BSProxy(proxy)
        , BlobOffset(blobOffset)
        , BlockIndex(blockIndex)
        , GroupId(groupId)
    {}
};

using TReadBlocksRequests = TVector<TReadBlocksRequest>;

////////////////////////////////////////////////////////////////////////////////

struct TEvPartitionPrivate
{
    //
    // ReadBlob
    //

    struct TReadBlobRequest
    {
        NKikimr::TLogoBlobID BlobId;
        NActors::TActorId Proxy;
        TVector<ui16> BlobOffsets;
        TGuardedSgList Sglist;
        ui32 GroupId = 0;
        bool Async = false;
        TInstant Deadline;

        TReadBlobRequest() = default;

        TReadBlobRequest(
                const NKikimr::TLogoBlobID& blobId,
                NActors::TActorId proxy,
                TVector<ui16> blobOffsets,
                TGuardedSgList sglist,
                ui32 groupId,
                bool async,
                TInstant deadline)
            : BlobId(blobId)
            , Proxy(proxy)
            , BlobOffsets(std::move(blobOffsets))
            , Sglist(std::move(sglist))
            , GroupId(groupId)
            , Async(async)
            , Deadline(deadline)
        {}
    };

    struct TReadBlobResponse
    {
        ui64 ExecCycles = 0;

        TReadBlobResponse() = default;
    };

    struct TReadBlobCompleted
    {
        NKikimr::TLogoBlobID BlobId;
        ui32 BytesCount = 0;
        TDuration RequestTime;
        ui32 GroupId = 0;
        bool DeadlineSeen = false;

        TReadBlobCompleted() = default;

        TReadBlobCompleted(
                const NKikimr::TLogoBlobID blobId,
                ui32 bytesCount,
                TDuration requestTime,
                ui32 groupId)
            : BlobId(blobId)
            , BytesCount(bytesCount)
            , RequestTime(requestTime)
            , GroupId(groupId)
        {}
    };

    //
    // WriteBlob
    //

    struct TWriteBlobRequest
    {
        NActors::TActorId Proxy;

        TPartialBlobId BlobId;
        std::variant<TGuardedSgList, TString> Data;

        bool Async = false;
        TInstant Deadline;

        TWriteBlobRequest() = default;

        template <typename TData>
        TWriteBlobRequest(
                TPartialBlobId blobId,
                TData data,
                bool async = false,
                TInstant deadline = TInstant::Max())
            : BlobId(blobId)
            , Data(std::move(data))
            , Async(async)
            , Deadline(deadline)
        {}
    };

    struct TWriteBlobResponse
    {
        ui64 ExecCycles = 0;
    };

    struct TWriteBlobCompleted
    {
        TPartialBlobId BlobId;
        NKikimr::TStorageStatusFlags StorageStatusFlags;
        double ApproximateFreeSpaceShare = 0;
        TDuration RequestTime;

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
    // AddBlobs
    //

    struct TAddBlobsRequest
    {
        EAddBlobMode Mode;
        TVector<TAddBlob> NewBlobs;
        TGarbageInfo GarbageInfo;
        TAffectedBlobInfos AffectedBlobInfos;
        ui32 BlobsSkippedByCompaction = 0;
        ui32 BlocksSkippedByCompaction = 0;

        TAddBlobsRequest() = default;

        TAddBlobsRequest(
                EAddBlobMode mode,
                TVector<TAddBlob> newBlobs)
            : Mode(mode)
            , NewBlobs(std::move(newBlobs))
            , BlobsSkippedByCompaction(0)
            , BlocksSkippedByCompaction(0)
        {
            Y_DEBUG_ABORT_UNLESS(mode != EAddBlobMode::ADD_COMPACTION_RESULT);
        }

        TAddBlobsRequest(
                EAddBlobMode mode,
                TVector<TAddBlob> newBlobs,
                TGarbageInfo garbageInfo,
                TAffectedBlobInfos affectedBlobInfos,
                ui32 blobsSkippedByCompaction,
                ui32 blocksSkippedByCompaction)
            : Mode(mode)
            , NewBlobs(std::move(newBlobs))
            , GarbageInfo(std::move(garbageInfo))
            , AffectedBlobInfos(std::move(affectedBlobInfos))
            , BlobsSkippedByCompaction(blobsSkippedByCompaction)
            , BlocksSkippedByCompaction(blocksSkippedByCompaction)
        {}
    };

    struct TAddBlobsResponse
    {
        ui64 ExecCycles = 0;
        TVector<IProfileLog::TBlockCommitId> BlockCommitIds;
    };

    //
    // AddFreshBlocks
    //

    struct TAddFreshBlocksRequest
    {
        const ui64 CommitId;
        TVector<TBlockRange32> BlockRanges;
        TVector<IWriteBlocksHandlerPtr> WriteHandlers;
        const ui64 FirstRequestDeletionId;

        TAddFreshBlocksRequest(
                ui64 commitId,
                TVector<TBlockRange32> blockRanges,
                TVector<IWriteBlocksHandlerPtr> writeHandlers,
                ui64 firstRequestDeletionId)
            : CommitId(commitId)
            , BlockRanges(std::move(blockRanges))
            , WriteHandlers(std::move(writeHandlers))
            , FirstRequestDeletionId(firstRequestDeletionId)
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
        TMaybe<ui32> BlockIndex;
        TCompactionOptions CompactionOptions;
        TGarbageInfo GarbageInfo;

        TCompactionRequest() = default;

        TCompactionRequest(
                ui32 blockIndex,
                TCompactionOptions compactionOptions)
            : BlockIndex(blockIndex)
            , CompactionOptions(compactionOptions)
        {}

        TCompactionRequest(ui32 blockIndex)
            : TCompactionRequest(blockIndex, {})
        {}

        TCompactionRequest(ECompactionMode mode)
            : Mode(mode)
        {}


        TCompactionRequest(TGarbageInfo garbageInfo)
            : Mode(GarbageCompaction)
            , GarbageInfo(std::move(garbageInfo))
        {}
    };

    struct TCompactionResponse
    {
    };

    //
    // Cleanup
    //

    enum ECleanupMode
    {
        DirtyBlobCleanup,
        CheckpointBlobCleanup
    };

    struct TCleanupRequest
    {
        ECleanupMode Mode = DirtyBlobCleanup;

        TCleanupRequest() = default;

        TCleanupRequest(ECleanupMode mode)
            : Mode(mode)
        {}
    };

    struct TCleanupResponse
    {
    };

    //
    // InitIndex
    //

    struct TInitIndexRequest
    {
        TVector<TBlockRange32> BlockRanges;

        TInitIndexRequest(TVector<TBlockRange32> blockRanges)
            : BlockRanges(std::move(blockRanges))
        {}
    };

    struct TInitIndexResponse
    {
    };

    //
    // UpdateIndexStructures
    //

    struct TUpdateIndexStructuresRequest
    {
        TBlockRange32 BlockRange;

        TUpdateIndexStructuresRequest() = default;

        TUpdateIndexStructuresRequest(const TBlockRange32& blockRange)
            : BlockRange(blockRange)
        {
        }
    };

    struct TUpdateIndexStructuresResponse
    {
        ui32 MixedZoneCount = 0;

        TUpdateIndexStructuresResponse() = default;

        TUpdateIndexStructuresResponse(ui32 mixedZoneCount)
            : MixedZoneCount(mixedZoneCount)
        {
        }
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
    // OperationCompleted
    //

    struct TOperationCompleted
    {
        NProto::TPartitionStats Stats;
        ui64 CommitId = 0;

        ui64 TotalCycles = 0;
        ui64 ExecCycles = 0;

        TVector<TBlockRange64> AffectedRanges;
        TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
        TVector<IProfileLog::TBlockCommitId> BlockCommitIds;
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
        bool CollectBarrierAcquired;

        TWriteBlocksCompleted(
                bool collectBarrierAcquired)
            : CollectBarrierAcquired(collectBarrierAcquired)
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
    // ForcedCleanupCompleted
    //

    struct TForcedCleanupCompleted
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::PARTITION_START,

        BLOCKSTORE_PARTITION2_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvUpdateCounters,
        EvUpdateYellowState,
        EvSendBackpressureReport,
        EvProcessWriteQueue,

        EvReadBlobCompleted,
        EvWriteBlobCompleted,
        EvReadBlocksCompleted,
        EvWriteBlocksCompleted,
        EvZeroBlocksCompleted,
        EvFlushCompleted,
        EvCompactionCompleted,
        EvCollectGarbageCompleted,
        EvForcedCompactionCompleted,
        EvForcedCleanupCompleted,
        EvInitFreshZonesCompleted,
        EvGetChangedBlocksCompleted,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::PARTITION_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::PARTITION_END");

    BLOCKSTORE_PARTITION2_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvUpdateCounters = TRequestEvent<TEmpty, EvUpdateCounters>;
    using TEvUpdateYellowState = TRequestEvent<TEmpty, EvUpdateYellowState>;
    using TEvSendBackpressureReport = TRequestEvent<TEmpty, EvSendBackpressureReport>;
    using TEvProcessWriteQueue = TRequestEvent<TEmpty, EvProcessWriteQueue>;

    using TEvReadBlobCompleted = TResponseEvent<TReadBlobCompleted, EvReadBlobCompleted>;
    using TEvWriteBlobCompleted = TResponseEvent<TWriteBlobCompleted, EvWriteBlobCompleted>;
    using TEvReadBlocksCompleted = TResponseEvent<TReadBlocksCompleted, EvReadBlocksCompleted>;
    using TEvWriteBlocksCompleted = TResponseEvent<TWriteBlocksCompleted, EvWriteBlocksCompleted>;
    using TEvZeroBlocksCompleted = TResponseEvent<TOperationCompleted, EvZeroBlocksCompleted>;
    using TEvFlushCompleted = TResponseEvent<TOperationCompleted, EvFlushCompleted>;
    using TEvCompactionCompleted = TResponseEvent<TCompactionCompleted, EvCompactionCompleted>;
    using TEvCollectGarbageCompleted = TResponseEvent<TOperationCompleted, EvCollectGarbageCompleted>;
    using TEvForcedCompactionCompleted = TResponseEvent<TForcedCompactionCompleted, EvForcedCompactionCompleted>;
    using TEvForcedCleanupCompleted = TResponseEvent<TForcedCleanupCompleted, EvForcedCleanupCompleted>;
    using TEvInitFreshZonesCompleted = TResponseEvent<TEmpty, EvInitFreshZonesCompleted>;
    using TEvGetChangedBlocksCompleted = TResponseEvent<TOperationCompleted, EvGetChangedBlocksCompleted>;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
