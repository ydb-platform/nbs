#pragma once

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_common/model/blob_markers.h>
#include <cloud/blockstore/libs/storage/partition_common/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/logoblob.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(xxx, ...)                 \
    xxx(ReadBlob,                  __VA_ARGS__)                                \
    xxx(TrimFreshLog,              __VA_ARGS__)                                \
    xxx(WriteBlob,                 __VA_ARGS__)                                \
    xxx(PatchBlob,                 __VA_ARGS__)                                \
    xxx(AddFreshBlocks,            __VA_ARGS__)                                \
// BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvPartitionCommonPrivate
{
    //
    // TrimFreshLog
    //

    struct TTrimFreshLogRequest
    {
    };

    struct TTrimFreshLogResponse
    {
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
    // LoadFreshBlobsCompleted
    //

    struct TLoadFreshBlobsCompleted
    {
        TVector<TFreshBlob> Blobs;

        TLoadFreshBlobsCompleted(TVector<TFreshBlob> blobs)
            : Blobs(std::move(blobs))
        {}
    };

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
        bool ShouldCalculateChecksums = false;

        TReadBlobRequest() = default;

        TReadBlobRequest(
                const NKikimr::TLogoBlobID& blobId,
                NActors::TActorId proxy,
                TVector<ui16> blobOffsets,
                TGuardedSgList sglist,
                ui32 groupId,
                bool async,
                TInstant deadline,
                bool shouldCalculateChecksums)
            : BlobId(blobId)
            , Proxy(proxy)
            , BlobOffsets(std::move(blobOffsets))
            , Sglist(std::move(sglist))
            , GroupId(groupId)
            , Async(async)
            , Deadline(deadline)
            , ShouldCalculateChecksums(shouldCalculateChecksums)
        {}
    };

    struct TReadBlobResponse
    {
        TVector<ui32> BlockChecksums;
        ui64 ExecCycles = 0;
    };

    // TReadBlobCompleted

    struct TReadBlobCompleted
    {
        NKikimr::TLogoBlobID BlobId;
        ui32 BytesCount = 0;
        TDuration RequestTime;
        ui32 GroupId = 0;
        bool DeadlineSeen = false;
        ui64 BSGroupOperationId = 0;

        TReadBlobCompleted() = default;

        TReadBlobCompleted(
                const NKikimr::TLogoBlobID& blobId,
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
    // DescribeBlocksCompleted
    //

    struct TDescribeBlocksCompleted
    {
        NBlobMarkers::TBlockMarks BlockMarks;

        TDescribeBlocksCompleted(
                NBlobMarkers::TBlockMarks blockMarks)
            : BlockMarks(std::move(blockMarks))
        {}
    };

    //
    // Tracking long running ReadBlob and WriteBlob operations
    //

    struct TLongRunningOperation
    {
        enum EOperation
        {
            DontCare,
            ReadBlob,
            WriteBlob,
            Count,
        };
        enum class EReason
        {
            LongRunningDetected,
            FinishedOk,
            Cancelled,
        };

        const EOperation Operation;
        const bool FirstNotify;
        const EReason Reason;
        const TDuration Duration;
        const ui32 GroupId;
        const NProto::TError Error;

        TLongRunningOperation(
                EOperation operation,
                bool firstNotify,
                TDuration duration,
                ui32 groupId,
                EReason reason,
                const NProto::TError& error)
            : Operation(operation)
            , FirstNotify(firstNotify)
            , Reason(reason)
            , Duration(duration)
            , GroupId(groupId)
            , Error(error)
        {}
    };

    //
    // GetPartCounters
    //

    struct TGetPartCountersRequest
    {
        TGetPartCountersRequest() = default;
    };

    struct TGetPartCountersResponse
    {
        NActors::TActorId PartActorId;
        ui64 VolumeSystemCpu;
        ui64 VolumeUserCpu;
        TPartitionDiskCountersPtr DiskCounters;
        NBlobMetrics::TBlobLoadMetrics BlobLoadMetrics;
        NKikimrTabletBase::TMetrics TabletMetrics;

        TGetPartCountersResponse() = default;

        TGetPartCountersResponse(
                NActors::TActorId partActorId,
                ui64 volumeSystemCpu,
                ui64 volumeUserCpu,
                TPartitionDiskCountersPtr diskCounters,
                NBlobMetrics::TBlobLoadMetrics metrics,
                NKikimrTabletBase::TMetrics tabletMetrics)
            : PartActorId(partActorId)
            , VolumeSystemCpu(volumeSystemCpu)
            , VolumeUserCpu(volumeUserCpu)
            , DiskCounters(std::move(diskCounters))
            , BlobLoadMetrics(std::move(metrics))
            , TabletMetrics(std::move(tabletMetrics))
        {}
    };

    struct TPartCountersCombined
    {
        TVector<TGetPartCountersResponse> PartCounters;

        TPartCountersCombined() = default;
    };

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

    //
    // WriteBlobCompleted
    //

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
                TDuration requestTime,
                ui64 bsGroupOperationId)
            : BlobId(blobId)
            , StorageStatusFlags(storageStatusFlags)
            , ApproximateFreeSpaceShare(approximateFreeSpaceShare)
            , RequestTime(requestTime)
            , BSGroupOperationId(bsGroupOperationId)
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
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::PARTITION_COMMON_START,

        BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvLoadFreshBlobsCompleted,
        EvTrimFreshLogCompleted,
        EvReadBlobCompleted,
        EvTDescribeBlocksCompleted,
        EvLongRunningOperation,
        EvGetPartCountersRequest,
        EvGetPartCountersResponse,
        EvPartCountersCombined,
        EvPatchBlobCompleted,
        EvWriteBlobCompleted,
        EvWriteFreshBlocksCompleted,
        EvZeroFreshBlocksCompleted,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::PARTITION_COMMON_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::PARTITION_COMMON_END");

    BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvLoadFreshBlobsCompleted = TResponseEvent<TLoadFreshBlobsCompleted, EvLoadFreshBlobsCompleted>;
    using TEvTrimFreshLogCompleted = TResponseEvent<TOperationCompleted, EvTrimFreshLogCompleted>;
    using TEvReadBlobCompleted = TResponseEvent<TReadBlobCompleted, EvReadBlobCompleted>;
    using TEvDescribeBlocksCompleted = TResponseEvent<TDescribeBlocksCompleted, EvTDescribeBlocksCompleted>;
    using TEvLongRunningOperation = TRequestEvent<TLongRunningOperation, EvLongRunningOperation>;
    using TEvGetPartCountersRequest =
        TRequestEvent<TGetPartCountersRequest, EvGetPartCountersRequest>;
    using TEvGetPartCountersResponse =
        TResponseEvent<TGetPartCountersResponse, EvGetPartCountersResponse>;
    using TEvPartCountersCombined =
        TResponseEvent<TPartCountersCombined, EvPartCountersCombined>;
    using TEvWriteBlobCompleted =
        TResponseEvent<TWriteBlobCompleted, EvWriteBlobCompleted>;

    using TEvPatchBlobCompleted =
        TResponseEvent<TPatchBlobCompleted, EvPatchBlobCompleted>;

    using TEvWriteFreshBlocksCompleted =
        TResponseEvent<TOperationCompleted, EvWriteFreshBlocksCompleted>;
    using TEvZeroFreshBlocksCompleted =
        TResponseEvent<TOperationCompleted, EvZeroFreshBlocksCompleted>;
};

}   // namespace NCloud::NBlockStore::NStorage
