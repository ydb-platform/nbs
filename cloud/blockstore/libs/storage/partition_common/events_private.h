#pragma once

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_common/model/blob_markers.h>
#include <cloud/blockstore/libs/storage/partition_common/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>

#include <ydb/core/base/logoblob.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(xxx, ...)                 \
    xxx(ReadBlob,                  __VA_ARGS__)                                \
    xxx(TrimFreshLog,              __VA_ARGS__)                                \
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
        ui64 TotalCycles = 0;
        ui64 ExecCycles = 0;

        ui64 CommitId = 0;
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

        TReadBlobRequest() = default;

        TReadBlobRequest(
                const NKikimr::TLogoBlobID& blobId,
                NActors::TActorId proxy,
                TVector<ui16> blobOffsets,
                TGuardedSgList sglist,
                ui32 groupId,
                bool async = false,
                TInstant deadline = TInstant::Max())
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

    // TReadBlobCompleted

    struct TReadBlobCompleted
    {
        NKikimr::TLogoBlobID BlobId;
        ui32 BytesCount = 0;
        TDuration RequestTime;
        ui32 GroupId = 0;

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
            Finished,
            Cancelled,
        };

        const EOperation Operation;
        const bool FirstNotify;
        const EReason Reason;
        const TDuration Duration;
        const ui32 GroupId;

        TLongRunningOperation(
                EOperation operation,
                bool firstNotify,
                TDuration duration,
                ui32 groupId,
                EReason reason)
            : Operation(operation)
            , FirstNotify(firstNotify)
            , Reason(reason)
            , Duration(duration)
            , GroupId(groupId)
        {}
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
};

}   // namespace NCloud::NBlockStore::NStorage
