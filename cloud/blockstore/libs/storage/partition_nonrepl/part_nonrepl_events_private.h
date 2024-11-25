#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NProto {

    using TChecksumBlocksRequest = NProto::TChecksumDeviceBlocksRequest;
    using TChecksumBlocksResponse = NProto::TChecksumDeviceBlocksResponse;
}   // namespace NCloud::NBlockStore::NProto

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_NONREPL_REQUESTS_PRIVATE(xxx, ...)             \
    xxx(ChecksumBlocks, __VA_ARGS__)                                        \
// BLOCKSTORE_PARTITION_NONREPL_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvNonreplPartitionPrivate
{
    //
    // RangeMigrated
    //

    struct TRangeMigrated
    {
        TBlockRange64 Range;
        TInstant ReadStartTs;
        TDuration ReadDuration;
        TInstant WriteStartTs;
        TDuration WriteDuration;
        TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
        bool AllZeroes;
        ui64 ExecCycles;

        TRangeMigrated(
                TBlockRange64 range,
                TInstant readStartTs,
                TDuration readDuration,
                TInstant writeStartTs,
                TDuration writeDuration,
                TVector<IProfileLog::TBlockInfo> affectedBlockInfos,
                bool allZeroes,
                ui64 execCycles)
            : Range(range)
            , ReadStartTs(readStartTs)
            , ReadDuration(readDuration)
            , WriteStartTs(writeStartTs)
            , WriteDuration(writeDuration)
            , AffectedBlockInfos(std::move(affectedBlockInfos))
            , AllZeroes(allZeroes)
            , ExecCycles(execCycles)
        {
        }
    };

    //
    // MigrateNextRange
    //

    struct TMigrateNextRange
    {
    };

    //
    // WriteOrZeroCompleted
    //

    struct TWriteOrZeroCompleted
    {
        const ui64 RequestCounter;
        const ui64 TotalCycles;
        const bool FollowerGotNonRetriableError;

        TWriteOrZeroCompleted(
                ui64 requestCounter,
                ui64 totalCycles,
                bool followerGotNonRetriableError)
            : RequestCounter(requestCounter)
            , TotalCycles(totalCycles)
            , FollowerGotNonRetriableError(followerGotNonRetriableError)
        {
        }
    };

    //
    // MirroredReadCompleted
    //

    struct TMirroredReadCompleted
    {
        const ui64 RequestCounter;
        const bool ChecksumMismatchObserved;

        TMirroredReadCompleted(
                ui64 requestCounter,
                bool checksumMismatchObserved)
            : RequestCounter(requestCounter)
            , ChecksumMismatchObserved(checksumMismatchObserved)
        {
        }
    };

    //
    // ResyncNextRange
    //

    struct TResyncNextRange
    {
    };

    //
    // RangeResynced
    //

    struct TRangeResynced
    {
        TBlockRange64 Range;
        TInstant ChecksumStartTs;
        TDuration ChecksumDuration;
        TInstant ReadStartTs;
        TDuration ReadDuration;
        TInstant WriteStartTs;
        TDuration WriteDuration;
        TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

        TRangeResynced(
                TBlockRange64 range,
                TInstant checksumStartTs,
                TDuration checksumDuration,
                TInstant readStartTs,
                TDuration readDuration,
                TInstant writeStartTs,
                TDuration writeDuration,
                TVector<IProfileLog::TBlockInfo> affectedBlockInfos)
            : Range(range)
            , ChecksumStartTs(checksumStartTs)
            , ChecksumDuration(checksumDuration)
            , ReadStartTs(readStartTs)
            , ReadDuration(readDuration)
            , WriteStartTs(writeStartTs)
            , WriteDuration(writeDuration)
            , AffectedBlockInfos(std::move(affectedBlockInfos))
        {
        }
    };

    //
    // ReadResyncFastPathResponse
    //

    struct TReadResyncFastPathResponse
    {
        NProto::TError Error;
    };

    //
    // OperationCompleted
    //

    struct TOperationCompleted
    {
        enum class EStatus
        {
            Success,   // The request was completed successfully
            Fail,      // The request was executed with an error
            Timeout,   // The response from the server was not received during
                       // the timeout.
        };

        // Request completion status
        EStatus Status = EStatus::Fail;

        NProto::TPartitionStats Stats;

        ui64 TotalCycles = 0;
        ui64 ExecCycles = 0;
        // Request execution total time.
        TDuration ExecutionTime;

        // Indexes of devices that participated in the request.
        TStackVec<ui32, 2> DeviceIndices;

        ui32 NonVoidBlockCount = 0;
        ui32 VoidBlockCount = 0;
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::PARTITION_NONREPL_START,

        EvUpdateCounters,
        EvScrubbingNextRange,
        EvReadBlocksCompleted,
        EvWriteBlocksCompleted,
        EvZeroBlocksCompleted,
        EvRangeMigrated,
        EvMigrateNextRange,
        EvWriteOrZeroCompleted,
        EvMirroredReadCompleted,
        EvChecksumBlocksCompleted,
        EvResyncNextRange,
        EvRangeResynced,
        EvReadResyncFastPathResponse,

        BLOCKSTORE_PARTITION_NONREPL_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::PARTITION_NONREPL_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::PARTITION_NONREPL_END");

    using TEvUpdateCounters = TResponseEvent<TEmpty, EvUpdateCounters>;
    using TEvScrubbingNextRange = TResponseEvent<TEmpty, EvScrubbingNextRange>;
    using TEvReadBlocksCompleted = TResponseEvent<TOperationCompleted, EvReadBlocksCompleted>;
    using TEvWriteBlocksCompleted = TResponseEvent<TOperationCompleted, EvWriteBlocksCompleted>;
    using TEvZeroBlocksCompleted = TResponseEvent<TOperationCompleted, EvZeroBlocksCompleted>;
    using TEvChecksumBlocksCompleted = TResponseEvent<TOperationCompleted, EvChecksumBlocksCompleted>;

    using TEvRangeMigrated = TResponseEvent<
        TRangeMigrated,
        EvRangeMigrated
    >;

    using TEvMigrateNextRange = TResponseEvent<
        TMigrateNextRange,
        EvMigrateNextRange
    >;

    using TEvWriteOrZeroCompleted = TResponseEvent<
        TWriteOrZeroCompleted,
        EvWriteOrZeroCompleted
    >;

    using TEvMirroredReadCompleted = TResponseEvent<
        TMirroredReadCompleted,
        EvMirroredReadCompleted
    >;

    using TEvResyncNextRange = TResponseEvent<
        TResyncNextRange,
        EvResyncNextRange
    >;

    using TEvRangeResynced = TResponseEvent<
        TRangeResynced,
        EvRangeResynced
    >;

    using TEvReadResyncFastPathResponse = TResponseEvent<
        TReadResyncFastPathResponse,
        EvReadResyncFastPathResponse
    >;

    BLOCKSTORE_PARTITION_NONREPL_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_PROTO_EVENTS)

};

}   // namespace NCloud::NBlockStore::NStorage
