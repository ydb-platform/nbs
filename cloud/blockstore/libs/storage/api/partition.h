#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_REQUESTS(xxx, ...)                                \
    xxx(WaitReady,                                                 __VA_ARGS__)\
    xxx(StatPartition,                                             __VA_ARGS__)\
    /* Waits until there are no more in-flight write requests. */              \
    xxx(Drain,                                                     __VA_ARGS__)\
    /* Waits for current in-flight writes to finish and does not affect any    \
     * requests that come after. */                                            \
    xxx(WaitForInFlightWrites,                                     __VA_ARGS__)\
    /* Block range for writing requests. Wait for current in-flight writes     \
     * which overlap that range to finish and reply. Lock can be released by   \
     * sending a TEvReleaseRange message. */                                   \
    xxx(LockAndDrainRange,                                        __VA_ARGS__) \
// BLOCKSTORE_PARTITION_REQUESTS

// requests forwarded from service to partition
#define BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE(xxx, ...)                    \
    xxx(ReadBlocks,         __VA_ARGS__)                                       \
    xxx(WriteBlocks,        __VA_ARGS__)                                       \
    xxx(ZeroBlocks,         __VA_ARGS__)                                       \
    xxx(CreateCheckpoint,   __VA_ARGS__)                                       \
    xxx(DeleteCheckpoint,   __VA_ARGS__)                                       \
    xxx(GetChangedBlocks,   __VA_ARGS__)                                       \
    xxx(ReadBlocksLocal,    __VA_ARGS__)                                       \
    xxx(WriteBlocksLocal,   __VA_ARGS__)                                       \
// BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE

// requests forwarded from volume to partion
#define BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME(xxx, ...)                     \
    xxx(DescribeBlocks,           __VA_ARGS__)                                 \
    xxx(GetUsedBlocks,            __VA_ARGS__)                                 \
    xxx(GetPartitionInfo,         __VA_ARGS__)                                 \
    xxx(CompactRange,             __VA_ARGS__)                                 \
    xxx(GetCompactionStatus,      __VA_ARGS__)                                 \
    xxx(DeleteCheckpointData,     __VA_ARGS__)                                 \
    xxx(RebuildMetadata,          __VA_ARGS__)                                 \
    xxx(GetRebuildMetadataStatus, __VA_ARGS__)                                 \
    xxx(ScanDisk,                 __VA_ARGS__)                                 \
    xxx(GetScanDiskStatus,        __VA_ARGS__)                                 \
    xxx(CheckRange,               __VA_ARGS__)                                 \
// BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME

////////////////////////////////////////////////////////////////////////////////

struct TEvPartition
{
    //
    // WaitReady
    //

    struct TWaitReadyRequest
    {
    };

    struct TWaitReadyResponse
    {
    };

    //
    // StatPartition
    //

    struct TStatPartitionRequest
    {
    };

    struct TStatPartitionResponse
    {
        NProto::TStatVolumeResponse Record;
    };

    //
    // Drain
    //

    struct TDrainRequest
    {
    };

    struct TDrainResponse
    {
    };

    //
    // WaitForInFlightWrites
    //

    struct TWaitForInFlightWritesRequest
    {
    };

    struct TWaitForInFlightWritesResponse
    {
    };

    //
    // LockAndDrainRange
    //

    struct TLockAndDrainRangeRequest
    {
        TBlockRange64 Range;
        explicit TLockAndDrainRangeRequest(TBlockRange64 range)
            : Range(range)
        {}
    };

    struct TLockAndDrainRangeResponse
    {
    };

    //
    // ReleaseRange
    //

    struct TReleaseRange
    {
        TBlockRange64 Range;
        explicit TReleaseRange(TBlockRange64 range)
            : Range(range)
        {}
    };

    //
    // Garbage collector finish report
    //

    struct TGarbageCollectorCompleted
    {
        const ui64 TabletId;
        TGarbageCollectorCompleted(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::PARTITION_START,

        EvWaitReadyRequest = EvBegin + 1,
        EvWaitReadyResponse = EvBegin + 2,

        EvStatPartitionRequest = EvBegin + 3,
        EvStatPartitionResponse = EvBegin + 4,

        EvBackpressureReport = EvBegin + 5,

        EvDrainRequest = EvBegin + 6,
        EvDrainResponse = EvBegin + 7,

        EvGarbageCollectorCompleted = EvBegin + 8,

        EvAddLaggingAgentRequest = EvBegin + 9,
        EvRemoveLaggingReplicaRequest = EvBegin + 10,

        EvWaitForInFlightWritesRequest = EvBegin + 11,
        EvWaitForInFlightWritesResponse = EvBegin + 12,

        EvLockAndDrainRangeRequest = EvBegin + 13,
        EvLockAndDrainRangeResponse = EvBegin + 14,

        EvReleaseRange = EvBegin + 15,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::PARTITION_END,
        "EvEnd expected to be < TBlockStoreEvents::PARTITION_END");

    BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)

    using TEvReleaseRange = TRequestEvent<TReleaseRange, EvReleaseRange>;

    using TEvBackpressureReport = TRequestEvent<
        TBackpressureReport,
        EvBackpressureReport
    >;

    using TEvGarbageCollectorCompleted = TRequestEvent<
        TGarbageCollectorCompleted,
        EvGarbageCollectorCompleted
    >;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
