#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
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
        enum class EExecutionSide
        {
            Local,
            Remote
        };

        TBlockRange64 Range;
        TInstant ReadStartTs;
        TDuration ReadDuration;
        TInstant WriteStartTs;
        TDuration WriteDuration;
        TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
        bool AllZeroes;
        ui64 RecommendedBandwidth;
        EExecutionSide ExecutionSide;
        ui64 ExecCycles;

        TRangeMigrated(
                EExecutionSide executionSide,
                TBlockRange64 range,
                TInstant readStartTs,
                TDuration readDuration,
                TInstant writeStartTs,
                TDuration writeDuration,
                TVector<IProfileLog::TBlockInfo> affectedBlockInfos,
                ui64 recommendedBandwidth,
                bool allZeroes,
                ui64 execCycles)
            : Range(range)
            , ReadStartTs(readStartTs)
            , ReadDuration(readDuration)
            , WriteStartTs(writeStartTs)
            , WriteDuration(writeDuration)
            , AffectedBlockInfos(std::move(affectedBlockInfos))
            , AllZeroes(allZeroes)
            , RecommendedBandwidth(recommendedBandwidth)
            , ExecutionSide(executionSide)
            , ExecCycles(execCycles)
        {}
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
        const ui64 RequestId;
        const ui64 TotalCycles;
        const bool FollowerGotNonRetriableError;

        TWriteOrZeroCompleted(
                ui64 requestId,
                ui64 totalCycles,
                bool followerGotNonRetriableError)
            : RequestId(requestId)
            , TotalCycles(totalCycles)
            , FollowerGotNonRetriableError(followerGotNonRetriableError)
        {}
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
        enum class EStatus
        {
            Healthy,         // Range OK.
            HealedAll,       // All blocks in range resynced
            HealedPartial,   // Only a part of the blocks in the range were
                             // resynced
            HealedNone,      // Not a single block was resynced.
        };

        TBlockRange64 Range;
        TInstant ChecksumStartTs;
        TDuration ChecksumDuration;
        TInstant ReadStartTs;
        TDuration ReadDuration;
        TInstant WriteStartTs;
        TDuration WriteDuration;
        ui64 ExecCycles;
        TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
        EStatus Status;

        TRangeResynced(
                TBlockRange64 range,
                TInstant checksumStartTs,
                TDuration checksumDuration,
                TInstant readStartTs,
                TDuration readDuration,
                TInstant writeStartTs,
                TDuration writeDuration,
                ui64 execCycles,
                TVector<IProfileLog::TBlockInfo> affectedBlockInfos,
                EStatus status)
            : Range(range)
            , ChecksumStartTs(checksumStartTs)
            , ChecksumDuration(checksumDuration)
            , ReadStartTs(readStartTs)
            , ReadDuration(readDuration)
            , WriteStartTs(writeStartTs)
            , WriteDuration(writeDuration)
            , ExecCycles(execCycles)
            , AffectedBlockInfos(std::move(affectedBlockInfos))
            , Status(status)
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
    // GetDeviceForRange
    //

    struct TGetDeviceForRangeRequest
    {
        enum class EPurpose
        {
            ForReading,
            ForWriting
        };

        EPurpose Purpose;
        TBlockRange64 BlockRange;

        TGetDeviceForRangeRequest(EPurpose purpose, TBlockRange64 blockRange)
            : Purpose(purpose)
            , BlockRange(blockRange)
        {}
    };

    struct TGetDeviceForRangeResponse
    {
        NProto::TDeviceConfig Device;
        TBlockRange64 DeviceBlockRange;
        TDuration RequestTimeout;
        TNonreplicatedPartitionConfigPtr PartConfig;
    };

    //
    // CancelRequest
    //

    struct TCancelRequest
    {
        enum class EReason {
            TimedOut,
            Canceled
        };

        EReason Reason = EReason::TimedOut;
    };

    //
    // AddLaggingAgent
    //

    struct TAddLaggingAgentRequest
    {
        NProto::TLaggingAgent LaggingAgent;

        explicit TAddLaggingAgentRequest(NProto::TLaggingAgent laggingAgent)
            : LaggingAgent(std::move(laggingAgent))
        {}
    };

    //
    // RemoveLaggingAgent
    //

    struct TRemoveLaggingAgentRequest
    {
        NProto::TLaggingAgent LaggingAgent;

        explicit TRemoveLaggingAgentRequest(NProto::TLaggingAgent laggingAgent)
            : LaggingAgent(std::move(laggingAgent))
        {}
    };

    //
    // AgentIsUnavailable
    //

    struct TAgentIsUnavailable
    {
        const NProto::TLaggingAgent LaggingAgent;

        explicit TAgentIsUnavailable(NProto::TLaggingAgent laggingAgent)
            : LaggingAgent(std::move(laggingAgent))
        {}
    };

    //
    // AgentIsBackOnline
    //

    struct TAgentIsBackOnline
    {
        const TString AgentId;

        explicit TAgentIsBackOnline(TString agentId)
            : AgentId(std::move(agentId))
        {}
    };

    struct TStartLaggingAgentMigration
    {
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
        EvGetDeviceForRangeRequest,
        EvGetDeviceForRangeResponse,
        EvCancelRequest,
        EvAddLaggingAgentRequest,
        EvRemoveLaggingAgentRequest,
        EvAgentIsUnavailable,
        EvAgentIsBackOnline,
        EvStartLaggingAgentMigration,

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

    using TEvGetDeviceForRangeRequest = TResponseEvent<
        TGetDeviceForRangeRequest,
        EvGetDeviceForRangeRequest
    >;
    using TEvGetDeviceForRangeResponse = TResponseEvent<
        TGetDeviceForRangeResponse,
        EvGetDeviceForRangeResponse
    >;

    using TEvCancelRequest = TRequestEvent<TCancelRequest, EvCancelRequest>;

    using TEvAddLaggingAgentRequest = TRequestEvent<
        TAddLaggingAgentRequest,
        EvAddLaggingAgentRequest
    >;

    using TEvRemoveLaggingAgentRequest = TRequestEvent<
        TRemoveLaggingAgentRequest,
        EvRemoveLaggingAgentRequest
    >;

    using TEvAgentIsUnavailable = TRequestEvent<
        TAgentIsUnavailable,
        EvAgentIsUnavailable
    >;

    using TEvAgentIsBackOnline = TRequestEvent<
        TAgentIsBackOnline,
        EvAgentIsBackOnline
    >;

    using TEvStartLaggingAgentMigration = TRequestEvent<
        TStartLaggingAgentMigration,
        EvStartLaggingAgentMigration
    >;

    BLOCKSTORE_PARTITION_NONREPL_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_PROTO_EVENTS)

};

}   // namespace NCloud::NBlockStore::NStorage
