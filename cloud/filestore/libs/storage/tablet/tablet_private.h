#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/events.h>
#include <cloud/filestore/libs/storage/core/request_info.h>
#include <cloud/filestore/libs/storage/model/public.h>
#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/tablet/model/blob.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/shard_balancer.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC(xxx, ...)                      \
    xxx(Compaction,                             __VA_ARGS__)                   \
    xxx(CollectGarbage,                         __VA_ARGS__)                   \
    xxx(SyncSessions,                           __VA_ARGS__)                   \
    xxx(CleanupSessions,                        __VA_ARGS__)                   \
    xxx(DeleteCheckpoint,                       __VA_ARGS__)                   \
    xxx(DumpCompactionRange,                    __VA_ARGS__)                   \
    xxx(Flush,                                  __VA_ARGS__)                   \
    xxx(FlushBytes,                             __VA_ARGS__)                   \
    xxx(ForcedRangeOperation,                   __VA_ARGS__)                   \
    xxx(Truncate,                               __VA_ARGS__)                   \
    xxx(ReadBlob,                               __VA_ARGS__)                   \
    xxx(WriteBlob,                              __VA_ARGS__)                   \
    xxx(WriteBatch,                             __VA_ARGS__)                   \
// FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC

#define FILESTORE_TABLET_REQUESTS_PRIVATE_SYNC(xxx, ...)                       \
    xxx(AddBlob,                                __VA_ARGS__)                   \
    xxx(Cleanup,                                __VA_ARGS__)                   \
    xxx(DeleteZeroCompactionRanges,             __VA_ARGS__)                   \
    xxx(DeleteGarbage,                          __VA_ARGS__)                   \
    xxx(TruncateRange,                          __VA_ARGS__)                   \
    xxx(ZeroRange,                              __VA_ARGS__)                   \
    xxx(FilterAliveNodes,                       __VA_ARGS__)                   \
    xxx(GenerateCommitId,                       __VA_ARGS__)                   \
    xxx(SyncShardSessions,                      __VA_ARGS__)                   \
    xxx(LoadCompactionMapChunk,                 __VA_ARGS__)                   \
// FILESTORE_TABLET_REQUESTS_PRIVATE

#define FILESTORE_TABLET_REQUESTS_PRIVATE(xxx, ...)                            \
    FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC(xxx, __VA_ARGS__)                  \
    FILESTORE_TABLET_REQUESTS_PRIVATE_SYNC(xxx,  __VA_ARGS__)                  \
// FILESTORE_TABLET_REQUESTS_PRIVATE

#define FILESTORE_DECLARE_PRIVATE_EVENT_IDS(name, ...)                         \
    FILESTORE_DECLARE_EVENT_IDS(name, __VA_ARGS__)                             \
    Ev##name##Completed,                                                       \
// FILESTORE_DECLARE_PRIVATE_EVENT_IDS

#define FILESTORE_DECLARE_PRIVATE_EVENTS(name, ...)                            \
    FILESTORE_DECLARE_EVENTS(name, __VA_ARGS__)                                \
                                                                               \
    using TEv##name##Completed = TResponseEvent<                               \
        T##name##Completed,                                                    \
        Ev##name##Completed                                                    \
    >;                                                                         \
// FILESTORE_DECLARE_PRIVATE_EVENTS

#define FILESTORE_IMPLEMENT_ASYNC_REQUEST(name, ns)                            \
    FILESTORE_IMPLEMENT_REQUEST(name, ns)                                      \
    void Handle##name##Completed(                                              \
        const ns::TEv##name##Completed::TPtr& ev,                              \
        const NActors::TActorContext& ctx);                                    \
// FILESTORE_IMPLEMENT_ASYNC_REQUEST

#define FILESTORE_HANDLE_COMPLETION(name, ns)                                  \
    HFunc(ns::TEv##name##Completed, Handle##name##Completed);                  \
// FILESTORE_HANDLE_COMPLETION

#define FILESTORE_IGNORE_COMPLETION(name, ns)                                  \
    IgnoreFunc(ns::TEv##name##Completed);                                      \
// FILESTORE_IGNORE_COMPLETION

////////////////////////////////////////////////////////////////////////////////

struct TReadBlob
{
    struct TBlock
    {
        ui32 BlobOffset;
        ui32 BlockOffset;

        TBlock(ui32 blobOffset, ui32 blockOffset)
            : BlobOffset(blobOffset)
            , BlockOffset(blockOffset)
        {}
    };

    TPartialBlobId BlobId;
    TVector<TBlock> Blocks;

    TInstant Deadline = TInstant::Max();
    bool Async = false;

    TReadBlob(const TPartialBlobId& blobId, TVector<TBlock> blocks)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBlob
{
    TPartialBlobId BlobId;
    TString BlobContent;

    TInstant Deadline = TInstant::Max();
    bool Async = false;

    TWriteBlob(const TPartialBlobId& blobId, TString blobContent)
        : BlobId(blobId)
        , BlobContent(std::move(blobContent))
    {}
};

////////////////////////////////////////////////////////////////////////////////

enum class EAddBlobMode
{
    Write,
    WriteBatch,
    Flush,
    FlushBytes,
    Compaction,
};

////////////////////////////////////////////////////////////////////////////////

enum class EDeleteCheckpointMode
{
    MarkCheckpointDeleted,
    RemoveCheckpointNodes,
    RemoveCheckpointBlobs,
    RemoveCheckpoint,
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteRange
{
    ui64 NodeId = InvalidNodeId;
    ui64 MaxOffset = 0;
};

////////////////////////////////////////////////////////////////////////////////

using TCreateNodeInShardResult = std::variant<
    NProto::TCreateNodeResponse,
    NProto::TCreateHandleResponse>;

using TUnlinkNodeInShardResult = std::variant<
    NProto::TUnlinkNodeResponse,
    NProto::TRenameNodeResponse,
    NProtoPrivate::TRenameNodeInDestinationResponse>;

////////////////////////////////////////////////////////////////////////////////

struct TEvIndexTabletPrivate
{
    //
    // Operation completion
    //

    struct TIndexOperationCompleted
    {
        const TSet<ui32> MixedBlocksRanges;
        const ui64 CommitId;

        TIndexOperationCompleted(
                TSet<ui32> mixedBlocksRanges,
                ui64 commitId)
            : MixedBlocksRanges(std::move(mixedBlocksRanges))
            , CommitId(commitId)
        {
        }
    };

    //
    // SyncSessions
    //

    struct TShardSessionsInfo
    {
        TString ShardId;
        ui32 SessionCount = 0;
    };

    struct TSyncSessionsRequest
    {
    };

    struct TSyncSessionsResponse
    {
    };

    struct TSyncSessionsCompleted
    {
        TVector<TShardSessionsInfo> ShardSessionsInfos;
    };

    struct TSyncShardSessionsRequest
    {
        TString ShardId;
        NProtoPrivate::TDescribeSessionsResponse Sessions;
    };

    struct TSyncShardSessionsResponse
    {
        TShardSessionsInfo Info;
    };

    //
    // CleanupSessions
    //

    struct TCleanupSessionsRequest
    {
    };

    struct TCleanupSessionsResponse
    {
    };

    using TCleanupSessionsCompleted = TEmpty;

    //
    // WriteBatch
    //
    struct TWriteBatchRequest
    {
    };

    struct TWriteBatchResponse
    {
    };

    using TWriteBatchCompleted = TIndexOperationCompleted;

    //
    // ReadBlob
    //

    struct TReadBlobRequest
    {
        IBlockBufferPtr Buffer;
        TVector<TReadBlob> Blobs;
    };

    struct TReadBlobResponse
    {
    };

    struct TDataOperationCompleted
    {
        const ui32 Count;
        const ui32 Size;
        const TDuration Time;

        TDataOperationCompleted(
                ui32 requestCount,
                ui32 requestBytes,
                TDuration d)
            : Count(requestCount)
            , Size(requestBytes)
            , Time(d)
        {
        }
    };

    struct TOperationCompleted
        : TIndexOperationCompleted
        , TDataOperationCompleted
    {
        TOperationCompleted(
                TSet<ui32> mixedBlocksRanges,
                ui64 commitId,
                ui32 requestCount,
                ui32 requestBytes,
                TDuration d)
            : TIndexOperationCompleted(std::move(mixedBlocksRanges), commitId)
            , TDataOperationCompleted(requestCount, requestBytes, d)
        {
        }
    };

    using TReadBlobCompleted = TDataOperationCompleted;

    //
    // WriteBlob
    //

    struct TWriteBlobRequest
    {
        TVector<TWriteBlob> Blobs;
    };

    struct TWriteBlobResponse
    {
    };

    struct TWriteBlobCompleted: TDataOperationCompleted
    {
        struct TWriteRequestResult
        {
            NKikimr::TLogoBlobID BlobId;
            NKikimr::TStorageStatusFlags StorageStatusFlags;
            double ApproximateFreeSpaceShare = 0;
        };

        const TVector<TWriteRequestResult> Results;

        TWriteBlobCompleted(
                ui32 requestCount,
                ui32 requestBytes,
                TDuration d,
                TVector<TWriteRequestResult> results)
            : TDataOperationCompleted(requestCount, requestBytes, d)
            , Results(std::move(results))
        {
        }
    };

    //
    // ReadWrite completion
    //

    using TReadWriteCompleted = TOperationCompleted;

    //
    // AddData completion
    //

    struct TAddDataCompleted: TDataOperationCompleted
    {
        const ui64 CommitId;

        TAddDataCompleted(
                ui32 requestCount,
                ui32 requestBytes,
                TDuration d,
                ui64 commitId)
            : TDataOperationCompleted(requestCount, requestBytes, d)
            , CommitId(commitId)
        {
        }
    };

    //
    // AddBlob
    //

    struct TAddBlobRequest
    {
        EAddBlobMode Mode;
        TVector<TMixedBlobMeta> SrcBlobs;
        TVector<TBlock> SrcBlocks;
        TVector<TMixedBlobMeta> MixedBlobs;
        TVector<TMergedBlobMeta> MergedBlobs;
        TVector<TWriteRange> WriteRanges;
        TVector<TBlockBytesMeta> UnalignedDataParts;
    };

    struct TAddBlobResponse
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


    using TFlushCompleted = TOperationCompleted;

    //
    // FlushBytes
    //

    struct TFlushBytesRequest
    {
    };

    struct TFlushBytesResponse
    {
    };

    struct TFlushBytesCompleted: TDataOperationCompleted
    {
        const TCallContextPtr CallContext;
        const TSet<ui32> MixedBlocksRanges;
        const ui64 CommitId;
        const ui64 ChunkId;

        TFlushBytesCompleted(
                ui32 requestCount,
                ui32 requestBytes,
                TDuration d,
                TCallContextPtr callContext,
                TSet<ui32> mixedBlocksRanges,
                ui64 commitId,
                ui64 chunkId)
            : TDataOperationCompleted(requestCount, requestBytes, d)
            , CallContext(std::move(callContext))
            , MixedBlocksRanges(std::move(mixedBlocksRanges))
            , CommitId(commitId)
            , ChunkId(chunkId)
        {
        }
    };

    //
    // Cleanup
    //

    struct TCleanupRequest
    {
        ui32 RangeId;

        explicit TCleanupRequest(ui32 rangeId)
            : RangeId(rangeId)
        {}
    };

    struct TCleanupResponse
    {
    };

    //
    // Compaction
    //

    struct TCompactionRequest
    {
        const ui32 RangeId;
        const bool FilterNodes;

        TCompactionRequest(ui32 rangeId, bool filterNodes)
            : RangeId(rangeId)
            , FilterNodes(filterNodes)
        {}
    };

    struct TCompactionResponse
    {
    };

    using TCompactionCompleted = TOperationCompleted;

    //
    // DeleteZeroCompactionRanges
    //

    struct TDeleteZeroCompactionRangesRequest
    {
        const ui32 RangeId;

        explicit TDeleteZeroCompactionRangesRequest(ui32 rangeId)
            : RangeId(rangeId)
        {}
    };

    struct TDeleteZeroCompactionRangesResponse
    {
    };

    using TDeleteZeroCompactionRangesCompleted = TOperationCompleted;

    //
    // LoadCompactionMapChunk
    //

    struct TLoadCompactionMapChunkRequest
    {
        const ui32 FirstRangeId;
        const ui32 RangeCount;
        const bool OutOfOrder;

        TLoadCompactionMapChunkRequest(
                ui32 firstRangeId,
                ui32 rangeCount,
                bool outOfOrder)
            : FirstRangeId(firstRangeId)
            , RangeCount(rangeCount)
            , OutOfOrder(outOfOrder)
        {}
    };

    struct TLoadCompactionMapChunkResponse
    {
        const ui32 FirstRangeId = 0;
        const ui32 LastRangeId = 0;

        TLoadCompactionMapChunkResponse() = default;

        TLoadCompactionMapChunkResponse(
                ui32 firstRangeId,
                ui32 lastRangeId)
            : FirstRangeId(firstRangeId)
            , LastRangeId(lastRangeId)
        {}
    };

    //
    // LoadNodeRefs
    //

    struct TLoadNodeRefsRequest
    {
        const ui64 NodeId;
        const TString Cookie;
        const ui32 MaxNodeRefs;
        const TDuration SchedulePeriod;

        TLoadNodeRefsRequest(
                ui64 nodeId,
                TString cookie,
                ui32 maxNodeRefs,
                TDuration schedulePeriod)
            : NodeId(nodeId)
            , Cookie(std::move(cookie))
            , MaxNodeRefs(maxNodeRefs)
            , SchedulePeriod(schedulePeriod)
        {}
    };

    //
    // LoadNodes
    //

    struct TLoadNodesRequest
    {
        const ui64 NodeId;
        const ui32 MaxNodes;
        const TDuration SchedulePeriod;

        TLoadNodesRequest(
                ui64 nodeId,
                ui32 maxNodes,
                TDuration schedulePeriod)
            : NodeId(nodeId)
            , MaxNodes(maxNodes)
            , SchedulePeriod(schedulePeriod)
        {}
    };

    //
    // ForcedRangeOperation
    //

    enum EForcedRangeOperationMode
    {
        Compaction = 0,
        Cleanup = 1,
        DeleteZeroCompactionRanges = 2,
    };

    struct TForcedRangeOperationRequest
    {
        TVector<ui32> Ranges;
        EForcedRangeOperationMode Mode;
        TString OperationId;

        TForcedRangeOperationRequest(
                TVector<ui32> ranges,
                EForcedRangeOperationMode mode,
                TString operationId)
            : Ranges(std::move(ranges))
            , Mode(mode)
            , OperationId(std::move(operationId))
        {}
    };

    struct TForcedRangeOperationResponse
    {
    };

    using TForcedRangeOperationCompleted = TEmpty;

    struct TForcedRangeOperationProgress
    {
        const ui32 Current;

        explicit TForcedRangeOperationProgress(ui32 current)
            : Current(current)
        {
        }
    };

    //
    // NodeCreatedInShard
    //

    struct TNodeCreatedInShard
    {
        const TRequestInfoPtr RequestInfo;
        const TString SessionId;
        const ui64 RequestId;
        const ui64 OpLogEntryId;
        const TString NodeName;
        TCreateNodeInShardResult Result;

        TNodeCreatedInShard(
                TRequestInfoPtr requestInfo,
                TString sessionId,
                ui64 requestId,
                ui64 opLogEntryId,
                TString nodeName,
                TCreateNodeInShardResult result)
            : RequestInfo(std::move(requestInfo))
            , SessionId(std::move(sessionId))
            , RequestId(requestId)
            , OpLogEntryId(opLogEntryId)
            , NodeName(std::move(nodeName))
            , Result(std::move(result))
        {
        }
    };

    //
    // NodeUnlinkedInShard
    //

    struct TNodeUnlinkedInShard
    {
        const TRequestInfoPtr RequestInfo;
        const TString SessionId;
        const ui64 RequestId;
        const ui64 OpLogEntryId;
        TUnlinkNodeInShardResult Result;
        bool ShouldUnlockUponCompletion = false;
        NProto::TUnlinkNodeRequest OriginalRequest;

        TNodeUnlinkedInShard(
                TRequestInfoPtr requestInfo,
                TString sessionId,
                ui64 requestId,
                ui64 opLogEntryId,
                TUnlinkNodeInShardResult result,
                bool shouldUnlockUponCompletion,
                NProto::TUnlinkNodeRequest originalRequest)
            : RequestInfo(std::move(requestInfo))
            , SessionId(std::move(sessionId))
            , RequestId(requestId)
            , OpLogEntryId(opLogEntryId)
            , Result(std::move(result))
            , ShouldUnlockUponCompletion(shouldUnlockUponCompletion)
            , OriginalRequest(std::move(originalRequest))
        {
        }
    };

    //
    // NodeRenamedInDestination
    //

    struct TNodeRenamedInDestination
    {
        const TRequestInfoPtr RequestInfo;
        const TString SessionId;
        const ui64 RequestId;
        const ui64 OpLogEntryId;
        NProto::TRenameNodeRequest Request;
        NProtoPrivate::TRenameNodeInDestinationResponse Response;

        TNodeRenamedInDestination(
                TRequestInfoPtr requestInfo,
                TString sessionId,
                ui64 requestId,
                ui64 opLogEntryId,
                NProto::TRenameNodeRequest request,
                NProtoPrivate::TRenameNodeInDestinationResponse response)
            : RequestInfo(std::move(requestInfo))
            , SessionId(std::move(sessionId))
            , RequestId(requestId)
            , OpLogEntryId(opLogEntryId)
            , Request(std::move(request))
            , Response(std::move(response))
        {
        }
    };

    //
    // DumpCompactionRange
    //

    struct TDumpCompactionRangeRequest
    {
        const ui32 RangeId;

        explicit TDumpCompactionRangeRequest(ui32 rangeId)
            : RangeId(rangeId)
        {}
    };

    struct TDumpCompactionRangeResponse
    {
        const ui32 RangeId = 0;
        TVector<TMixedBlobMeta> Blobs;

        TDumpCompactionRangeResponse() = default;

        TDumpCompactionRangeResponse(ui32 rangeId, TVector<TMixedBlobMeta> blobs)
            : RangeId(rangeId)
            , Blobs(std::move(blobs))
        {}
    };

    using TDumpCompactionRangeCompleted = TEmpty;

    //
    // CollectGarbage
    //

    using TCollectGarbageRequest = TEmpty;

    using TCollectGarbageResponse = TEmpty;

    using TCollectGarbageCompleted = TDataOperationCompleted;

    //
    // DeleteGarbage
    //

    struct TDeleteGarbageRequest
    {
        ui64 CollectCommitId;
        TVector<TPartialBlobId> NewBlobs;
        TVector<TPartialBlobId> GarbageBlobs;

        TDeleteGarbageRequest(
                ui64 collectCommitId,
                TVector<TPartialBlobId> newBlobs,
                TVector<TPartialBlobId> garbageBlobs)
            : CollectCommitId(collectCommitId)
            , NewBlobs(std::move(newBlobs))
            , GarbageBlobs(std::move(garbageBlobs))
        {}
    };

    struct TDeleteGarbageResponse
    {
    };

    //
    // DeleteCheckpoint
    //

    struct TDeleteCheckpointRequest
    {
        TString CheckpointId;
        EDeleteCheckpointMode Mode;

        TDeleteCheckpointRequest(
                TString checkpointId,
                EDeleteCheckpointMode mode)
            : CheckpointId(std::move(checkpointId))
            , Mode(mode)
        {}
    };

    struct TDeleteCheckpointResponse
    {
    };

    using TDeleteCheckpointCompleted = TEmpty;

    //
    //  Truncate node
    //

    struct TTruncateRangeRequest
    {
        ui64 NodeId = 0;
        TByteRange Range;

        TTruncateRangeRequest(ui64 nodeId, TByteRange range)
            : NodeId(nodeId)
            , Range(range)
        {}
    };

    struct TTruncateRangeResponse
    {};

    //
    //  Truncate node
    //

    struct TTruncateRequest
    {
        ui64 NodeId = 0;
        TByteRange Range;

        TTruncateRequest(ui64 nodeId, TByteRange range)
            : NodeId(nodeId)
            , Range(range)
        {}
    };

    struct TTruncateResponse
    {
        ui64 NodeId = 0;
    };

    struct TTruncateCompleted
    {
        ui64 NodeId = 0;
    };

    //
    // Zero range
    //

    struct TZeroRangeRequest
    {
        ui64 NodeId = 0;
        TByteRange Range;

        TZeroRangeRequest(ui64 nodeId, TByteRange range)
            : NodeId(nodeId)
            , Range(range)
        {}
    };

    struct TZeroRangeResponse
    {
        ui64 NodeId = 0;
    };

    //
    // Filter alive nodes
    //

    struct TFilterAliveNodesRequest
    {
        TStackVec<ui64, 16> Nodes;

        TFilterAliveNodesRequest(TStackVec<ui64, 16> nodes)
            : Nodes(std::move(nodes))
        {}
    };

    struct TFilterAliveNodesResponse
    {
        TSet<ui64> Nodes;
    };

    //
    // Release collect barrier
    //

    struct TReleaseCollectBarrier
    {
        // Commit id to release
        ui64 CommitId;
        // Number of times to perform the release
        ui32 Count;

        TReleaseCollectBarrier(ui64 commitId, ui32 count)
            : CommitId(commitId)
            , Count(count)
        {}
    };

    //
    // Generate commit id
    //

    using TGenerateCommitIdRequest = TEmpty;

    struct TGenerateCommitIdResponse
    {
        ui64 CommitId = InvalidCommitId;
    };

    //
    // GetShardStats
    //

    struct TGetShardStatsCompleted
    {
        NProtoPrivate::TStorageStats AggregateStats;
        TVector<TShardStats> ShardStats;
        TInstant StartedTs;
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TFileStoreEventsPrivate::TABLET_START,

        FILESTORE_TABLET_REQUESTS_PRIVATE_SYNC(FILESTORE_DECLARE_EVENT_IDS)
        FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC(FILESTORE_DECLARE_PRIVATE_EVENT_IDS)

        EvUpdateCounters,
        EvUpdateLeakyBucketCounters,

        EvReadDataCompleted,
        EvWriteDataCompleted,
        EvAddDataCompleted,

        EvReleaseCollectBarrier,

        EvForcedRangeOperationProgress,

        EvNodeCreatedInShard,
        EvNodeUnlinkedInShard,
        EvNodeRenamedInDestination,

        EvGetShardStatsCompleted,

        EvShardRequestCompleted,

        EvLoadNodeRefs,
        EvLoadNodes,

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEventsPrivate::TABLET_END,
        "EvEnd expected to be < TFileStoreEventsPrivate::TABLET_END");

    FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC(FILESTORE_DECLARE_PRIVATE_EVENTS)
    FILESTORE_TABLET_REQUESTS_PRIVATE_SYNC(FILESTORE_DECLARE_EVENTS)

    using TEvUpdateCounters = TRequestEvent<TEmpty, EvUpdateCounters>;
    using TEvUpdateLeakyBucketCounters =
        TRequestEvent<TEmpty, EvUpdateLeakyBucketCounters>;

    using TEvReleaseCollectBarrier =
        TRequestEvent<TReleaseCollectBarrier, EvReleaseCollectBarrier>;

    using TEvReadDataCompleted =
        TResponseEvent<TReadWriteCompleted, EvReadDataCompleted>;
    using TEvWriteDataCompleted =
        TResponseEvent<TReadWriteCompleted, EvWriteDataCompleted>;
    using TEvAddDataCompleted =
        TResponseEvent<TAddDataCompleted, EvAddDataCompleted>;

    using TEvForcedRangeOperationProgress = TRequestEvent<
        TForcedRangeOperationProgress,
        EvForcedRangeOperationProgress>;

    using TEvNodeCreatedInShard =
        TRequestEvent<TNodeCreatedInShard, EvNodeCreatedInShard>;

    using TEvNodeUnlinkedInShard =
        TRequestEvent<TNodeUnlinkedInShard, EvNodeUnlinkedInShard>;

    using TEvNodeRenamedInDestination =
        TRequestEvent<TNodeRenamedInDestination, EvNodeRenamedInDestination>;

    using TEvGetShardStatsCompleted =
        TResponseEvent<TGetShardStatsCompleted, EvGetShardStatsCompleted>;

    using TEvShardRequestCompleted =
        TResponseEvent<TEmpty, EvShardRequestCompleted>;

    using TEvLoadNodeRefsRequest =
        TRequestEvent<TLoadNodeRefsRequest, EvLoadNodeRefs>;

    using TEvLoadNodesRequest =
        TRequestEvent<TLoadNodesRequest, EvLoadNodes>;
};

}   // namespace NCloud::NFileStore::NStorage
