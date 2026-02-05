#pragma once

#include "public.h"

#include "tablet_database.h"
#include "tablet_state_cache.h"

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/request_info.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>
#include <cloud/filestore/libs/storage/model/public.h>
#include <cloud/filestore/libs/storage/tablet/events/tablet_private.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>
#include <cloud/filestore/libs/storage/tablet/model/range_locks.h>
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/folder/pathsplit.h>
#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_VALIDATE_TX_SESSION(event, args)                             \
    if (auto session =                                                         \
            FindSession(args.ClientId, args.SessionId, args.SessionSeqNo);     \
        !session)                                                              \
    {                                                                          \
        args.Error = ErrorInvalidSession(                                      \
            args.ClientId,                                                     \
            args.SessionId,                                                    \
            args.SessionSeqNo);                                                \
        return true;                                                           \
    }                                                                          \
// FILESTORE_VALIDATE_TX_SESSION

#define FILESTORE_VALIDATE_DUPTX_SESSION(event, args)                          \
    FILESTORE_VALIDATE_TX_SESSION(event, args)                                 \
    else if (session->LookupDupEntry(args.RequestId)) {                        \
        args.Error = ErrorDuplicate();                                         \
        return true;                                                           \
    }                                                                          \
// FILESTORE_VALIDATE_TX_SESSION

#define FILESTORE_VALIDATE_TX_ERROR(event, args)                               \
    if (FAILED(args.Error.GetCode())) {                                        \
        return;                                                                \
    }                                                                          \
// FILESTORE_VALIDATE_TX_ERROR

////////////////////////////////////////////////////////////////////////////////

// read-only transactions can be executed atop of an in-memory cache
#define FILESTORE_TABLET_INDEX_RO_TRANSACTIONS(xxx, ...)                       \
    xxx(ResolvePath,                        __VA_ARGS__)                       \
    xxx(AccessNode,                         __VA_ARGS__)                       \
    xxx(ListNodes,                          __VA_ARGS__)                       \
    xxx(ReadLink,                           __VA_ARGS__)                       \
                                                                               \
    xxx(GetNodeAttr,                        __VA_ARGS__)                       \
    xxx(GetNodeAttrBatch,                   __VA_ARGS__)                       \
    xxx(GetNodeXAttr,                       __VA_ARGS__)                       \
    xxx(ListNodeXAttr,                      __VA_ARGS__)                       \
                                                                               \
    xxx(UnsafeGetNode,                      __VA_ARGS__)                       \
    xxx(UnsafeGetNodeRef,                   __VA_ARGS__)                       \
                                                                               \
    xxx(LoadNodeRefs,                       __VA_ARGS__)                       \
    xxx(LoadNodes,                          __VA_ARGS__)                       \
                                                                               \
    xxx(ReadData,                           __VA_ARGS__)                       \
                                                                               \
    xxx(ReadNodeRefs,                       __VA_ARGS__)                       \
// FILESTORE_TABLET_RO_TRANSACTIONS

#define FILESTORE_TABLET_RW_TRANSACTIONS(xxx, ...)                             \
    xxx(InitSchema,                         __VA_ARGS__)                       \
    xxx(LoadState,                          __VA_ARGS__)                       \
    xxx(LoadCompactionMapChunk,             __VA_ARGS__)                       \
    xxx(UpdateConfig,                       __VA_ARGS__)                       \
    xxx(ConfigureShards,                    __VA_ARGS__)                       \
    xxx(ConfigureAsShard,                   __VA_ARGS__)                       \
                                                                               \
    xxx(CreateSession,                      __VA_ARGS__)                       \
    xxx(ResetSession,                       __VA_ARGS__)                       \
    xxx(DestroySession,                     __VA_ARGS__)                       \
                                                                               \
    xxx(CreateCheckpoint,                   __VA_ARGS__)                       \
    xxx(DeleteCheckpoint,                   __VA_ARGS__)                       \
                                                                               \
    xxx(CreateNode,                         __VA_ARGS__)                       \
    xxx(UnlinkNode,                         __VA_ARGS__)                       \
    xxx(CompleteUnlinkNode,                 __VA_ARGS__)                       \
    xxx(PrepareUnlinkDirectoryNode,         __VA_ARGS__)                       \
    xxx(AbortUnlinkDirectoryNode,           __VA_ARGS__)                       \
    xxx(RenameNode,                         __VA_ARGS__)                       \
    xxx(PrepareRenameNodeInSource,          __VA_ARGS__)                       \
    xxx(RenameNodeInDestination,            __VA_ARGS__)                       \
    xxx(CommitRenameNodeInSource,           __VA_ARGS__)                       \
                                                                               \
    xxx(SetNodeAttr,                        __VA_ARGS__)                       \
    xxx(SetNodeXAttr,                       __VA_ARGS__)                       \
    xxx(RemoveNodeXAttr,                    __VA_ARGS__)                       \
    xxx(SetHasXAttrs,                       __VA_ARGS__)                       \
                                                                               \
    xxx(CreateHandle,                       __VA_ARGS__)                       \
    xxx(DestroyHandle,                      __VA_ARGS__)                       \
                                                                               \
    xxx(AcquireLock,                        __VA_ARGS__)                       \
    xxx(ReleaseLock,                        __VA_ARGS__)                       \
    xxx(TestLock,                           __VA_ARGS__)                       \
                                                                               \
    xxx(WriteData,                          __VA_ARGS__)                       \
    xxx(AddData,                            __VA_ARGS__)                       \
    xxx(WriteBatch,                         __VA_ARGS__)                       \
    xxx(AllocateData,                       __VA_ARGS__)                       \
                                                                               \
    xxx(AddBlob,                            __VA_ARGS__)                       \
    xxx(Cleanup,                            __VA_ARGS__)                       \
    xxx(Compaction,                         __VA_ARGS__)                       \
    xxx(DeleteZeroCompactionRanges,         __VA_ARGS__)                       \
    xxx(WriteCompactionMap,                 __VA_ARGS__)                       \
    xxx(DeleteGarbage,                      __VA_ARGS__)                       \
    xxx(DumpCompactionRange,                __VA_ARGS__)                       \
    xxx(FlushBytes,                         __VA_ARGS__)                       \
    xxx(TrimBytes,                          __VA_ARGS__)                       \
    xxx(TruncateCompleted,                  __VA_ARGS__)                       \
    xxx(TruncateRange,                      __VA_ARGS__)                       \
    xxx(ZeroRange,                          __VA_ARGS__)                       \
                                                                               \
    xxx(FilterAliveNodes,                   __VA_ARGS__)                       \
    xxx(ChangeStorageConfig,                __VA_ARGS__)                       \
                                                                               \
    xxx(DeleteOpLogEntry,                   __VA_ARGS__)                       \
    xxx(GetOpLogEntry,                      __VA_ARGS__)                       \
    xxx(WriteOpLogEntry,                    __VA_ARGS__)                       \
    xxx(CommitNodeCreationInShard,          __VA_ARGS__)                       \
                                                                               \
    xxx(UnsafeDeleteNode,                   __VA_ARGS__)                       \
    xxx(UnsafeUpdateNode,                   __VA_ARGS__)                       \
    xxx(UnsafeCreateNodeRef,                __VA_ARGS__)                       \
    xxx(UnsafeDeleteNodeRef,                __VA_ARGS__)                       \
    xxx(UnsafeUpdateNodeRef,                __VA_ARGS__)                       \
    xxx(UnsafeCreateHandle,                 __VA_ARGS__)                       \
// FILESTORE_TABLET_RW_TRANSACTIONS

#define FILESTORE_TABLET_TRANSACTIONS(xxx, ...)                                \
    FILESTORE_TABLET_INDEX_RO_TRANSACTIONS(xxx, __VA_ARGS__)                   \
    FILESTORE_TABLET_RW_TRANSACTIONS(xxx, __VA_ARGS__)                         \
// FILESTORE_TABLET_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TErrorAware
{
    NProto::TError Error;
    bool CommitIdOverflow = false;
    TString CommitIdOverflowMessage;

    void OnCommitIdOverflow()
    {
        Error = ErrorCommitIdOverflow();
        CommitIdOverflow = true;
    }

    void Clear()
    {
        Y_DEBUG_ABORT_UNLESS(!HasError(Error));
        Y_DEBUG_ABORT_UNLESS(!CommitIdOverflow);

        if (HasError(Error) || CommitIdOverflow) {
            Error = MakeError(E_INVALID_STATE, TStringBuilder()
                << "Attempt to clear tx context with an error"
                << ", Error: " << FormatError(Error)
                << ", CommitIdOverflow: " << CommitIdOverflow);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSessionAware
{
    const TString ClientId;
    const TString FileSystemId;
    const TString SessionId;
    const ui64 RequestId;
    const ui64 SessionSeqNo;

    template<typename T>
    explicit TSessionAware(const T& request) noexcept
        : ClientId(GetClientId(request))
        , FileSystemId(GetFileSystemId(request))
        , SessionId(GetSessionId(request))
        , RequestId(GetRequestId(request))
        , SessionSeqNo(GetSessionSeqNo(request))
    {}
};

/**
 * @brief Transactions, derived from this class, may modify inode-related data.
 * Thus, to guarantee consistency, they store all the changes applied to this
 * data in the request log.
 */
struct TIndexStateNodeUpdates
{
    TVector<TInMemoryIndexState::TIndexStateRequest> NodeUpdates;

    void Clear()
    {
        NodeUpdates.clear();
    }
};

struct TProfileAware
{
    NProto::TProfileLogRequestInfo ProfileLogRequest;

    explicit TProfileAware(EFileStoreSystemRequest requestType) noexcept
    {
        ProfileLogRequest.SetRequestType(static_cast<ui32>(requestType));
    }

    explicit TProfileAware(NProto::TProfileLogRequestInfo profileLogRequest) noexcept
        : ProfileLogRequest(std::move(profileLogRequest))
    {
    }

protected:
    void Clear()
    {
        const ui32 requestType = ProfileLogRequest.GetRequestType();

        ProfileLogRequest.Clear();
        ProfileLogRequest.SetRequestType(requestType);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTxIndexTabletBase
{
    virtual ~TTxIndexTabletBase() = default;

    virtual void Clear() = 0;

    // It is possible to implement custom logic that mutates the transaction
    // state upon transaction restart
    virtual void OnRestart()
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteRequest
    : TIntrusiveListItem<TWriteRequest>
    , TErrorAware
    , TSessionAware
{
    const TRequestInfoPtr RequestInfo;
    const ui64 Handle;
    const TByteRange ByteRange;
    const IBlockBufferPtr Buffer;

    ui64 NodeId = InvalidNodeId;

    TWriteRequest(
            TRequestInfoPtr requestInfo,
            const NProto::TWriteDataRequest& request,
            TByteRange byteRange,
            IBlockBufferPtr buffer)
        : TSessionAware(request)
        , RequestInfo(std::move(requestInfo))
        , Handle(request.GetHandle())
        , ByteRange(byteRange)
        , Buffer(std::move(buffer))
    {}
};

using TWriteRequestList = TIntrusiveListWithAutoDelete<TWriteRequest, TDelete>;

////////////////////////////////////////////////////////////////////////////////

struct TNodeOps
{
    template <typename T>
    static auto GetNodeId(const T& value)
    {
        return value;
    }

    static auto GetNodeId(const IIndexTabletDatabase::TNode& node)
    {
        return node.NodeId;
    }

    struct TNodeSetHash
    {
        template <typename T>
        size_t operator ()(const T& value) const noexcept
        {
            return IntHash(GetNodeId(value));
        }
    };

    struct TNodeSetEqual
    {
        template <typename T1, typename T2>
        bool operator ()(const T1& lhs, const T2& rhs) const noexcept
        {
            return GetNodeId(lhs) == GetNodeId(rhs);
        }
    };
};

using TNodeSet = THashSet<
    IIndexTabletDatabase::TNode,
    TNodeOps::TNodeSetHash,
    TNodeOps::TNodeSetEqual>;

////////////////////////////////////////////////////////////////////////////////

struct TTxIndexTablet
{
    //
    // InitSchema
    //

    struct TInitSchema: TTxIndexTabletBase
    {
        // actually unused, needed in tablet_tx.h to avoid sophisticated
        // template tricks
        const TRequestInfoPtr RequestInfo;
        const bool UseNoneCompactionPolicy;

        TInitSchema(bool useNoneCompactionPolicy)
            : UseNoneCompactionPolicy(useNoneCompactionPolicy)
        {}

        void Clear() override
        {
            // nothing to do
        }
    };

    //
    // LoadState
    //

    struct TLoadState: TTxIndexTabletBase, TErrorAware
    {
        // actually unused, needed in tablet_tx.h to avoid sophisticated
        // template tricks
        const TRequestInfoPtr RequestInfo;

        NProto::TFileSystem FileSystem;
        NProto::TFileSystemStats FileSystemStats;
        NCloud::NProto::TTabletStorageInfo TabletStorageInfo;
        TMaybe<IIndexTabletDatabase::TNode> RootNode;
        TVector<NProto::TSession> Sessions;
        TVector<NProto::TSessionHandle> Handles;
        TVector<NProto::TSessionLock> Locks;
        TVector<TIndexTabletDatabase::TFreshBytesEntry> FreshBytes;
        TVector<TIndexTabletDatabase::TFreshBlock> FreshBlocks;
        TVector<TPartialBlobId> NewBlobs;
        TVector<TPartialBlobId> GarbageBlobs;
        TVector<NProto::TCheckpoint> Checkpoints;
        TVector<NProto::TDupCacheEntry> DupCache;
        TVector<NProto::TTruncateEntry> TruncateQueue;
        TMaybe<NProto::TStorageConfig> StorageConfig;
        TVector<NProto::TSessionHistoryEntry> SessionHistory;
        TVector<NProto::TOpLogEntry> OpLog;
        TVector<TDeletionMarker> LargeDeletionMarkers;
        TVector<ui64> OrphanNodeIds;

        void Clear() override
        {
            TErrorAware::Clear();

            FileSystem.Clear();
            FileSystemStats.Clear();
            TabletStorageInfo.Clear();
            RootNode.Clear();
            Sessions.clear();
            Handles.clear();
            Locks.clear();
            FreshBytes.clear();
            FreshBlocks.clear();
            NewBlobs.clear();
            GarbageBlobs.clear();
            Checkpoints.clear();
            DupCache.clear();
            TruncateQueue.clear();
            StorageConfig.Clear();
            SessionHistory.clear();
            OpLog.clear();
            LargeDeletionMarkers.clear();
            OrphanNodeIds.clear();
        }
    };

    //
    // LoadCompactionMapChunk
    //

    struct TLoadCompactionMapChunk: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const ui32 FirstRangeId;
        const ui32 RangeCount;

        TVector<TCompactionRangeInfo> CompactionMap;
        ui32 LastRangeId = 0;

        TLoadCompactionMapChunk(
                TRequestInfoPtr requestInfo,
                ui32 firstRangeId,
                ui32 rangeCount)
            : RequestInfo(std::move(requestInfo))
            , FirstRangeId(firstRangeId)
            , RangeCount(rangeCount)
        {
        }

        void Clear() override
        {
            CompactionMap.clear();
            LastRangeId = 0;
        }
    };

    //
    // UpdateConfig
    //

    struct TUpdateConfig: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 TxId;
        const NProto::TFileSystem FileSystem;

        TUpdateConfig(
                TRequestInfoPtr requestInfo,
                ui64 txId,
                NProto::TFileSystem fileSystem)
            : RequestInfo(std::move(requestInfo))
            , TxId(txId)
            , FileSystem(std::move(fileSystem))
        {}

        void Clear() override
        {
            // nothing to do
        }
    };

    //
    // ConfigureShards
    //

    struct TConfigureShards: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        NProtoPrivate::TConfigureShardsRequest Request;

        TConfigureShards(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TConfigureShardsRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            // nothing to do
        }
    };

    //
    // ConfigureAsShard
    //

    struct TConfigureAsShard: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        NProtoPrivate::TConfigureAsShardRequest Request;

        TConfigureAsShard(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TConfigureAsShardRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            // nothing to do
        }
    };

    //
    // CreateSession
    //

    struct TCreateSession: TTxIndexTabletBase, TErrorAware
    {
        /* const */ TRequestInfoPtr RequestInfo;
        /* const */ NProtoPrivate::TCreateSessionRequest Request;

        TString SessionId;

        TCreateSession(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TCreateSessionRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();

            SessionId.clear();
        }
    };

    //
    // ResetSession
    //

    struct TResetSession
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        /* const */ TRequestInfoPtr RequestInfo;
        const TString SessionId;
        const ui64 SessionSeqNo;
        /* const */ NProto::TResetSessionRequest Request;

        TNodeSet Nodes;

        TResetSession(
                TRequestInfoPtr requestInfo,
                TString sessionId,
                ui64 sessionSeqNo,
                NProto::TResetSessionRequest request)
            : RequestInfo(std::move(requestInfo))
            , SessionId(std::move(sessionId))
            , SessionSeqNo(sessionSeqNo)
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            Nodes.clear();
        }
    };

    //
    // DestroySession
    //

    struct TDestroySession
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        /* const */ TRequestInfoPtr RequestInfo;
        const TString SessionId;
        const ui64 SessionSeqNo;
        /* const */ NProtoPrivate::TDestroySessionRequest Request;

        TNodeSet Nodes;
        ui64 CommitId = InvalidCommitId;

        TDestroySession(
                TRequestInfoPtr requestInfo,
                TString sessionId,
                ui64 sessionSeqNo,
                NProtoPrivate::TDestroySessionRequest request)
            : RequestInfo(std::move(requestInfo))
            , SessionId(std::move(sessionId))
            , SessionSeqNo(sessionSeqNo)
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            Nodes.clear();
        }
    };

    //
    // CreateCheckpoint
    //

    struct TCreateCheckpoint
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const TString CheckpointId;
        const ui64 NodeId;

        ui64 CommitId = InvalidCommitId;

        TCreateCheckpoint(
                TRequestInfoPtr requestInfo,
                TString checkpointId,
                ui64 nodeId)
            : RequestInfo(std::move(requestInfo))
            , CheckpointId(std::move(checkpointId))
            , NodeId(nodeId)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
        }
    };

    //
    // DeleteCheckpoint
    //

    struct TDeleteCheckpoint
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const TString CheckpointId;
        const EDeleteCheckpointMode Mode;
        const ui64 CollectBarrier;

        ui64 CommitId = InvalidCommitId;

        TVector<ui64> NodeIds;
        TVector<IIndexTabletDatabase::TNode> Nodes;
        TVector<TIndexTabletDatabase::TNodeAttr> NodeAttrs;
        TVector<IIndexTabletDatabase::TNodeRef> NodeRefs;

        TVector<TIndexTabletDatabase::TCheckpointBlob> Blobs;
        TVector<TIndexTabletDatabase::IIndexTabletDatabase::TMixedBlob> MixedBlobs;

        // NOTE: should persist state across tx restarts
        TSet<ui32> MixedBlocksRanges;

        TDeleteCheckpoint(
                TRequestInfoPtr requestInfo,
                TString checkpointId,
                EDeleteCheckpointMode mode,
                ui64 collectBarrier)
            : RequestInfo(std::move(requestInfo))
            , CheckpointId(std::move(checkpointId))
            , Mode(mode)
            , CollectBarrier(collectBarrier)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;

            NodeIds.clear();
            Nodes.clear();
            NodeAttrs.clear();
            NodeRefs.clear();

            Blobs.clear();
            MixedBlobs.clear();
        }
    };

    //
    // ResolvePath
    //

    struct TResolvePath
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TResolvePathRequest Request;
        const TString Path;

        ui64 CommitId = InvalidCommitId;

        TResolvePath(
                TRequestInfoPtr requestInfo,
                const NProto::TResolvePathRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , Path(request.GetPath())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
        }
    };

    //
    // CreateNode
    //

    struct TCreateNode
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 ParentNodeId;
        const ui64 TargetNodeId;
        const TString Name;
        NProto::TNode Attrs;
        const TString RequestShardId;
        TString ShardId;
        TString ShardNodeName;
        NProto::TCreateNodeRequest Request;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> ParentNode;
        ui64 ChildNodeId = InvalidNodeId;
        TMaybe<IIndexTabletDatabase::TNode> ChildNode;

        NProto::TOpLogEntry OpLogEntry;

        NProto::TCreateNodeResponse Response;

        TCreateNode(
                TRequestInfoPtr requestInfo,
                NProto::TCreateNodeRequest request,
                ui64 parentNodeId,
                ui64 targetNodeId,
                NProto::TNode attrs)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , ParentNodeId(parentNodeId)
            , TargetNodeId(targetNodeId)
            , Name(request.GetName())
            , Attrs(std::move(attrs))
            , RequestShardId(request.GetShardFileSystemId())
            , ShardId(RequestShardId)
            , Request(std::move(request))
        {
        }

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            ParentNode.Clear();
            ChildNodeId = InvalidNodeId;
            ChildNode.Clear();

            OpLogEntry.Clear();

            Response.Clear();
        }
    };

    //
    // UnlinkNode
    //

    struct TUnlinkNode
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TUnlinkNodeRequest Request;
        const ui64 ParentNodeId;
        const TString Name;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> ParentNode;
        TMaybe<IIndexTabletDatabase::TNode> ChildNode;
        TMaybe<IIndexTabletDatabase::TNodeRef> ChildRef;

        NProto::TOpLogEntry OpLogEntry;

        TUnlinkNode(
                TRequestInfoPtr requestInfo,
                NProto::TUnlinkNodeRequest request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
            , ParentNodeId(Request.GetNodeId())
            , Name(Request.GetName())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            ParentNode.Clear();
            ChildNode.Clear();
            ChildRef.Clear();
            OpLogEntry.Clear();
        }
    };

    //
    // CompleteUnlinkNode
    //

    struct TCompleteUnlinkNode
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TUnlinkNodeRequest Request;
        const ui64 ParentNodeId;
        const TString Name;
        ui64 OpLogEntryId;
        NProto::TUnlinkNodeResponse Response;
        const bool IsExplicitRequest;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> ParentNode;
        TMaybe<IIndexTabletDatabase::TNode> ChildNode;
        TMaybe<IIndexTabletDatabase::TNodeRef> ChildRef;

        NProto::TOpLogEntry OpLogEntry;

        TCompleteUnlinkNode(
                TRequestInfoPtr requestInfo,
                NProto::TUnlinkNodeRequest request,
                ui64 opLogEntryId,
                NProto::TUnlinkNodeResponse response,
                bool isExplicitRequest)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
            , ParentNodeId(Request.GetNodeId())
            , Name(Request.GetName())
            , OpLogEntryId(opLogEntryId)
            , Response(std::move(response))
            , IsExplicitRequest(isExplicitRequest)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            ParentNode.Clear();
            ChildNode.Clear();
            ChildRef.Clear();
            OpLogEntry.Clear();
        }
    };

    //
    // PrepareUnlinkDirectoryNode
    //

    struct TPrepareUnlinkDirectoryNode
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TPrepareUnlinkDirectoryNodeInShardRequest Request;
        NProtoPrivate::TPrepareUnlinkDirectoryNodeInShardResponse Response;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;

        TPrepareUnlinkDirectoryNode(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TPrepareUnlinkDirectoryNodeInShardRequest request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
        }
    };

    //
    // AbortUnlinkDirectoryNode
    //

    struct TAbortUnlinkDirectoryNode
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TAbortUnlinkDirectoryNodeInShardRequest Request;
        NProtoPrivate::TAbortUnlinkDirectoryNodeInShardResponse Response;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;

        TAbortUnlinkDirectoryNode(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TAbortUnlinkDirectoryNodeInShardRequest request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
        }
    };

    //
    // RenameNode
    //

    struct TRenameNode
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 ParentNodeId;
        const TString Name;
        const ui64 NewParentNodeId;
        const TString NewName;
        const ui32 Flags;
        const NProto::TRenameNodeRequest Request;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> ParentNode;
        TMaybe<IIndexTabletDatabase::TNode> ChildNode;
        TMaybe<IIndexTabletDatabase::TNodeRef> ChildRef;

        TMaybe<IIndexTabletDatabase::TNode> NewParentNode;
        TMaybe<IIndexTabletDatabase::TNode> NewChildNode;
        TMaybe<IIndexTabletDatabase::TNodeRef> NewChildRef;

        NProto::TOpLogEntry OpLogEntry;

        NProto::TRenameNodeResponse Response;

        TString ShardIdForUnlink;
        TString ShardNodeNameForUnlink;

        TRenameNode(
                TRequestInfoPtr requestInfo,
                NProto::TRenameNodeRequest request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , ParentNodeId(request.GetNodeId())
            , Name(request.GetName())
            , NewParentNodeId(request.GetNewParentId())
            , NewName(request.GetNewName())
            , Flags(request.GetFlags())
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            ParentNode.Clear();
            ChildNode.Clear();
            ChildRef.Clear();

            NewParentNode.Clear();
            NewChildNode.Clear();
            NewChildRef.Clear();

            OpLogEntry.Clear();

            Response.Clear();

            ShardIdForUnlink.clear();
            ShardNodeNameForUnlink.clear();
        }
    };

    //
    // PrepareRenameNodeInSource
    //

    struct TPrepareRenameNodeInSource
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 ParentNodeId;
        const TString Name;
        const NProto::TRenameNodeRequest Request;
        const TString NewParentShardId;
        const bool IsExplicitRequest;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> ParentNode;
        TMaybe<IIndexTabletDatabase::TNode> ChildNode;
        TMaybe<IIndexTabletDatabase::TNodeRef> ChildRef;

        NProto::TOpLogEntry OpLogEntry;

        TPrepareRenameNodeInSource(
                TRequestInfoPtr requestInfo,
                NProto::TRenameNodeRequest request,
                TString newParentShardId,
                bool isExplicitRequest)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , ParentNodeId(request.GetNodeId())
            , Name(request.GetName())
            , Request(std::move(request))
            , NewParentShardId(std::move(newParentShardId))
            , IsExplicitRequest(isExplicitRequest)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            ParentNode.Clear();
            ChildNode.Clear();
            ChildRef.Clear();

            OpLogEntry.Clear();
        }
    };

    //
    // RenameNodeInDestination
    //

    struct TRenameNodeInDestination
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 NewParentNodeId;
        const TString NewName;
        const ui32 Flags;
        const NProtoPrivate::TRenameNodeInDestinationRequest Request;
        const NProto::TNodeAttr SourceNodeAttr;
        const NProto::TNodeAttr DestinationNodeAttr;
        const bool IsSecondPass;
        const ui64 AbortUnlinkOpLogEntryId;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> NewParentNode;
        TMaybe<IIndexTabletDatabase::TNodeRef> NewChildRef;

        NProto::TOpLogEntry OpLogEntry;
        NProtoPrivate::TRenameNodeInDestinationResponse Response;

        TString ShardIdForUnlink;
        TString ShardNodeNameForUnlink;

        bool SecondPassRequired = false;

        TRenameNodeInDestination(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TRenameNodeInDestinationRequest request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , NewParentNodeId(request.GetNewParentId())
            , NewName(request.GetNewName())
            , Flags(request.GetFlags())
            , Request(std::move(request))
            , IsSecondPass(false)
            , AbortUnlinkOpLogEntryId(0)
        {}

        TRenameNodeInDestination(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TRenameNodeInDestinationRequest request,
                NProto::TNodeAttr sourceNodeAttr,
                NProto::TNodeAttr destinationNodeAttr,
                ui64 abortUnlinkOpLogEntryId)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , NewParentNodeId(request.GetNewParentId())
            , NewName(request.GetNewName())
            , Flags(request.GetFlags())
            , Request(std::move(request))
            , SourceNodeAttr(std::move(sourceNodeAttr))
            , DestinationNodeAttr(std::move(destinationNodeAttr))
            , IsSecondPass(true)
            , AbortUnlinkOpLogEntryId(abortUnlinkOpLogEntryId)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;

            NewParentNode.Clear();
            NewChildRef.Clear();

            OpLogEntry.Clear();

            Response.Clear();

            ShardIdForUnlink.clear();
            ShardNodeNameForUnlink.clear();

            SecondPassRequired = false;
        }
    };

    //
    // CommitRenameNodeInSource
    //

    struct TCommitRenameNodeInSource
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TRenameNodeRequest Request;
        const NProtoPrivate::TRenameNodeInDestinationResponse Response;
        const ui64 OpLogEntryId;
        const bool IsExplicitRequest;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNodeRef> ChildRef;

        TCommitRenameNodeInSource(
                TRequestInfoPtr requestInfo,
                NProto::TRenameNodeRequest request,
                NProtoPrivate::TRenameNodeInDestinationResponse response,
                ui64 opLogEntryId,
                bool isExplicitRequest)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
            , Response(std::move(response))
            , OpLogEntryId(opLogEntryId)
            , IsExplicitRequest(isExplicitRequest)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            ChildRef.Clear();
        }
    };

    //
    // AccessNode
    //

    struct TAccessNode
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TAccessNodeRequest Request;
        const ui64 NodeId;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;

        TAccessNode(
                TRequestInfoPtr requestInfo,
                const NProto::TAccessNodeRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
        }
    };

    //
    // ReadLink
    //

    struct TReadLink
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TReadLinkRequest Request;
        const ui64 NodeId;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;

        TReadLink(
                TRequestInfoPtr requestInfo,
                const NProto::TReadLinkRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , NodeId(request.GetNodeId())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
        }
    };

    //
    // ListNodes
    //

    struct TListNodes
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TListNodesRequest Request;
        const ui64 NodeId;
        const TString Cookie;
        const ui32 MaxBytes;
        const ui32 MaxBytesMultiplier;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;
        TVector<IIndexTabletDatabase::TNodeRef> ChildRefs;
        TVector<IIndexTabletDatabase::TNode> ChildNodes;
        TString Next;

        ui32 BytesToPrecharge = 0;
        ui32 PrepareAttempts = 1;

        TListNodes(
                TRequestInfoPtr requestInfo,
                const NProto::TListNodesRequest& request,
                ui32 maxBytes,
                ui32 maxBytesMultiplier)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
            , Cookie(request.GetCookie())
            , MaxBytes(maxBytes)
            , MaxBytesMultiplier(maxBytesMultiplier)
            , BytesToPrecharge(MaxBytes)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
            ChildRefs.clear();
            ChildNodes.clear();
            Next.clear();
        }

        void OnRestart() override
        {
            ++PrepareAttempts;
            BytesToPrecharge = ClampVal(
                2 * BytesToPrecharge,
                MaxBytes,
                MaxBytesMultiplier * MaxBytes);
        }
    };

    //
    // SetNodeAttr
    //

    struct TSetNodeAttr
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TSetNodeAttrRequest Request;
        const ui64 NodeId;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;

        TSetNodeAttr(
                TRequestInfoPtr requestInfo,
                const NProto::TSetNodeAttrRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
        }
    };

    //
    // GetNodeAttr
    //

    struct TGetNodeAttr
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TGetNodeAttrRequest Request;
        const ui64 NodeId;
        const TString Name;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> ParentNode;
        ui64 TargetNodeId = InvalidNodeId;
        TMaybe<IIndexTabletDatabase::TNode> TargetNode;
        TString ShardId;
        TString ShardNodeName;

        TGetNodeAttr(
                TRequestInfoPtr requestInfo,
                const NProto::TGetNodeAttrRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
            , Name(request.GetName())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            ParentNode.Clear();
            TargetNodeId = InvalidNodeId;
            TargetNode.Clear();
            ShardId.clear();
            ShardNodeName.clear();
        }
    };

    //
    // GetNodeAttrBatch
    //

    struct TGetNodeAttrBatch
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TGetNodeAttrBatchRequest Request;

        NProtoPrivate::TGetNodeAttrBatchResponse Response;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> ParentNode;

        TGetNodeAttrBatch(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TGetNodeAttrBatchRequest request,
                NProtoPrivate::TGetNodeAttrBatchResponse response)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
            , Response(std::move(response))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            ParentNode.Clear();
        }
    };

    //
    // SetNodeXAttr
    //

    struct TSetNodeXAttr
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TSetNodeXAttrRequest Request;
        const ui64 NodeId;
        const TString Name;
        const TString Value;

        ui64 Version = 0;
        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;
        TMaybe<TIndexTabletDatabase::TNodeAttr> Attr;

        TSetNodeXAttr(
                TRequestInfoPtr requestInfo,
                const NProto::TSetNodeXAttrRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
            , Name(request.GetName())
            , Value(request.GetValue())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            Version = 0;
            CommitId = InvalidCommitId;
            Node.Clear();
            Attr.Clear();
        }
    };

    //
    // GetNodeXAttr
    //

    struct TGetNodeXAttr
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TGetNodeXAttrRequest Request;
        const ui64 NodeId;
        const TString Name;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;
        TMaybe<TIndexTabletDatabase::TNodeAttr> Attr;

        TGetNodeXAttr(
                TRequestInfoPtr requestInfo,
                const NProto::TGetNodeXAttrRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
            , Name(request.GetName())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
            Attr.Clear();
        }
    };

    //
    // ListNodeXAttr
    //

    struct TListNodeXAttr
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TListNodeXAttrRequest Request;
        const ui64 NodeId;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;
        TVector<TIndexTabletDatabase::TNodeAttr> Attrs;

        TListNodeXAttr(
                TRequestInfoPtr requestInfo,
                const NProto::TListNodeXAttrRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
            Attrs.clear();
        }
    };

    //
    // RemoveNodeXAttr
    //

    struct TRemoveNodeXAttr
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TRemoveNodeXAttrRequest Request;
        const ui64 NodeId;
        const TString Name;

        ui64 CommitId = InvalidCommitId;
        TMaybe<IIndexTabletDatabase::TNode> Node;
        TMaybe<TIndexTabletDatabase::TNodeAttr> Attr;

        TRemoveNodeXAttr(
                TRequestInfoPtr requestInfo,
                const NProto::TRemoveNodeXAttrRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
            , Name(request.GetName())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Node.Clear();
            Attr.Clear();
        }
    };

    //
    // SetHasXAttrs
    //

    struct TSetHasXAttrs
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TSetHasXAttrsRequest Request;
        // This flag is set only if the HasXAttrs flag is changed
        bool IsToBeChanged = false;

        TSetHasXAttrs(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TSetHasXAttrsRequest request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();

            IsToBeChanged = false;
        }
    };

    //
    // CreateHandle
    //

    struct TCreateHandle
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 NodeId;
        const TString Name;
        const ui32 Flags;
        const ui32 Mode;
        const ui32 Uid;
        ui32 Gid;
        const TString RequestShardId;
        NProto::TCreateHandleRequest Request;

        ui64 ReadCommitId = InvalidCommitId;
        ui64 WriteCommitId = InvalidCommitId;
        ui64 TargetNodeId = InvalidNodeId;
        TString ShardId;
        TString ShardNodeName;
        bool IsNewShardNode = false;
        TMaybe<IIndexTabletDatabase::TNode> TargetNode;
        TMaybe<IIndexTabletDatabase::TNode> ParentNode;
        TVector<ui64> UpdatedNodes;

        NProto::TOpLogEntry OpLogEntry;

        NProto::TCreateHandleResponse Response;

        TCreateHandle(
                TRequestInfoPtr requestInfo,
                NProto::TCreateHandleRequest request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , NodeId(request.GetNodeId())
            , Name(request.GetName())
            , Flags(request.GetFlags())
            , Mode(request.GetMode())
            , Uid(request.GetUid())
            , Gid(request.GetGid())
            , RequestShardId(request.GetShardFileSystemId())
            , Request(std::move(request))
        {
        }

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            ReadCommitId = InvalidCommitId;
            WriteCommitId = InvalidCommitId;
            TargetNodeId = InvalidNodeId;
            ShardId.clear();
            ShardNodeName.clear();
            IsNewShardNode = false;
            TargetNode.Clear();
            ParentNode.Clear();
            UpdatedNodes.clear();

            OpLogEntry.Clear();

            Response.Clear();
        }
    };

    //
    // DestroyHandle
    //

    struct TDestroyHandle
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TDestroyHandleRequest Request;

        TMaybe<IIndexTabletDatabase::TNode> Node;

        TDestroyHandle(
                TRequestInfoPtr requestInfo,
                const NProto::TDestroyHandleRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            Node.Clear();
        }
    };

    //
    // AcquireLock
    //

    struct TAcquireLock
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TAcquireLockRequest Request;

        TAcquireLock(
                TRequestInfoPtr requestInfo,
                const NProto::TAcquireLockRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
        }
    };

    //
    // ReleaseLock
    //

    struct TReleaseLock
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TReleaseLockRequest Request;

        std::optional<NProto::ELockOrigin> IncompatibleLockOrigin;

        TReleaseLock(
                TRequestInfoPtr requestInfo,
                const NProto::TReleaseLockRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
        {}

        void Clear() override
        {
            TErrorAware::Clear();

            IncompatibleLockOrigin.reset();
        }
    };

    //
    // TestLock
    //

    struct TTestLock
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TTestLockRequest Request;

        std::optional<TLockIncompatibleInfo> Incompatible;

        TTestLock(
                TRequestInfoPtr requestInfo,
                const NProto::TTestLockRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Request(request)
        {}

        void Clear() override
        {
            TErrorAware::Clear();

            Incompatible.reset();
        }
    };

    //
    // ReadData
    //

    struct TReadData
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TProfileAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 Handle;
        const TByteRange OriginByteRange;
        const TByteRange AlignedByteRange;
        /*const*/ IBlockBufferPtr Buffer;
        const bool DescribeOnly;
        // Used when we want to read data from a specific node, not the node
        // inferred from the handle.
        const ui64 ExplicitNodeId = InvalidNodeId;

        ui64 CommitId = InvalidCommitId;
        ui64 NodeId = InvalidNodeId;
        TMaybe<TByteRange> ReadAheadRange;
        TMaybe<IIndexTabletDatabase::TNode> Node;
        TVector<TBlockDataRef> Blocks;
        TVector<TBlockBytes> Bytes;

        // NOTE: should persist state across tx restarts
        TSet<ui32> MixedBlocksRanges;

        template <typename TReadRequest>
        TReadData(
                TRequestInfoPtr requestInfo,
                const TReadRequest& request,
                TByteRange originByteRange,
                TByteRange alignedByteRange,
                IBlockBufferPtr buffer,
                bool describeOnly,
                NProto::TProfileLogRequestInfo profileLogRequest)
            : TSessionAware(request)
            , TProfileAware(std::move(profileLogRequest))
            , RequestInfo(std::move(requestInfo))
            , Handle(request.GetHandle())
            , OriginByteRange(originByteRange)
            , AlignedByteRange(alignedByteRange)
            , Buffer(std::move(buffer))
            , DescribeOnly(describeOnly)
            , ExplicitNodeId(request.GetNodeId())
            , Blocks(AlignedByteRange.BlockCount())
            , Bytes(AlignedByteRange.BlockCount())
        {
            Y_DEBUG_ABORT_UNLESS(AlignedByteRange.IsAligned());
        }

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            NodeId = InvalidNodeId;
            ReadAheadRange.Clear();
            Node.Clear();

            std::fill(Blocks.begin(), Blocks.end(), TBlockDataRef());
            std::fill(Bytes.begin(), Bytes.end(), TBlockBytes());

            // deliberately not calling TProfileAware::Clear()
        }

        const TByteRange& ActualRange() const
        {
            return ReadAheadRange.GetOrElse(AlignedByteRange);
        }
    };

    //
    // WriteData
    //

    struct TWriteData
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TProfileAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui32 WriteBlobThreshold;
        const ui64 Handle;
        const TByteRange ByteRange;
        /*const*/ IBlockBufferPtr Buffer;
        // Used when we want to write data to a specific node, not the node
        // inferred from the handle.
        const ui64 ExplicitNodeId = InvalidNodeId;

        ui64 CommitId = InvalidCommitId;
        ui64 NodeId = InvalidNodeId;
        TMaybe<IIndexTabletDatabase::TNode> Node;

        TWriteData(
                TRequestInfoPtr requestInfo,
                const ui32 writeBlobThreshold,
                const NProto::TWriteDataRequest& request,
                TByteRange byteRange,
                IBlockBufferPtr buffer,
                NProto::TProfileLogRequestInfo profileLogRequest)
            : TSessionAware(request)
            , TProfileAware(std::move(profileLogRequest))
            , RequestInfo(std::move(requestInfo))
            , WriteBlobThreshold(writeBlobThreshold)
            , Handle(request.GetHandle())
            , ByteRange(byteRange)
            , Buffer(std::move(buffer))
            , ExplicitNodeId(request.GetNodeId())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            NodeId = InvalidNodeId;
            Node.Clear();

            // deliberately not calling TProfileAware::Clear()
        }

        bool ShouldWriteBlob() const
        {
            // skip fresh completely for large aligned writes
            return ByteRange.IsAligned()
                && ByteRange.Length >= WriteBlobThreshold;
        }
    };

    //
    // AddData
    //

    struct TAddData
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TProfileAware
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 Handle;
        const TByteRange ByteRange;
        TVector<NKikimr::TLogoBlobID> BlobIds;
        TVector<TBlockBytesMeta> UnalignedDataParts;
        ui64 CommitId;
        // Used when we want to access a specific node, not the node
        // inferred from the handle.
        const ui64 ExplicitNodeId = InvalidNodeId;

        ui64 NodeId = InvalidNodeId;
        TMaybe<IIndexTabletDatabase::TNode> Node;

        TAddData(
                TRequestInfoPtr requestInfo,
                const NProtoPrivate::TAddDataRequest& request,
                TByteRange byteRange,
                TVector<NKikimr::TLogoBlobID> blobIds,
                TVector<TBlockBytesMeta> unalignedDataParts,
                ui64 commitId,
                NProto::TProfileLogRequestInfo profileLogRequest)
            : TSessionAware(request)
            , TProfileAware(std::move(profileLogRequest))
            , RequestInfo(std::move(requestInfo))
            , Handle(request.GetHandle())
            , ByteRange(byteRange)
            , BlobIds(std::move(blobIds))
            , UnalignedDataParts(std::move(unalignedDataParts))
            , CommitId(commitId)
            , ExplicitNodeId(request.GetNodeId())
        {}

        void Clear() override
        {
            TErrorAware::Clear();

            NodeId = InvalidNodeId;
            Node.Clear();

            // deliberately not calling TProfileAware::Clear()
        }
    };

    //
    // WriteBatch
    //

    struct TWriteBatch
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const bool SkipFresh;
        /*const*/ TWriteRequestList WriteBatch;

        ui64 CommitId = InvalidCommitId;
        TMap<ui64, ui64> WriteRanges;
        TNodeSet Nodes;

        TWriteBatch(
                TRequestInfoPtr requestInfo,
                bool skipFresh,
                TWriteRequestList writeBatch)
            : RequestInfo(std::move(requestInfo))
            , SkipFresh(skipFresh)
            , WriteBatch(std::move(writeBatch))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            WriteRanges.clear();
            Nodes.clear();
        }
    };

    //
    // AllocateData
    //

    struct TAllocateData
        : TTxIndexTabletBase
        , TErrorAware
        , TSessionAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 Handle;
        const ui64 Offset;
        const ui64 Length;
        const ui32 Flags;

        ui64 CommitId = InvalidCommitId;
        ui64 NodeId = InvalidNodeId;
        TMaybe<IIndexTabletDatabase::TNode> Node;

        TAllocateData(
                TRequestInfoPtr requestInfo,
                const NProto::TAllocateDataRequest& request)
            : TSessionAware(request)
            , RequestInfo(std::move(requestInfo))
            , Handle(request.GetHandle())
            , Offset(request.GetOffset())
            , Length(request.GetLength())
            , Flags(request.GetFlags())
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            NodeId = InvalidNodeId;
            Node.Clear();
        }
    };

    //
    // AddBlob
    //

    struct TAddBlob
        : TTxIndexTabletBase
        , TErrorAware
        , TProfileAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const EAddBlobMode Mode;
        /*const*/ TVector<TMixedBlobMeta> SrcBlobs;
        /*const*/ TVector<TBlock> SrcBlocks;
        /*const*/ TVector<TMixedBlobMeta> MixedBlobs;
        /*const*/ TVector<TMergedBlobMeta> MergedBlobs;
        const TVector<TWriteRange> WriteRanges;
        const TVector<TBlockBytesMeta> UnalignedDataParts;

        ui64 CommitId = InvalidCommitId;
        TNodeSet Nodes;

        TAddBlob(
                TRequestInfoPtr requestInfo,
                EAddBlobMode mode,
                TVector<TMixedBlobMeta> srcBlobs,
                TVector<TBlock> srcBlocks,
                TVector<TMixedBlobMeta> mixedBlobs,
                TVector<TMergedBlobMeta> mergedBlobs,
                TVector<TWriteRange> writeRanges,
                TVector<TBlockBytesMeta> unalignedDataParts)
            : TProfileAware(EFileStoreSystemRequest::AddBlob)
            , RequestInfo(std::move(requestInfo))
            , Mode(mode)
            , SrcBlobs(std::move(srcBlobs))
            , SrcBlocks(std::move(srcBlocks))
            , MixedBlobs(std::move(mixedBlobs))
            , MergedBlobs(std::move(mergedBlobs))
            , WriteRanges(std::move(writeRanges))
            , UnalignedDataParts(std::move(unalignedDataParts))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TProfileAware::Clear();
            TIndexStateNodeUpdates::Clear();

            CommitId = InvalidCommitId;
            Nodes.clear();
        }
    };

    //
    // FlushBytes
    //

    struct TFlushBytes
        : TTxIndexTabletBase
        , TProfileAware
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 ReadCommitId;
        const ui64 ChunkId;
        const TVector<TBytes> Bytes;

        ui64 CommitId = InvalidCommitId;

        // NOTE: should persist state across tx restarts
        TSet<ui32> MixedBlocksRanges;

        TFlushBytes(
                TRequestInfoPtr requestInfo,
                ui64 readCommitId,
                ui64 chunkId,
                TVector<TBytes> bytes)
            : TProfileAware(EFileStoreSystemRequest::FlushBytes)
            , RequestInfo(std::move(requestInfo))
            , ReadCommitId(readCommitId)
            , ChunkId(chunkId)
            , Bytes(std::move(bytes))
        {}

        void Clear() override
        {
            TProfileAware::Clear();

            CommitId = InvalidCommitId;
        }
    };

    //
    // TrimBytes
    //

    struct TTrimBytes
        : TTxIndexTabletBase
        , TProfileAware
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 ChunkId;
        ui64 TrimmedBytes = 0;
        bool TrimmedAll = false;

        TTrimBytes(TRequestInfoPtr requestInfo, ui64 chunkId)
            : TProfileAware(EFileStoreSystemRequest::TrimBytes)
            , RequestInfo(std::move(requestInfo))
            , ChunkId(chunkId)
        {}

        void Clear() override
        {
            TProfileAware::Clear();

            TrimmedBytes = 0;
            TrimmedAll = false;
        }
    };

    //
    // Compaction
    //

    struct TCompaction
        : TTxIndexTabletBase
        , TProfileAware
    {
        const TRequestInfoPtr RequestInfo;
        const ui32 RangeId;
        const bool FilterNodes;

        // should persist across tx restarts
        bool RangeLoaded = false;

        TSet<ui64> Nodes;
        TVector<TMixedBlobMeta> CompactionBlobs;
        ui64 CommitId = InvalidCommitId;
        bool SkipRangeRewrite = false;

        TCompaction(TRequestInfoPtr requestInfo, ui32 rangeId, bool filterNodes)
            : TProfileAware(EFileStoreSystemRequest::Compaction)
            , RequestInfo(std::move(requestInfo))
            , RangeId(rangeId)
            , FilterNodes(filterNodes)
        {}

        void Clear() override
        {
            TProfileAware::Clear();

            Nodes.clear();
            CompactionBlobs.clear();
            CommitId = InvalidCommitId;
            SkipRangeRewrite = false;
        }
    };

    //
    // Cleanup
    //

    struct TCleanup
        : TTxIndexTabletBase
        , TProfileAware
    {
        const TRequestInfoPtr RequestInfo;
        const ui32 RangeId;
        const ui64 CollectBarrier;

        ui64 CommitId = InvalidCommitId;
        ui32 ProcessedDeletionMarkerCount = 0;

        TCleanup(TRequestInfoPtr requestInfo, ui32 rangeId, ui64 collectBarrier)
            : TProfileAware(EFileStoreSystemRequest::Cleanup)
            , RequestInfo(std::move(requestInfo))
            , RangeId(rangeId)
            , CollectBarrier(collectBarrier)
        {}

        void Clear() override
        {
            TProfileAware::Clear();

            CommitId = InvalidCommitId;
            ProcessedDeletionMarkerCount = 0;
        }
    };

    //
    // DeleteZeroCompactionRanges
    //

    struct TDeleteZeroCompactionRanges: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const ui32 StartIndex;

        TDeleteZeroCompactionRanges(
                TRequestInfoPtr requestInfo,
                ui32 startIndex)
            : RequestInfo(std::move(requestInfo))
            , StartIndex(startIndex)
        {}

        void Clear() override
        {
        }
    };

    //
    // WriteCompactionMap
    //

    struct TWriteCompactionMap: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const TVector<NProtoPrivate::TCompactionRangeStats> Ranges;

        TWriteCompactionMap(
                TRequestInfoPtr requestInfo,
                TVector<NProtoPrivate::TCompactionRangeStats> ranges)
            : RequestInfo(std::move(requestInfo))
            , Ranges(std::move(ranges))
        {}

        void Clear() override
        {
        }
    };

    //
    // DeleteGarbage
    //

    struct TDeleteGarbage
        : TTxIndexTabletBase
        , TProfileAware
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 CollectCommitId;

        TVector<TPartialBlobId> NewBlobs;
        TVector<TPartialBlobId> GarbageBlobs;
        TVector<TPartialBlobId> RemainingNewBlobs;
        TVector<TPartialBlobId> RemainingGarbageBlobs;

        TDeleteGarbage(
                TRequestInfoPtr requestInfo,
                ui64 collectCommitId,
                TVector<TPartialBlobId> newBlobs,
                TVector<TPartialBlobId> garbageBlobs)
            : TProfileAware(EFileStoreSystemRequest::DeleteGarbage)
            , RequestInfo(std::move(requestInfo))
            , CollectCommitId(collectCommitId)
            , NewBlobs(std::move(newBlobs))
            , GarbageBlobs(std::move(garbageBlobs))
        {}

        void Clear() override
        {
            TProfileAware::Clear();
        }
    };

    //
    // TruncateRange
    //

    struct TTruncateRange
        : TTxIndexTabletBase
        , TErrorAware
        , TProfileAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 NodeId;
        const TByteRange Range;

        TTruncateRange(
                TRequestInfoPtr requestInfo,
                ui64 nodeId,
                TByteRange range)
            : TProfileAware(EFileStoreSystemRequest::TruncateRange)
            , RequestInfo(std::move(requestInfo))
            , NodeId(nodeId)
            , Range(range)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TProfileAware::Clear();
            TIndexStateNodeUpdates::Clear();
        }
    };

    //
    // TruncateCompleted
    //

    struct TTruncateCompleted: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 NodeId;

        TTruncateCompleted(
                TRequestInfoPtr requestInfo,
                ui64 nodeId)
            : RequestInfo(std::move(requestInfo))
            , NodeId(nodeId)
        {}

        void Clear() override
        {
            // nothing to do
        }
    };

    //
    // ZeroRange
    //

    struct TZeroRange
        : TTxIndexTabletBase
        , TErrorAware
        , TProfileAware
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 NodeId;
        const TByteRange Range;

        ui64 CommitId = InvalidCommitId;

        TZeroRange(
                TRequestInfoPtr requestInfo,
                ui64 nodeId,
                TByteRange range)
            : TProfileAware(EFileStoreSystemRequest::ZeroRange)
            , RequestInfo(std::move(requestInfo))
            , NodeId(nodeId)
            , Range(range)
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TProfileAware::Clear();
        }
    };

    //
    // FilterAliveNodes
    //

    struct TFilterAliveNodes: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const TStackVec<ui64, 16> Nodes;

        ui64 CommitId = InvalidCommitId;
        TSet<ui64> AliveNodes;

        TFilterAliveNodes(
                TRequestInfoPtr requestInfo,
                TStackVec<ui64, 16> nodes)
            : RequestInfo(std::move(requestInfo))
            , Nodes(std::move(nodes))
        {}

        void Clear() override
        {
            CommitId = InvalidCommitId;
            AliveNodes.clear();
        }
    };

    //
    // DumpCompactionRange
    //

    struct TDumpCompactionRange: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;

        const ui32 RangeId = 0;
        TVector<TMixedBlobMeta> Blobs;

        TDumpCompactionRange(
                TRequestInfoPtr requestInfo,
                ui32 rangeId)
            : RequestInfo(std::move(requestInfo))
            , RangeId(rangeId)
        {}

        void Clear() override
        {
            Blobs.clear();
        }
    };

    //
    // ChangeStorageConfig
    //

    struct TChangeStorageConfig: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TStorageConfig StorageConfigNew;
        const bool MergeWithStorageConfigFromTabletDB;

        TMaybe<NProto::TStorageConfig> StorageConfigFromDB;
        NProto::TStorageConfig ResultStorageConfig;

        TChangeStorageConfig(
                TRequestInfoPtr requestInfo,
                NProto::TStorageConfig storageConfig,
                bool mergeWithStorageConfigFromTabletDB)
            : RequestInfo(std::move(requestInfo))
            , StorageConfigNew(std::move(storageConfig))
            , MergeWithStorageConfigFromTabletDB(
                mergeWithStorageConfigFromTabletDB)
        {}

        void Clear() override
        {
            StorageConfigFromDB.Clear();
            ResultStorageConfig.Clear();
        }
    };

    //
    // DeleteOpLogEntry
    //

    struct TDeleteOpLogEntry: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 EntryId;

        explicit TDeleteOpLogEntry(
                TRequestInfoPtr requestInfo,
                ui64 entryId)
            : RequestInfo(std::move(requestInfo))
            , EntryId(entryId)
        {}

        void Clear() override
        {
        }
    };

    //
    // GetOpLogEntry
    //

    struct TGetOpLogEntry: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 EntryId;
        TMaybe<NProto::TOpLogEntry> Entry;

        explicit TGetOpLogEntry(TRequestInfoPtr requestInfo, ui64 entryId)
            : RequestInfo(std::move(requestInfo))
            , EntryId(entryId)
        {}

        void Clear() override
        {
            Entry.Clear();
        }
    };

    //
    // WriteOpLogEntry
    //

    struct TWriteOpLogEntry: TTxIndexTabletBase, TErrorAware
    {
        const TRequestInfoPtr RequestInfo;
        NProto::TOpLogEntry Entry;

        explicit TWriteOpLogEntry(
                TRequestInfoPtr requestInfo,
                NProto::TOpLogEntry entry)
            : RequestInfo(std::move(requestInfo))
            , Entry(std::move(entry))
        {}

        void Clear() override
        {
            TErrorAware::Clear();

            Entry.ClearEntryId();
        }
    };

    //
    // CommitNodeCreationInShard
    //

    struct TCommitNodeCreationInShard: TTxIndexTabletBase
    {
        // actually unused, needed in tablet_tx.h to avoid sophisticated
        // template tricks
        const TRequestInfoPtr RequestInfo;
        const TString SessionId;
        const ui64 RequestId;
        NProto::TCreateNodeResponse Response;
        const ui64 EntryId;

        TCommitNodeCreationInShard(
                TString sessionId,
                ui64 requestId,
                NProto::TCreateNodeResponse response,
                ui64 entryId)
            : SessionId(std::move(sessionId))
            , RequestId(requestId)
            , Response(std::move(response))
            , EntryId(entryId)
        {}

        void Clear() override
        {
        }
    };

    //
    // UnsafeNodeOps / UnsafeNodeRefOps
    //

    struct TUnsafeDeleteNode
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TUnsafeDeleteNodeRequest Request;

        TMaybe<IIndexTabletDatabase::TNode> Node;

        TUnsafeDeleteNode(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TUnsafeDeleteNodeRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            Node.Clear();
        }
    };

    struct TUnsafeUpdateNode
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TUnsafeUpdateNodeRequest Request;

        TMaybe<IIndexTabletDatabase::TNode> Node;

        TUnsafeUpdateNode(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TUnsafeUpdateNodeRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();
            Node.Clear();
        }
    };

    struct TUnsafeGetNode
        : TTxIndexTabletBase
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TUnsafeGetNodeRequest Request;

        TMaybe<IIndexTabletDatabase::TNode> Node;

        TUnsafeGetNode(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TUnsafeGetNodeRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TIndexStateNodeUpdates::Clear();
            Node.Clear();
        }
    };

    struct TUnsafeCreateNodeRef
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TUnsafeCreateNodeRefRequest Request;

        TMaybe<IIndexTabletDatabase::TNodeRef> NodeRef;

        TUnsafeCreateNodeRef(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TUnsafeCreateNodeRefRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            NodeRef.Clear();
        }
    };

    struct TUnsafeDeleteNodeRef
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TUnsafeDeleteNodeRefRequest Request;

        TMaybe<IIndexTabletDatabase::TNodeRef> NodeRef;

        TUnsafeDeleteNodeRef(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TUnsafeDeleteNodeRefRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            NodeRef.Clear();
        }
    };

    struct TUnsafeUpdateNodeRef
        : TTxIndexTabletBase
        , TErrorAware
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TUnsafeUpdateNodeRefRequest Request;

        TMaybe<IIndexTabletDatabase::TNodeRef> NodeRef;

        TUnsafeUpdateNodeRef(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TUnsafeUpdateNodeRefRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TErrorAware::Clear();
            TIndexStateNodeUpdates::Clear();

            NodeRef.Clear();
        }
    };

    struct TUnsafeGetNodeRef
        : TTxIndexTabletBase
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TUnsafeGetNodeRefRequest Request;

        TMaybe<IIndexTabletDatabase::TNodeRef> NodeRef;

        TUnsafeGetNodeRef(
                TRequestInfoPtr requestInfo,
                NProtoPrivate::TUnsafeGetNodeRefRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {
            TIndexStateNodeUpdates::Clear();
            NodeRef.Clear();
        }
    };

    struct TUnsafeCreateHandle: TTxIndexTabletBase
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TUnsafeCreateHandleRequest Request;
        NProtoPrivate::TUnsafeCreateHandleResponse Response;

        NProto::TError Error;

        TUnsafeCreateHandle(
            TRequestInfoPtr requestInfo,
            NProtoPrivate::TUnsafeCreateHandleRequest request)
            : RequestInfo(std::move(requestInfo))
            , Request(std::move(request))
        {}

        void Clear() override
        {}
    };

    // The whole point of these transactions is to observe some data in NodeRefs
    // and Nodes tables and populate the contents of TIndexStateNodeUpdates with
    // it

    //
    // LoadNodeRefs
    //
    struct TLoadNodeRefs
        : TTxIndexTabletBase
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;

        const ui64 NodeId;
        const TString Cookie;
        const ui64 MaxNodeRefs;
        const TDuration SchedulePeriod;

        ui64 NextNodeId = 0;
        TString NextCookie;

        TLoadNodeRefs(
                TRequestInfoPtr requestInfo,
                ui64 nodeId,
                TString cookie,
                ui64 maxNodeRefs,
                TDuration schedulePeriod)
            : RequestInfo(std::move(requestInfo))
            , NodeId(nodeId)
            , Cookie(std::move(cookie))
            , MaxNodeRefs(maxNodeRefs)
            , SchedulePeriod(schedulePeriod)
        {}

        void Clear() override
        {
            TIndexStateNodeUpdates::Clear();

            NextNodeId = 0;
            NextCookie.clear();
        }
    };

    //
    // LoadNodes
    //

    struct TLoadNodes
        : TTxIndexTabletBase
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 NodeId;
        const ui64 MaxNodes;
        const TDuration SchedulePeriod;

        ui64 NextNodeId = 0;

        TLoadNodes(
                TRequestInfoPtr requestInfo,
                ui64 nodeId,
                ui64 maxNodes,
                TDuration schedulePeriod)
            : RequestInfo(std::move(requestInfo))
            , NodeId(nodeId)
            , MaxNodes(maxNodes)
            , SchedulePeriod(schedulePeriod)
        {}

        void Clear() override
        {
            TIndexStateNodeUpdates::Clear();
            NextNodeId = 0;
        }
    };

    //
    // ReadNodeRefs
    //

    struct TReadNodeRefs
        : TTxIndexTabletBase
        , TIndexStateNodeUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const NProtoPrivate::TReadNodeRefsRequest Request;
        const ui64 NodeId;
        const TString Cookie;
        const ui64 Limit;

        TVector<IIndexTabletDatabase::TNodeRef> Refs;
        ui64 NextNodeId = 0;
        TString NextCookie;

        TReadNodeRefs(
                TRequestInfoPtr requestInfo,
                const NProtoPrivate::TReadNodeRefsRequest& request)
            : RequestInfo(std::move(requestInfo))
            , Request(request)
            , NodeId(request.GetNodeId())
            , Cookie(request.GetCookie())
            , Limit(request.GetLimit())
            , Refs(Limit)
        {}

        void Clear() override
        {
            TIndexStateNodeUpdates::Clear();
            Refs.clear();
            NextNodeId = 0;
            NextCookie.clear();
        }
   };
};

}   // namespace NCloud::NFileStore::NStorage
