#pragma once

#include "public.h"

#include <cloud/filestore/config/storage.pb.h>
#include <cloud/filestore/libs/storage/core/tablet_schema.h>
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

#include <cloud/storage/core/protos/tablet.pb.h>

#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TIndexTabletSchema
    : public NKikimr::NIceDb::Schema
{
    enum EChannels
    {
        SystemChannel,
        IndexChannel,
        FreshChannel,
        DataChannel,
    };

    struct FileSystem: TTableSchema<1>
    {
        struct Id                   : Column<1, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct Proto                : ProtoColumn<2, NProto::TFileSystem> {};

        struct LastNodeId           : Column<3,  NKikimr::NScheme::NTypeIds::Uint64> {};
        struct LastLockId           : Column<4,  NKikimr::NScheme::NTypeIds::Uint64> {};
        struct LastCollectCommitId  : Column<5,  NKikimr::NScheme::NTypeIds::Uint64> {};

        struct UsedNodesCount       : Column<6, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct UsedSessionsCount    : Column<7, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct UsedHandlesCount     : Column<8, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct UsedLocksCount       : Column<9, NKikimr::NScheme::NTypeIds::Uint64> {};

        struct FreshBlocksCount     : Column<10, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MixedBlocksCount     : Column<11, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MixedBlobsCount      : Column<12, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct DeletionMarkersCount : Column<13, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct GarbageQueueSize     : Column<14, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct GarbageBlocksCount   : Column<15, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CheckpointNodesCount : Column<16, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CheckpointBlocksCount: Column<17, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CheckpointBlobsCount : Column<18, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct FreshBytesCount      : Column<19, NKikimr::NScheme::NTypeIds::Uint64> {};

        struct UsedBlocksCount      : Column<20, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct LastXAttr            : Column<21, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct AttrsUsedBytesCount  : Column<22, NKikimr::NScheme::NTypeIds::Uint64> {};

        struct StorageConfig        : ProtoColumn<23, NProto::TStorageConfig> {};

        struct DeletedFreshBytesCount
            : Column<24, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct LargeDeletionMarkersCount
            : Column<25, NKikimr::NScheme::NTypeIds::Uint64> {};

        struct HasXAttrs            : Column<26, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Id>;

        using TColumns = TableColumns<
            Id,
            Proto,
            LastNodeId,
            LastLockId,
            LastCollectCommitId,
            LastXAttr,
            UsedNodesCount,
            UsedSessionsCount,
            UsedHandlesCount,
            UsedLocksCount,
            FreshBlocksCount,
            MixedBlocksCount,
            MixedBlobsCount,
            DeletionMarkersCount,
            GarbageQueueSize,
            GarbageBlocksCount,
            CheckpointNodesCount,
            CheckpointBlocksCount,
            CheckpointBlobsCount,
            FreshBytesCount,
            UsedBlocksCount,
            AttrsUsedBytesCount,
            StorageConfig,
            DeletedFreshBytesCount,
            LargeDeletionMarkersCount,
            HasXAttrs
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct Sessions: TTableSchema<2>
    {
        struct ClientId     : Column<1, NKikimr::NScheme::NTypeIds::String> {};
        struct SessionId    : Column<2, NKikimr::NScheme::NTypeIds::String> {};
        struct Proto        : ProtoColumn<3, NProto::TSession> {};

        using TKey = TableKey<SessionId>;
        using TColumns = TableColumns<ClientId, SessionId, Proto>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct SessionHandles: TTableSchema<3>
    {
        struct SessionId    : Column<1, NKikimr::NScheme::NTypeIds::String> {};
        struct Handle       : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto        : ProtoColumn<3, NProto::TSessionHandle> {};

        using TKey = TableKey<SessionId, Handle>;
        using TColumns = TableColumns<SessionId, Handle, Proto>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct SessionLocks: TTableSchema<4>
    {
        struct SessionId    : Column<1, NKikimr::NScheme::NTypeIds::String> {};
        struct LockId       : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto        : ProtoColumn<3, NProto::TSessionLock> {};

        using TKey = TableKey<SessionId, LockId>;
        using TColumns = TableColumns<SessionId, LockId, Proto>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct Nodes: TTableSchema<5>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CommitId     : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto        : ProtoColumn<3, NProto::TNode> {};

        using TKey = TableKey<NodeId>;

        using TColumns = TableColumns<
            NodeId,
            CommitId,
            Proto
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct Nodes_Ver: TTableSchema<6>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MinCommitId  : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MaxCommitId  : Column<3, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto        : ProtoColumn<4, NProto::TNode> {};

        using TKey = TableKey<NodeId, MinCommitId>;

        using TColumns = TableColumns<
            NodeId,
            MinCommitId,
            MaxCommitId,
            Proto
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct NodeAttrs: TTableSchema<7>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CommitId     : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Name         : Column<3, NKikimr::NScheme::NTypeIds::String> {};
        struct Value        : Column<4, NKikimr::NScheme::NTypeIds::String> {};
        struct Version      : Column<5, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<NodeId, Name>;

        using TColumns = TableColumns<
            NodeId,
            CommitId,
            Name,
            Value,
            Version
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct NodeAttrs_Ver: TTableSchema<8>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MinCommitId  : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MaxCommitId  : Column<3, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Name         : Column<4, NKikimr::NScheme::NTypeIds::String> {};
        struct Value        : Column<5, NKikimr::NScheme::NTypeIds::String> {};
        struct Version      : Column<6, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<NodeId, Name, MinCommitId>;

        using TColumns = TableColumns<
            NodeId,
            MinCommitId,
            MaxCommitId,
            Name,
            Value,
            Version
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct NodeRefs: TTableSchema<9>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CommitId     : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Name         : Column<3, NKikimr::NScheme::NTypeIds::String> {};
        struct ChildId      : Column<4, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct ShardId      : Column<5, NKikimr::NScheme::NTypeIds::String> {};
        struct ShardNodeName: Column<6, NKikimr::NScheme::NTypeIds::String> {};

        using TKey = TableKey<NodeId, Name>;

        using TColumns = TableColumns<
            NodeId,
            CommitId,
            Name,
            ChildId,
            ShardId,
            ShardNodeName
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct NodeRefs_Ver: TTableSchema<10>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MinCommitId  : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MaxCommitId  : Column<3, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Name         : Column<4, NKikimr::NScheme::NTypeIds::String> {};
        struct ChildId      : Column<5, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct ShardId      : Column<6, NKikimr::NScheme::NTypeIds::String> {};
        struct ShardNodeName: Column<7, NKikimr::NScheme::NTypeIds::String> {};

        using TKey = TableKey<NodeId, Name, MinCommitId>;

        using TColumns = TableColumns<
            NodeId,
            MinCommitId,
            MaxCommitId,
            Name,
            ChildId,
            ShardId,
            ShardNodeName
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct FreshBlocks: TTableSchema<11>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct MinCommitId  : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        // XXX is MaxCommitId needed for FreshBlocks?
        struct MaxCommitId  : Column<3, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct BlockIndex   : Column<4, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct BlockData    : Column<5, NKikimr::NScheme::NTypeIds::String> {};

        using TKey = TableKey<NodeId, BlockIndex, MinCommitId>;

        using TColumns = TableColumns<
            NodeId,
            MinCommitId,
            MaxCommitId,
            BlockIndex,
            BlockData
        >;

        using StoragePolicy = TStoragePolicy<FreshChannel>;
    };

    struct MixedBlocks: TTableSchema<12>
    {
        struct RangeId                  : Column<1, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct BlobCommitId             : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct BlobUniqueId             : Column<3, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Blocks                   : Column<4, NKikimr::NScheme::NTypeIds::String> { using Type = TStringBuf; };
        struct DeletionMarkers          : Column<5, NKikimr::NScheme::NTypeIds::String> { using Type = TStringBuf; };
        struct GarbageBlocksCount       : Column<6, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct CheckpointBlocksCount    : Column<7, NKikimr::NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<RangeId, BlobCommitId, BlobUniqueId>;

        using TColumns = TableColumns<
            RangeId,
            BlobCommitId,
            BlobUniqueId,
            Blocks,
            DeletionMarkers,
            GarbageBlocksCount,
            CheckpointBlocksCount
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct DeletionMarkers: TTableSchema<13>
    {
        struct RangeId      : Column<1, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct NodeId       : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CommitId     : Column<3, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct BlockIndex   : Column<4, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct BlocksCount  : Column<5, NKikimr::NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<RangeId, NodeId, CommitId, BlockIndex>;

        using TColumns = TableColumns<
            RangeId,
            NodeId,
            CommitId,
            BlockIndex,
            BlocksCount
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct NewBlobs: TTableSchema<14>
    {
        struct BlobCommitId : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct BlobUniqueId : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<BlobCommitId, BlobUniqueId>;
        using TColumns = TableColumns<BlobCommitId, BlobUniqueId>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct GarbageBlobs: TTableSchema<15>
    {
        struct BlobCommitId : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct BlobUniqueId : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<BlobCommitId, BlobUniqueId>;
        using TColumns = TableColumns<BlobCommitId, BlobUniqueId>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct Checkpoints: TTableSchema<16>
    {
        struct CheckpointId : Column<1, NKikimr::NScheme::NTypeIds::String> {};
        struct Proto        : ProtoColumn<2, NProto::TCheckpoint> {};

        using TKey = TableKey<CheckpointId>;
        using TColumns = TableColumns<CheckpointId, Proto>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct CheckpointNodes: TTableSchema<17>
    {
        struct CheckpointId     : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct NodeId           : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<CheckpointId, NodeId>;
        using TColumns = TableColumns<CheckpointId, NodeId>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct CheckpointBlobs: TTableSchema<18>
    {
        struct CheckpointId : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct RangeId      : Column<2, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct BlobCommitId : Column<3, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct BlobUniqueId : Column<4, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<
            CheckpointId,
            RangeId,
            BlobCommitId,
            BlobUniqueId
        >;

        using TColumns = TableColumns<
            CheckpointId,
            RangeId,
            BlobCommitId,
            BlobUniqueId
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct FreshBytes: TTableSchema<19>
    {
        struct MinCommitId  : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct NodeId       : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Offset       : Column<3, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Data         : Column<4, NKikimr::NScheme::NTypeIds::String> {};
        struct Len          : Column<5, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<MinCommitId, NodeId, Offset>;

        using TColumns = TableColumns<
            MinCommitId,
            NodeId,
            Offset,
            Data,
            Len
        >;

        using StoragePolicy = TStoragePolicy<FreshChannel>;
    };

    struct CompactionMap: TTableSchema<20>
    {
        // TODO: migrate to a new table with Uint16 fields - 16 bits are enough
        // for BlobsCount, DeletionsCount and GarbageBlocksCount
        struct RangeId              : Column<1, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct BlobsCount           : Column<2, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct DeletionsCount       : Column<3, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct GarbageBlocksCount   : Column<4, NKikimr::NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<RangeId>;

        using TColumns = TableColumns<
            RangeId,
            BlobsCount,
            DeletionsCount,
            GarbageBlocksCount
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct SessionDupCache: TTableSchema<21>
    {
        struct SessionId    : Column<1, NKikimr::NScheme::NTypeIds::String> {};
        struct EntryId      : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto        : ProtoColumn<3, NProto::TDupCacheEntry> {};

        using TKey = TableKey<SessionId, EntryId>;

        using TColumns = TableColumns<
            SessionId,
            EntryId,
            Proto
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct TabletStorageInfo: TTableSchema<22>
    {
        struct Id       : Column<1, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct Proto    : ProtoColumn<2, NCloud::NProto::TTabletStorageInfo> {};

        using TKey = TableKey<Id>;

        using TColumns = TableColumns<
            Id,
            Proto
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct TruncateQueue: TTableSchema<23>
    {
        struct Id       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto    : ProtoColumn<2, NProto::TTruncateEntry> {};

        using TKey = TableKey<Id>;

        using TColumns = TableColumns<
            Id,
            Proto
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct SessionHistory: TTableSchema<24>
    {
        struct Id       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto    : ProtoColumn<2, NProto::TSessionHistoryEntry> {};

        using TKey = TableKey<Id>;

        using TColumns = TableColumns<
            Id,
            Proto
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct OpLog: TTableSchema<25>
    {
        struct Id       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct Proto    : ProtoColumn<2, NProto::TOpLogEntry> {};

        using TKey = TableKey<Id>;

        using TColumns = TableColumns<
            Id,
            Proto
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct LargeDeletionMarkers: TTableSchema<26>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct CommitId     : Column<2, NKikimr::NScheme::NTypeIds::Uint64> {};
        struct BlockIndex   : Column<3, NKikimr::NScheme::NTypeIds::Uint32> {};
        struct BlocksCount  : Column<4, NKikimr::NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<NodeId, CommitId, BlockIndex>;

        using TColumns = TableColumns<
            NodeId,
            CommitId,
            BlockIndex,
            BlocksCount
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct OrphanNodes: TTableSchema<27>
    {
        struct NodeId       : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<NodeId>;

        using TColumns = TableColumns<
            NodeId
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    using TTables = SchemaTables<
        FileSystem,
        Sessions,
        SessionHandles,
        SessionLocks,
        Nodes,
        Nodes_Ver,
        NodeAttrs,
        NodeAttrs_Ver,
        NodeRefs,
        NodeRefs_Ver,
        FreshBlocks,
        MixedBlocks,
        DeletionMarkers,
        NewBlobs,
        GarbageBlobs,
        Checkpoints,
        CheckpointNodes,
        CheckpointBlobs,
        FreshBytes,
        CompactionMap,
        SessionDupCache,
        TabletStorageInfo,
        TruncateQueue,
        SessionHistory,
        OpLog,
        LargeDeletionMarkers,
        OrphanNodes
    >;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<0>
    >;
};

}   // namespace NCloud::NFileStore::NStorage
