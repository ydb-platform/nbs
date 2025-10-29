#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/tablet_schema.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionSchema
    : public NKikimr::NIceDb::Schema
{
    enum EChannels
    {
        SystemChannel,
        LogChannel,
        IndexChannel,
        FirstDataChannel,
        MaxDataChannel = FirstDataChannel + MaxMergedChannelCount - 1,
    };

    struct Meta
        : public TTableSchema<1>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct PartitionMeta
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TPartitionMeta;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            PartitionMeta>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct FreshBlocksIndex
        : public TTableSchema<2>
    {
        struct BlockIndex
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct CommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlockContent
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;
        };

        using TKey = TableKey<BlockIndex, CommitId>;
        using TColumns = TableColumns<
            BlockIndex,
            CommitId,
            BlockContent
        >;

        using StoragePolicy = TStoragePolicy<
            LogChannel,
            NKikimr::NTable::NPage::ECache::Ever
        >;

        using Precharge = NoAutoPrecharge;
    };

    struct MixedBlocksIndex
        : public TTableSchema<3>
    {
        struct BlockIndex
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct CommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobCommitId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobOffset
            : public Column<5, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<BlockIndex, CommitId>;
        using TColumns = TableColumns<
            BlockIndex,
            CommitId,
            BlobCommitId,
            BlobId,
            BlobOffset
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
        using Precharge = NoAutoPrecharge;
    };

    struct MergedBlocksIndex
        : public TTableSchema<4>
    {
        struct RangeStart
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct RangeEnd
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct CommitId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        // Deprecated.
        struct HoleMask
            : public Column<5, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;    // THoleMask
        };

        struct SkipMask
            : public Column<6, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;    // TBlockMask
        };

        using TKey = TableKey<RangeEnd, CommitId>;
        using TColumns = TableColumns<
            RangeStart,
            RangeEnd,
            CommitId,
            BlobId,
            HoleMask,
            SkipMask
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
        using Precharge = NoAutoPrecharge;
    };

    struct BlobsIndex
        : public TTableSchema<5>
    {
        struct CommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobMeta
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TBlobMeta;
        };

        struct BlockMask
            : public Column<4, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;    // TBlockMask
        };

        using TKey = TableKey<CommitId, BlobId>;
        using TColumns = TableColumns<
            CommitId,
            BlobId,
            BlobMeta,
            BlockMask
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
        using Precharge = NoAutoPrecharge;
    };

    struct CompactionMap
        : public TTableSchema<6>
    {
        struct BlockIndex
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct BlobCount
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct BlockCount
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<BlockIndex>;
        using TColumns = TableColumns<
            BlockIndex,
            BlobCount,
            BlockCount>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct CleanupQueue
        : public TTableSchema<7>
    {
        struct DeletionCommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct CommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<DeletionCommitId, CommitId, BlobId>;
        using TColumns = TableColumns<
            DeletionCommitId,
            CommitId,
            BlobId
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct GarbageBlobs
        : public TTableSchema<8>
    {
        struct CommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<CommitId, BlobId>;
        using TColumns = TableColumns<
            CommitId,
            BlobId
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct Checkpoints
        : public TTableSchema<9>
    {
        struct CheckpointId
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct CommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct IdempotenceId
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct DateCreated
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct Stats
            : public Column<5, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TPartitionStats;
        };

        struct DataDeleted
            : public Column<6, NKikimr::NScheme::NTypeIds::Bool>
        {
        };

        using TKey = TableKey<CheckpointId>;
        using TColumns = TableColumns<
            CheckpointId,
            CommitId,
            IdempotenceId,
            DateCreated,
            Stats,
            DataDeleted
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct UsedBlocks
        : public TTableSchema<10>
    {
        struct RangeIndex
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct Bitmap
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;
        };

        using TKey = TableKey<RangeIndex>;
        using TColumns = TableColumns<
            RangeIndex,
            Bitmap>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct LogicalUsedBlocks
        : public TTableSchema<11>
    {
        struct RangeIndex
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct Bitmap
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;
        };

        using TKey = TableKey<RangeIndex>;
        using TColumns = TableColumns<
            RangeIndex,
            Bitmap>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    // merged blobs that are not confirmed yet
    struct UnconfirmedBlobs
        : public TTableSchema<12>
    {
        struct CommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct RangeStart
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct RangeEnd
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<CommitId, BlobId>;
        using TColumns = TableColumns<
            CommitId,
            BlobId,
            RangeStart,
            RangeEnd
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
        using Precharge = NoAutoPrecharge;
    };

    using TTables = SchemaTables<
        Meta,
        FreshBlocksIndex,
        MixedBlocksIndex,
        MergedBlocksIndex,
        BlobsIndex,
        CompactionMap,
        CleanupQueue,
        GarbageBlobs,
        Checkpoints,
        UsedBlocks,
        LogicalUsedBlocks,
        UnconfirmedBlobs
    >;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<0>
    >;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
