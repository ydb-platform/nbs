#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/tablet_schema.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <contrib/ydb/core/scheme/scheme_types_defs.h>
#include <contrib/ydb/core/tablet_flat/flat_cxx_database.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

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

    struct FreshBlocks
        : public TTableSchema<2>
    {
        struct BlockIndex
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct MinCommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct MaxCommitId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlockContent
            : public Column<4, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;
        };

        using TKey = TableKey<BlockIndex, MinCommitId>;
        using TColumns = TableColumns<
            BlockIndex,
            MinCommitId,
            MaxCommitId,
            BlockContent
        >;

        using StoragePolicy = TStoragePolicy<
            LogChannel,
            NKikimr::NTable::NPage::ECache::Ever
        >;
    };

    struct GlobalBlobs
        : public TTableSchema<3>
    {
        struct BlobCommitId
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
            using Type = NProto::TBlobMeta2;
        };

        using TKey = TableKey<BlobCommitId, BlobId>;
        using TColumns = TableColumns<
            BlobCommitId,
            BlobId,
            BlobMeta
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct BlockLists
        : public TTableSchema<4>
    {
        struct BlobCommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct Blocks
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;
        };

        struct DeletedBlocks
            : public Column<4, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;
        };

        using TKey = TableKey<BlobCommitId, BlobId>;
        using TColumns = TableColumns<
            BlobCommitId,
            BlobId,
            Blocks,
            DeletedBlocks
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct GlobalBlobUpdates
        : public TTableSchema<5>
    {
        struct DeletionId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct CommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct StartIndex
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct EndIndex
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<DeletionId>;
        using TColumns = TableColumns<
            DeletionId,
            CommitId,
            StartIndex,
            EndIndex
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct GlobalBlobGarbage
        : public TTableSchema<6>
    {
        struct BlobCommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlockCount
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<BlobCommitId, BlobId>;
        using TColumns = TableColumns<
            BlobCommitId,
            BlobId,
            BlockCount
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct Checkpoints
        : public TTableSchema<7>
    {
        struct CommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct CheckpointMeta
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TCheckpointMeta;
        };

        using TKey = TableKey<CommitId>;
        using TColumns = TableColumns<
            CommitId,
            CheckpointMeta
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct CheckpointBlobs
        : public TTableSchema<8>
    {
        struct CommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobCommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<CommitId, BlobCommitId, BlobId>;
        using TColumns = TableColumns<
            CommitId,
            BlobCommitId,
            BlobId
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct CompactionMap
        : public TTableSchema<9>
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
            BlockCount
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct GarbageBlobs
        : public TTableSchema<10>
    {
        struct BlobCommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<BlobCommitId, BlobId>;
        using TColumns = TableColumns<
            BlobCommitId,
            BlobId
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    struct ZoneBlobs
        : public TTableSchema<11>
    {
        struct ZoneId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct BlobCommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobMeta
            : public Column<4, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TBlobMeta2;
        };

        using TKey = TableKey<ZoneId, BlobCommitId, BlobId>;
        using TColumns = TableColumns<
            ZoneId,
            BlobCommitId,
            BlobId,
            BlobMeta
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct ZoneBlobUpdates
        : public TTableSchema<12>
    {
        struct ZoneId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct DeletionId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct CommitId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct StartIndex
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct EndIndex
            : public Column<5, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<ZoneId, DeletionId>;
        using TColumns = TableColumns<
            ZoneId,
            DeletionId,
            CommitId,
            StartIndex,
            EndIndex
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct ZoneBlobGarbage
        : public TTableSchema<13>
    {
        struct ZoneId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct BlobCommitId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlobId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct BlockCount
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<ZoneId, BlobCommitId, BlobId>;
        using TColumns = TableColumns<
            ZoneId,
            BlobCommitId,
            BlobId,
            BlockCount
        >;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    struct FreshBlockUpdates
        : public TTableSchema<14>
    {
        struct CommitId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct StartIndex
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct EndIndex
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<CommitId, StartIndex>;
        using TColumns = TableColumns<
            CommitId,
            StartIndex,
            EndIndex>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
        using CompactionPolicy = TCompactionPolicy<ECompactionPolicy::IndexTable>;
    };

    using TTables = SchemaTables<
        Meta,
        FreshBlocks,
        GlobalBlobs,
        BlockLists,
        GlobalBlobUpdates,
        GlobalBlobGarbage,
        Checkpoints,
        CheckpointBlobs,
        CompactionMap,
        GarbageBlobs,
        ZoneBlobs,
        ZoneBlobUpdates,
        ZoneBlobGarbage,
        FreshBlockUpdates
    >;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<0>
    >;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
