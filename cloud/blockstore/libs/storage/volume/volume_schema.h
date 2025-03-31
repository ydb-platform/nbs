#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/tablet_schema.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>
#include <cloud/blockstore/config/storage.pb.h>

#include <contrib/ydb/core/scheme/scheme_types_defs.h>
#include <contrib/ydb/core/tablet_flat/flat_cxx_database.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeSchema
    : public NKikimr::NIceDb::Schema
{
    struct Meta
        : public TTableSchema<1>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct VolumeMeta
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TVolumeMeta;
        };

        struct StartPartitionsNeeded
            : public Column<3, NKikimr::NScheme::NTypeIds::Bool>
        {
        };

        struct StorageConfig
            : public Column<4, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TStorageServiceConfig;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id, VolumeMeta, StartPartitionsNeeded, StorageConfig>;
    };

    struct Clients
        : public TTableSchema<2>
    {
        struct ClientId
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct ClientInfo
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TVolumeClientInfo;
        };

        using TKey = TableKey<ClientId>;
        using TColumns = TableColumns<ClientId, ClientInfo>;
    };

    struct History
        : public TTableSchema<3>
    {
        struct Timestamp
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct SeqNo
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct OperationInfo
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TVolumeOperation;
        };

        using TKey = TableKey<Timestamp, SeqNo>;
        using TKeyColumns = TableColumns<Timestamp, SeqNo>;
        using TColumns = TableColumns<Timestamp, SeqNo, OperationInfo>;
    };

    struct PartStats
        : public TTableSchema<4>
    {
        struct PartTabletId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct Stats
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TCachedPartStats;
        };

        using TKey = TableKey<PartTabletId>;
        using TColumns = TableColumns<PartTabletId, Stats>;
    };

    struct CheckpointRequests
        : public TTableSchema<5>
    {
        struct RequestId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct CheckpointId
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct Timestamp
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct State
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct ReqType
            : public Column<5, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct CheckpointType
            : public Column<6, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct ShadowDiskId
            : public Column<7, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct ShadowDiskProcessedBlockCount
            : public Column<8, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct ShadowDiskState
            : public Column<9, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct CheckpointError
            : public Column<10, NKikimr::NScheme::NTypeIds::String>
        {
        };

        using TKey = TableKey<RequestId>;
        using TColumns = TableColumns<
            RequestId,
            CheckpointId,
            Timestamp,
            State,
            ReqType,
            CheckpointType,
            ShadowDiskId,
            ShadowDiskProcessedBlockCount,
            ShadowDiskState,
            CheckpointError>;
    };

    struct NonReplPartStats
        : public TTableSchema<6>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct Stats
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TCachedPartStats;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Stats>;
    };

    struct UsedBlocks
        : public TTableSchema<7>
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
    };

    struct ThrottlerState
        : public TTableSchema<8>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct Budget
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Budget>;
    };

    struct MetaHistory
        : public TTableSchema<9>
    {
        struct Version
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct Timestamp
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct VolumeMeta
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TVolumeMeta;
        };

        using TKey = TableKey<Version>;
        using TColumns = TableColumns<Version, Timestamp, VolumeMeta>;
    };

    struct VolumeParams
        : public TTableSchema<10>
    {
        struct Key
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct Value
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct ValidUntil
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value, ValidUntil>;
    };

    struct FollowerDisks: public TTableSchema<11>
    {
        struct Uuid: public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct CreatedAt
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };
        struct FollowerDiskId
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct ScaleUnitId: public Column<4, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct State: public Column<5, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct MigratedBytes
            : public Column<6, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct MediaKind
            : public Column<7, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct ErrorMessage
            : public Column<8, NKikimr::NScheme::NTypeIds::String>
        {
        };

        using TKey = TableKey<Uuid>;
        using TColumns = TableColumns<
            Uuid,
            CreatedAt,
            FollowerDiskId,
            ScaleUnitId,
            State,
            MigratedBytes,
            MediaKind,
            ErrorMessage>;
    };

    struct LeaderDisks: public TTableSchema<12>
    {
        struct Uuid: public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct CreatedAt
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };
        struct LeaderDiskId
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct ScaleUnitId: public Column<4, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct State: public Column<5, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct ErrorMessage
            : public Column<6, NKikimr::NScheme::NTypeIds::String>
        {
        };

        using TKey = TableKey<Uuid>;
        using TColumns = TableColumns<
            Uuid,
            CreatedAt,
            LeaderDiskId,
            ScaleUnitId,
            State,
            ErrorMessage>;
    };

    using TTables = SchemaTables<
        Meta,
        Clients,
        History,
        PartStats,
        CheckpointRequests,
        NonReplPartStats,
        UsedBlocks,
        ThrottlerState,
        MetaHistory,
        VolumeParams,
        FollowerDisks,
        LeaderDisks
    >;
};

}   // namespace NCloud::NBlockStore::NStorage
