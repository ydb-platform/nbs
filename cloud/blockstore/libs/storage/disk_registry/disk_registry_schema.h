#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/tablet_schema.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/ydb/core/scheme/scheme_types_defs.h>
#include <contrib/ydb/core/tablet_flat/flat_cxx_database.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDiskRegistrySchema
    : public NKikimr::NIceDb::Schema
{
    /* obsolete
    struct Agents
        : public TTableSchema<1>
    {
        struct NodeId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct Config
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TAgentConfig;
        };

        using TKey = TableKey<NodeId>;
        using TColumns = TableColumns<NodeId, Config>;
    };
    */

    struct Disks
        : public TTableSchema<2>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct Config
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TDiskConfig;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Config>;
    };

    /* obsolete
    struct Sessions
        : public TTableSchema<3>
    {
        struct SessionId
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        struct Config
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TDiskSession;
        };

        using TKey = TableKey<SessionId>;
        using TColumns = TableColumns<SessionId, Config>;
    };
    */

    struct DiskRegistryConfig
        : public TTableSchema<4>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {};

        struct Config
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TDiskRegistryConfig;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Config>;
    };

    struct DirtyDevices
        : public TTableSchema<5>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        struct DiskId
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, DiskId>;
    };

    struct PlacementGroups
        : public TTableSchema<6>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct Config
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TPlacementGroupConfig;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Config>;
    };

    struct DiskStateChanges
        : public TTableSchema<7>
    {
        struct SeqNo
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct Id
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct State
            : public Column<3, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TDiskState;
        };

        using TKey = TableKey<SeqNo, Id>;
        using TColumns = TableColumns<SeqNo, Id, State>;
    };

    struct BrokenDisks
        : public TTableSchema<8>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        struct TsToDestroy
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, TsToDestroy>;
    };

    struct DisksToNotify
        : public TTableSchema<9>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id>;
    };

    struct DisksToCleanup
        : public TTableSchema<10>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id>;
    };

    // Obsolete. TODO: Remove legacy compatibility in next release
    struct ErrorNotifications
        : public TTableSchema<11>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id>;
    };

    struct OutdatedVolumeConfigs
        : public TTableSchema<12>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id>;
    };

    struct AgentById
        : public TTableSchema<13>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct Config
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TAgentConfig;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Config>;
    };

    struct SuspendedDevices
        : public TTableSchema<14>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        struct Config
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TSuspendedDevice;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Config>;
    };

    struct AutomaticallyReplacedDevices
        : public TTableSchema<16>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        struct ReplacementTs
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, ReplacementTs>;
    };

    /* deprecated
    struct NonreplMetricsCache
        : public TTableSchema<15>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {};

        struct ObjectId
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {};

        struct ValueFirst
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint64>
        {};

        struct ValueSecond
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint64>
        {};

        using TKey = TableKey<Id, ObjectId>;
        using TColumns = TableColumns<Id, ObjectId, ValueFirst, ValueSecond>;
    };
    */

    struct DiskRegistryAgentListParams
        : public TTableSchema<17>
    {
        struct AgentId
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct Params
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TDiskRegistryAgentParams;
        };

        using TKey = TableKey<AgentId>;
        using TColumns = TableColumns<AgentId, Params>;
    };

    struct UserNotifications
        : public TTableSchema<18>
    {
        struct SeqNo
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {};

        struct Notification
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TUserNotification;
        };

        using TKey = TableKey<SeqNo>;
        using TColumns = TableColumns<SeqNo, Notification>;
    };

    struct DisksWithRecentlyReplacedDevices
        : public TTableSchema<19>
    {
        struct MasterDiskId
            : public Column<1, NKikimr::NScheme::NTypeIds::String>
        {};

        struct ReplicaId
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {};

        using TKey = TableKey<MasterDiskId>;
        using TColumns = TableColumns<MasterDiskId, ReplicaId>;
    };

    using TTables = SchemaTables<
        Disks,
        DiskRegistryConfig,
        DirtyDevices,
        PlacementGroups,
        DiskStateChanges,
        BrokenDisks,
        DisksToNotify,
        DisksToCleanup,
        ErrorNotifications,
        OutdatedVolumeConfigs,
        AgentById,
        SuspendedDevices,
        AutomaticallyReplacedDevices,
        DiskRegistryAgentListParams,
        UserNotifications,
        DisksWithRecentlyReplacedDevices
    >;
};

}   // namespace NCloud::NBlockStore::NStorage
