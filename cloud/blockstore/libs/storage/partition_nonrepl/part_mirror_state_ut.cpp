#include "part_mirror_state.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TDevices Devices;
    TDevices ReplicaDevices;
    THashSet<TString> FreshDeviceIds;
    TMigrations Migrations;
    TNonreplicatedPartitionConfigPtr Config;

    void Init(TNonreplicatedPartitionConfig::TVolumeInfo volumeInfo)
    {
        {
            auto* device = Devices.Add();
            device->SetAgentId("1");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("1_1");
        }
        {
            auto* device = Devices.Add();
            device->SetAgentId("1");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("1_2");
        }
        {
            auto* device = Devices.Add();
            device->SetAgentId("2");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("2_1");
        }
        {
            auto* device = Devices.Add();
            device->SetAgentId("1");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("1_3");
        }

        TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfigInitParams
            params{
                Devices,
                volumeInfo,
                "vol0",
                DefaultBlockSize,
                NActors::TActorId()};
        params.FreshDeviceIds = FreshDeviceIds;
        Config =
            std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));

        {
            auto* device = ReplicaDevices.Add();
            device->SetAgentId("3");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("3_1");
        }
        {
            auto* device = ReplicaDevices.Add();
            device->SetAgentId("4");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("4_1");
        }
        {
            auto* device = ReplicaDevices.Add();
            device->SetAgentId("5");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("5_1");
        }
        {
            auto* device = ReplicaDevices.Add();
            device->SetBlocksCount(1024);
        }
    }

    void Init()
    {
        // only SSD/HDD distinction matters
        Init(
            {.CreationTs = Now(),
             .MediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR3,
             .EncryptionMode = NProto::NO_ENCRYPTION});
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMirrorPartitionStateTest)
{
#define TEST_READ_REPLICA(expected, state, startIndex, blockCount) {           \
        ui32 actorIndex;                                                       \
        const auto blockRange =                                                \
            TBlockRange64::WithLength(startIndex, blockCount);                 \
        const auto error = state.NextReadReplica(blockRange, actorIndex);      \
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage()); \
        NActors::TActorId actorId = state.GetReplicaActor(actorIndex);         \
        UNIT_ASSERT_VALUES_EQUAL(expected, actorId);                           \
}                                                                              \
// TEST_READ_REPLICA

    Y_UNIT_TEST(ShouldSelectProperReadReplicas)
    {
        TEnv env;
        env.FreshDeviceIds = {"1_2", "2_1", "3_1"};
        env.Init();

        TMirrorPartitionState state(
            std::make_shared<TStorageConfig>(
                NProto::TStorageServiceConfig(),
                nullptr),
            "xxx",      // rwClientId
            env.Config,
            env.Migrations,
            {env.ReplicaDevices}
        );

        NActors::TActorId actor1(1, "vasya");
        NActors::TActorId actor2(2, "petya");

        state.AddReplicaActor(actor1);
        state.AddReplicaActor(actor2);

        TEST_READ_REPLICA(actor1, state, 0, 1024);
        TEST_READ_REPLICA(actor1, state, 0, 1024);

        TEST_READ_REPLICA(actor2, state, 1024, 1024);
        TEST_READ_REPLICA(actor2, state, 1024, 1024);

        TEST_READ_REPLICA(actor2, state, 2048, 1024);
        TEST_READ_REPLICA(actor2, state, 2048, 1024);

        TEST_READ_REPLICA(actor1, state, 3072, 1024);
        TEST_READ_REPLICA(actor1, state, 3072, 1024);
    }

    Y_UNIT_TEST(ShouldBuildReplicationMigrationConfig)
    {
        using namespace NMonitoring;

        TEnv env;
        env.FreshDeviceIds = {"1_2", "2_1", "3_1"};
        env.Init();

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto freshDeviceNotFoundInConfig =
            counters->GetCounter("AppCriticalEvents/FreshDeviceNotFoundInConfig", true);

        TMirrorPartitionState state(
            std::make_shared<TStorageConfig>(
                NProto::TStorageServiceConfig(),
                nullptr),
            "xxx",      // rwClientId
            env.Config,
            env.Migrations,
            {env.ReplicaDevices}
        );

        UNIT_ASSERT_VALUES_EQUAL(0, freshDeviceNotFoundInConfig->Val());

        state.PrepareMigrationConfig();

        UNIT_ASSERT_VALUES_EQUAL(2, state.GetReplicaInfos().size());

        const auto& replica0 = state.GetReplicaInfos()[0];
        UNIT_ASSERT_VALUES_EQUAL(
            "1_1",
            replica0.Config->GetDevices()[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[0].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_2",
            replica0.Config->GetDevices()[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[1].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "2_1",
            replica0.Config->GetDevices()[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[2].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_3",
            replica0.Config->GetDevices()[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[3].GetBlocksCount());

        const auto& migrations0 = replica0.Migrations;
        UNIT_ASSERT_VALUES_EQUAL(1, migrations0.size());

        UNIT_ASSERT_VALUES_EQUAL(TString(), migrations0[0].GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_2",
            migrations0[0].GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            migrations0[0].GetTargetDevice().GetBlocksCount());

        const auto& replica1 = state.GetReplicaInfos()[1];
        UNIT_ASSERT_VALUES_EQUAL(
            "3_1",
            replica1.Config->GetDevices()[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[0].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "4_1",
            replica1.Config->GetDevices()[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[1].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "5_1",
            replica1.Config->GetDevices()[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[2].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            replica1.Config->GetDevices()[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[3].GetBlocksCount());

        const auto& migrations1 = replica1.Migrations;
        UNIT_ASSERT_VALUES_EQUAL(0, migrations1.size());
    }

    Y_UNIT_TEST(ShouldReplicateFreshDevicesFirst)
    {
        TEnv env;
        env.FreshDeviceIds = {"1_1"};
        {
            auto* m = env.Migrations.Add();
            m->SetSourceDeviceId("1_1");
            auto* device = m->MutableTargetDevice();
            device->SetAgentId("5");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("5_4");
        }
        {
            auto* m = env.Migrations.Add();
            m->SetSourceDeviceId("1_2");
            auto* device = m->MutableTargetDevice();
            device->SetAgentId("5");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("5_2");
        }
        {
            auto* m = env.Migrations.Add();
            m->SetSourceDeviceId("1_3");
            auto* device = m->MutableTargetDevice();
            device->SetAgentId("5");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("5_3");
        }
        env.Init();

        TMirrorPartitionState state(
            std::make_shared<TStorageConfig>(
                NProto::TStorageServiceConfig(),
                nullptr),
            "xxx",      // rwClientId
            env.Config,
            env.Migrations,
            {env.ReplicaDevices}
        );

        state.PrepareMigrationConfig();

        UNIT_ASSERT_VALUES_EQUAL(2, state.GetReplicaInfos().size());

        const auto& replica0 = state.GetReplicaInfos()[0];
        UNIT_ASSERT_VALUES_EQUAL(
            "1_1",
            replica0.Config->GetDevices()[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[0].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_2",
            replica0.Config->GetDevices()[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[1].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "2_1",
            replica0.Config->GetDevices()[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[2].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_3",
            replica0.Config->GetDevices()[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[3].GetBlocksCount());

        const auto& migrations0 = replica0.Migrations;
        UNIT_ASSERT_VALUES_EQUAL(1, migrations0.size());

        UNIT_ASSERT_VALUES_EQUAL(TString(), migrations0[0].GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_1",
            migrations0[0].GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            migrations0[0].GetTargetDevice().GetBlocksCount());

        const auto& replica1 = state.GetReplicaInfos()[1];
        UNIT_ASSERT_VALUES_EQUAL(
            "3_1",
            replica1.Config->GetDevices()[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[0].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "4_1",
            replica1.Config->GetDevices()[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[1].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "5_1",
            replica1.Config->GetDevices()[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[2].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            replica1.Config->GetDevices()[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[3].GetBlocksCount());

        const auto& migrations1 = replica1.Migrations;
        UNIT_ASSERT_VALUES_EQUAL(0, migrations1.size());
    }

    Y_UNIT_TEST(ShouldSkipExplicitMigrationsIfAllOfThemAreFromFreshDevices)
    {
        TEnv env;
        env.FreshDeviceIds = {"1_1"};
        {
            auto* m = env.Migrations.Add();
            m->SetSourceDeviceId("1_1");
            auto* device = m->MutableTargetDevice();
            device->SetAgentId("5");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("5_2");
        }
        env.Init();

        TMirrorPartitionState state(
            std::make_shared<TStorageConfig>(
                NProto::TStorageServiceConfig(),
                nullptr),
            "xxx",      // rwClientId
            env.Config,
            env.Migrations,
            {env.ReplicaDevices}
        );

        state.PrepareMigrationConfig();

        UNIT_ASSERT_VALUES_EQUAL(2, state.GetReplicaInfos().size());

        const auto& replica0 = state.GetReplicaInfos()[0];
        UNIT_ASSERT_VALUES_EQUAL(
            "1_1",
            replica0.Config->GetDevices()[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[0].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_2",
            replica0.Config->GetDevices()[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[1].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "2_1",
            replica0.Config->GetDevices()[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[2].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_3",
            replica0.Config->GetDevices()[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica0.Config->GetDevices()[3].GetBlocksCount());

        const auto& migrations0 = replica0.Migrations;
        UNIT_ASSERT_VALUES_EQUAL(1, migrations0.size());

        UNIT_ASSERT_VALUES_EQUAL(TString(), migrations0[0].GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "1_1",
            migrations0[0].GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            migrations0[0].GetTargetDevice().GetBlocksCount());

        const auto& replica1 = state.GetReplicaInfos()[1];
        UNIT_ASSERT_VALUES_EQUAL(
            "3_1",
            replica1.Config->GetDevices()[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[0].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "4_1",
            replica1.Config->GetDevices()[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[1].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "5_1",
            replica1.Config->GetDevices()[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[2].GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            replica1.Config->GetDevices()[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            replica1.Config->GetDevices()[3].GetBlocksCount());

        const auto& migrations1 = replica1.Migrations;
        UNIT_ASSERT_VALUES_EQUAL(0, migrations1.size());
    }

    Y_UNIT_TEST(ShouldPrepareMigrationConfigForSecondReplica)
    {
        using namespace NMonitoring;

        TEnv env;
        {
            auto* m = env.Migrations.Add();
            m->SetSourceDeviceId("3_1");
            auto* device = m->MutableTargetDevice();
            device->SetAgentId("5");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("5_2");
        }
        env.Init();

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto migrationSourceNotFound =
            counters->GetCounter("AppCriticalEvents/MigrationSourceNotFound", true);

        TMirrorPartitionState state(
            std::make_shared<TStorageConfig>(
                NProto::TStorageServiceConfig(),
                nullptr),
            "xxx",      // rwClientId
            env.Config,
            env.Migrations,
            {env.ReplicaDevices}
        );

        UNIT_ASSERT_VALUES_EQUAL(0, migrationSourceNotFound->Val());

        state.PrepareMigrationConfig();

        UNIT_ASSERT_VALUES_EQUAL(0, migrationSourceNotFound->Val());

        const auto& replica1 = state.GetReplicaInfos()[1];
        UNIT_ASSERT_VALUES_EQUAL(1, replica1.Migrations.size());

        const auto& m = replica1.Migrations[0];

        UNIT_ASSERT_VALUES_EQUAL("3_1", m.GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL("5_2", m.GetTargetDevice().GetDeviceUUID());
    }

    Y_UNIT_TEST(ShouldReportMigrationSourceNotFound)
    {
        using namespace NMonitoring;

        TEnv env;
        {
            auto* m = env.Migrations.Add();
            m->SetSourceDeviceId("nonexistent_device");
            auto* device = m->MutableTargetDevice();
            device->SetAgentId("5");
            device->SetBlocksCount(1024);
            device->SetDeviceUUID("5_2");
        }
        env.Init();

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto migrationSourceNotFound =
            counters->GetCounter("AppCriticalEvents/MigrationSourceNotFound", true);

        TMirrorPartitionState state(
            std::make_shared<TStorageConfig>(
                NProto::TStorageServiceConfig(),
                nullptr),
            "xxx",      // rwClientId
            env.Config,
            env.Migrations,
            {env.ReplicaDevices}
        );

        UNIT_ASSERT_VALUES_EQUAL(0, migrationSourceNotFound->Val());

        state.PrepareMigrationConfig();

        UNIT_ASSERT_VALUES_EQUAL(1, migrationSourceNotFound->Val());

        const auto& replica1 = state.GetReplicaInfos()[1];
        UNIT_ASSERT_VALUES_EQUAL(0, replica1.Migrations.size());
    }

    Y_UNIT_TEST(ShouldReportFreshDeviceNotFoundInConfig)
    {
        using namespace NMonitoring;

        TEnv env;
        env.FreshDeviceIds = {"nonexistent_device", "2_1", "3_1"};
        env.Init();

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto freshDeviceNotFoundInConfig = counters->GetCounter(
            "AppCriticalEvents/FreshDeviceNotFoundInConfig",
            true);

        TMirrorPartitionState state(
            std::make_shared<TStorageConfig>(
                NProto::TStorageServiceConfig(),
                nullptr),
            "xxx",      // rwClientId
            env.Config,
            env.Migrations,
            {env.ReplicaDevices}
        );

        UNIT_ASSERT_VALUES_EQUAL(1, freshDeviceNotFoundInConfig->Val());
    }

    // TODO: test config validation

#undef TEST_READ_REPLICA
}

}   // namespace NCloud::NBlockStore::NStorage
