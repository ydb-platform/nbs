#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>


#include <contrib/ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

#include <filesystem>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldWaitReady)
    {
        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
    }

    Y_UNIT_TEST(ShouldDescribeConfig)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig([&] {
            auto config = CreateRegistryConfig(0, {agent});

            auto* d = config.AddDeviceOverrides();
            d->SetDevice("foo");
            d->SetDiskId("bar");
            d->SetBlocksCount(42);

            auto* ssd = config.AddDevicePoolConfigs();
            ssd->SetName("ssd");
            ssd->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
            ssd->SetAllocationUnit(368_GB);

            return config;
        }());

        const auto response = diskRegistry.DescribeConfig();
        const auto& current = response->Record.GetConfig();

        UNIT_ASSERT_VALUES_EQUAL(1, current.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(1, current.KnownAgentsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, current.DeviceOverridesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, current.DevicePoolConfigsSize());

        const auto& knownAgent = current.GetKnownAgents(0);
        UNIT_ASSERT_VALUES_EQUAL(agent.GetAgentId(), knownAgent.GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), knownAgent.DevicesSize());

        const auto& d = current.GetDeviceOverrides(0);
        UNIT_ASSERT_VALUES_EQUAL("foo", d.GetDevice());
        UNIT_ASSERT_VALUES_EQUAL("bar", d.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(42, d.GetBlocksCount());

        const auto& ssd = current.GetDevicePoolConfigs(0);
        UNIT_ASSERT_VALUES_EQUAL("ssd", ssd.GetName());
        UNIT_ASSERT_EQUAL(NProto::DEVICE_POOL_KIND_LOCAL, ssd.GetKind());
        UNIT_ASSERT_VALUES_EQUAL(368_GB, ssd.GetAllocationUnit());
    }

    Y_UNIT_TEST(ShouldRejectConfigWithBadVersion)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-1", 10_GB),
            Device("dev-2", "uuid-4", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1}));

        {
            diskRegistry.SendUpdateConfigRequest(
                CreateRegistryConfig(0, {agent1, agent2}));
            auto response = diskRegistry.RecvUpdateConfigResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                response->GetStatus(),
                E_ABORTED
            );
        }

        {
            diskRegistry.SendUpdateConfigRequest(
                CreateRegistryConfig(10, {agent1, agent2}));
            auto response = diskRegistry.RecvUpdateConfigResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                response->GetStatus(),
                E_ABORTED
            );
        }

        const auto response = diskRegistry.DescribeConfig();
        const auto& current = response->Record.GetConfig();

        UNIT_ASSERT_VALUES_EQUAL(current.GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(current.KnownAgentsSize(), 1);
    }

    Y_UNIT_TEST(ShouldIgnoreConfigVersion)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-1", 10_GB),
            Device("dev-2", "uuid-4", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        const auto config1 = CreateRegistryConfig(0, {agent1});
        const auto config2 = CreateRegistryConfig(10, {agent1, agent2});

        diskRegistry.UpdateConfig(config1);
        diskRegistry.UpdateConfig(config2, true);
    }

    Y_UNIT_TEST(ShouldRegisterAgent)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("test", "uuid-1", "rack-1", 4_MB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1 })
            .Build();

        bool registerAgentSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvRegisterAgentRequest:
                        {
                            registerAgentSeen = true;

                            auto& msg = *event->Get<TEvDiskRegistry::TEvRegisterAgentRequest>();
                            UNIT_ASSERT_VALUES_EQUAL(msg.Record.GetAgentConfig().DevicesSize(), 1);

                            auto& disk = msg.Record.GetAgentConfig().GetDevices()[0];

                            UNIT_ASSERT_VALUES_EQUAL("test", disk.GetDeviceName());
                            UNIT_ASSERT_VALUES_EQUAL(
                                4_MB / DefaultBlockSize,
                                disk.GetBlocksCount());

                            UNIT_ASSERT_VALUES_EQUAL(
                                DefaultBlockSize,
                                disk.GetBlockSize());

                            UNIT_ASSERT(!disk.GetDeviceUUID().empty());
                            UNIT_ASSERT(!disk.GetTransportId().empty());
                            UNIT_ASSERT(!disk.GetRdmaEndpoint().GetHost().empty());
                        }
                    break;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);

        UNIT_ASSERT(registerAgentSeen);
    }

    Y_UNIT_TEST(ShouldUpdateAgentStats)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1 })
            .With([] {
                auto config = CreateDefaultStorageConfig();
                config.SetDiskRegistryCountersHost("test");

                return config;
            }())
            .Build();

        runtime->SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent1 }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);

        NProto::TAgentStats agentStats;

        agentStats.SetNodeId(runtime->GetNodeId(0));
        agentStats.SetInitErrorsCount(42);

        auto& device1 = *agentStats.AddDeviceStats();
        device1.SetDeviceUUID("uuid-1");
        device1.SetDeviceName("dev-1");
        device1.SetBytesRead(10000);
        device1.SetNumReadOps(100);
        device1.SetBytesWritten(3000000);
        device1.SetNumWriteOps(200);

        {
            auto& bucket = *device1.MutableHistogramBuckets()->Add();
            bucket.SetValue(TDuration::Seconds(1).MicroSeconds());
            bucket.SetCount(1);
        }

        {
            auto& bucket = *device1.MutableHistogramBuckets()->Add();
            bucket.SetValue(TDuration::Max().MicroSeconds());
            bucket.SetCount(1);
        }

        auto& device2 = *agentStats.AddDeviceStats();
        device2.SetDeviceUUID("uuid-2");
        device2.SetDeviceName("dev-2");
        device2.SetBytesRead(20000);
        device2.SetNumReadOps(200);
        device2.SetBytesWritten(6000000);
        device2.SetNumWriteOps(300);

        diskRegistry.UpdateAgentStats(std::move(agentStats));

        // wait for publish stats
        runtime->AdvanceCurrentTime(TDuration::Seconds(15));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        auto counters = runtime->GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry")
            ->GetSubgroup("host", "test");

        auto agentCounters = counters
            ->GetSubgroup("agent", "agent-1");

        auto totalInitErrors = agentCounters->GetCounter("Errors/Init");

        UNIT_ASSERT_VALUES_EQUAL(42, totalInitErrors->Val());

        auto totalReadCount = agentCounters->GetCounter("ReadCount");
        auto totalReadBytes = agentCounters->GetCounter("ReadBytes");
        auto totalWriteCount = agentCounters->GetCounter("WriteCount");
        auto totalWriteBytes = agentCounters->GetCounter("WriteBytes");

        UNIT_ASSERT_VALUES_EQUAL(300, totalReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(30000, totalReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(500, totalWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(9000000, totalWriteBytes->Val());

        {
            auto device = agentCounters->GetSubgroup("device", "agent-1:dev-1");

            UNIT_ASSERT_VALUES_EQUAL(100, device->GetCounter("ReadCount")->Val());
            UNIT_ASSERT_VALUES_EQUAL(10000, device->GetCounter("ReadBytes")->Val());
            UNIT_ASSERT_VALUES_EQUAL(200, device->GetCounter("WriteCount")->Val());
            UNIT_ASSERT_VALUES_EQUAL(3000000, device->GetCounter("WriteBytes")->Val());

            auto p100 = device
                ->GetSubgroup("percentiles", "Time")
                ->GetCounter("100");

            UNIT_ASSERT_VALUES_EQUAL(1'000'000, p100->Val());
        }

        {
            auto device = agentCounters->GetSubgroup("device", "agent-1:dev-2");

            UNIT_ASSERT_VALUES_EQUAL(200, device->GetCounter("ReadCount")->Val());
            UNIT_ASSERT_VALUES_EQUAL(20000, device->GetCounter("ReadBytes")->Val());
            UNIT_ASSERT_VALUES_EQUAL(300, device->GetCounter("WriteCount")->Val());
            UNIT_ASSERT_VALUES_EQUAL(6000000, device->GetCounter("WriteBytes")->Val());
        }
    }

    Y_UNIT_TEST(ShouldRejectDestructiveUpdateConfig)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-1", 10_GB),
            Device("dev-2", "uuid-4", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        const auto config1 = CreateRegistryConfig(0, {agent1, agent2});
        const auto config2 = CreateRegistryConfig(1, {agent1});

        diskRegistry.UpdateConfig(config1);

        RegisterAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        diskRegistry.AllocateDisk("disk-1", 40_GB, DefaultLogicalBlockSize);

        {
            diskRegistry.SendUpdateConfigRequest(config2);
            auto response = diskRegistry.RecvUpdateConfigResponse();

            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, response->GetStatus());

            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.AffectedDisksSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", response->Record.GetAffectedDisks(0));
        }
    }

    Y_UNIT_TEST(ShouldBackup)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB)
            }),
            CreateAgentConfig("agent-3", {
                Device("dev-1", "uuid-3.1", "rack-3", 10_GB),
                Device("dev-2", "uuid-3.2", "rack-3", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.CreatePlacementGroup(
            "pg1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);
        diskRegistry.CreatePlacementGroup(
            "pg2",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);
        diskRegistry.CreatePlacementGroup(
            "pg3",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);

        auto allocateDisk = [&] (TString id, TString groupName) {
            auto response =
                diskRegistry.AllocateDisk(id, 10_GB, 4_KB, groupName);

            return TVector<NProto::TDeviceConfig>(
                response->Record.GetDevices().begin(),
                response->Record.GetDevices().end()
            );
        };

        auto disk1 = allocateDisk("disk-1", "pg1");
        auto disk2 = allocateDisk("disk-2", "pg1");
        auto disk3 = allocateDisk("disk-3", "pg1");
        auto disk4 = allocateDisk("disk-4", "pg2");

        auto makeBackup = [&] (bool localDB) {
            NProto::TDiskRegistryStateBackup backup;
            auto response = diskRegistry.BackupDiskRegistryState(localDB);
            backup.Swap(response->Record.MutableBackup());

            return backup;
        };

        auto validate = [](auto& backup, const TString& name) {
            auto& config = *backup.MutableConfig();

            SortBy(*config.MutableKnownAgents(), [] (auto& x) { return x.GetAgentId(); });
            SortBy(*backup.MutableAgents(), [] (auto& x) { return x.GetAgentId(); });
            SortBy(*backup.MutablePlacementGroups(), [] (auto& x) { return x.GetGroupId(); });
            SortBy(*backup.MutableDisks(), [] (auto& x) { return x.GetDiskId(); });

            const auto& agents = backup.GetAgents();
            UNIT_ASSERT_VALUES_EQUAL_C(3, agents.size(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("agent-1", agents[0].GetAgentId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("agent-2", agents[1].GetAgentId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("agent-3", agents[2].GetAgentId(), name);

            const auto& disks = backup.GetDisks();
            UNIT_ASSERT_VALUES_EQUAL_C(4, disks.size(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("disk-1", disks[0].GetDiskId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("disk-2", disks[1].GetDiskId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("disk-3", disks[2].GetDiskId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("disk-4", disks[3].GetDiskId(), name);

            const auto& pg = backup.GetPlacementGroups();
            UNIT_ASSERT_VALUES_EQUAL_C(3, pg.size(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("pg1", pg[0].GetGroupId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("pg2", pg[1].GetGroupId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("pg3", pg[2].GetGroupId(), name);

            UNIT_ASSERT_VALUES_EQUAL_C(3, pg[0].DisksSize(), name);
            UNIT_ASSERT_VALUES_EQUAL_C(1, pg[1].DisksSize(), name);
            UNIT_ASSERT_VALUES_EQUAL_C(0, pg[2].DisksSize(), name);

            UNIT_ASSERT_VALUES_EQUAL_C("disk-1", pg[0].GetDisks(0).GetDiskId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("disk-2", pg[0].GetDisks(1).GetDiskId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("disk-3", pg[0].GetDisks(2).GetDiskId(), name);

            UNIT_ASSERT_VALUES_EQUAL_C("disk-4", pg[1].GetDisks(0).GetDiskId(), name);

            UNIT_ASSERT_VALUES_EQUAL_C(true, config.GetWritableState(), name);

            const auto& knownAgents = config.GetKnownAgents();
            UNIT_ASSERT_VALUES_EQUAL_C(3, knownAgents.size(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("agent-1", knownAgents[0].GetAgentId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("agent-2", knownAgents[1].GetAgentId(), name);
            UNIT_ASSERT_VALUES_EQUAL_C("agent-3", knownAgents[2].GetAgentId(), name);
        };

        auto backupState = makeBackup(false);
        auto backupDB = makeBackup(true);

        validate(backupState, "state backup");
        validate(backupDB, "local DB backup");
    }

    struct TScheduleBackup
        : public TCurrentTestCase
    {
        static const TString Path;

        void SetUp(NUnitTest::TTestContext& context) override
        {
            std::filesystem::create_directories(Path.data());
            TCurrentTestCase::SetUp(context);
        }

        void TearDown(NUnitTest::TTestContext& context) override
        {
            TCurrentTestCase::TearDown(context);
            std::filesystem::remove_all(Path.data());
        }
    };

    const TString TScheduleBackup::Path =
        GetWorkPath() + "/TTestCaseScheduleBackup";

    Y_UNIT_TEST_F(ShouldScheduleBackup, TScheduleBackup)
    {
        NProto::TStorageServiceConfig storageConfig =
            CreateDefaultStorageConfig();
        storageConfig.SetDiskRegistryBackupPeriod(
            TInstant::Minutes(2).MilliSeconds());
        storageConfig.SetDiskRegistryBackupDirPath(Path.data());

        auto runtime = TTestRuntimeBuilder()
            .With(storageConfig)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {}));

        runtime->AdvanceCurrentTime(TDuration::Minutes(3));
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        const int backupsFileCount =
            std::distance(
                std::filesystem::directory_iterator{Path.data()},
                std::filesystem::directory_iterator{});
        UNIT_ASSERT_VALUES_EQUAL(backupsFileCount, 1);
    }

    Y_UNIT_TEST_F(ShouldScheduleBackupAfterReset, TScheduleBackup)
    {
        NProto::TStorageServiceConfig storageConfig =
            CreateDefaultStorageConfig();
        storageConfig.SetDiskRegistryBackupPeriod(
            TInstant::Minutes(2).MilliSeconds());
        storageConfig.SetDiskRegistryBackupDirPath(Path.data());

        auto runtime = TTestRuntimeBuilder()
            .With(storageConfig)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();

        runtime->AdvanceCurrentTime(TDuration::Minutes(3));
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        const int backupsFileCount =
            std::distance(
                std::filesystem::directory_iterator{Path.data()},
                std::filesystem::directory_iterator{});
        UNIT_ASSERT_VALUES_EQUAL(backupsFileCount, 0);
    }

    Y_UNIT_TEST(ShouldErrorOnBackup)
    {
        NProto::TStorageServiceConfig storageConfig =
            CreateDefaultStorageConfig();
        storageConfig.SetDiskRegistryBackupPeriod(
            TInstant::Minutes(2).MilliSeconds());
        storageConfig.SetDiskRegistryBackupDirPath("/AnyRandomDir");

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto publishDiskStateError = counters->GetCounter(
            "AppCriticalEvents/DiskRegistryBackupFailed", true);

        auto runtime = TTestRuntimeBuilder()
            .With(storageConfig)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {}));

        runtime->AdvanceCurrentTime(TDuration::Minutes(3));
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, publishDiskStateError->Val());
    }

    Y_UNIT_TEST(ShouldUpdateVolumeConfig)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-2", 10_GB),
            Device("dev-2", "uuid-4", "rack-2", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        TSSProxyClient ssProxy(*runtime);

        ssProxy.CreateVolume("disk-1");
        ssProxy.CreateVolume("disk-2");
        ssProxy.CreateVolume("disk-3");

        diskRegistry.AllocateDisk("disk-1", 10_GB);
        diskRegistry.AllocateDisk("disk-2", 10_GB);
        diskRegistry.AllocateDisk("disk-3", 10_GB);

        ui32 updateCount = 0;

        runtime->SetObserverFunc([&] (auto& event) {
            switch (event->GetTypeRewrite()) {
            case TEvDiskRegistryPrivate::EvUpdateVolumeConfigRequest:
                updateCount++;
                break;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);
        diskRegistry.AlterPlacementGroupMembership(
            "group-1",
            1, // version
            TVector<TString>{"disk-1"},
            TVector<TString>());

        UNIT_ASSERT_VALUES_EQUAL(updateCount, 1);

        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            2, // version
            TVector<TString>{"disk-3"},
            TVector<TString>());
        auto response = diskRegistry.RecvAlterPlacementGroupMembershipResponse();

        UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(updateCount, 1);

        diskRegistry.AlterPlacementGroupMembership(
            "group-1",
            2, // version
            TVector<TString>{"disk-2"},
            TVector<TString>());

        UNIT_ASSERT_VALUES_EQUAL(updateCount, 2);

        diskRegistry.DestroyPlacementGroup("group-1");

        UNIT_ASSERT_VALUES_EQUAL(updateCount, 4);
    }

    Y_UNIT_TEST(ShouldRetryVolumeConfigUpdateAfterReboot)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({agent1})
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        diskRegistry.AllocateDisk("disk-1", 10_GB);
        diskRegistry.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);

        ui32 describeCount = 0;
        ui32 modifyCount = 0;
        ui32 finishCount = 0;

        runtime->SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);
            switch (event->GetTypeRewrite()) {
            case TEvSSProxy::EvDescribeVolumeRequest:
                describeCount++;
                break;

            case TEvSSProxy::EvModifySchemeRequest:
                modifyCount++;
                break;

            case TEvDiskRegistryPrivate::EvFinishVolumeConfigUpdateRequest:
                finishCount++;
                break;
            }

            return false;
        });

        // It will not be completed until ssProxy handles TEvModifySchemeRequest
        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            1, // version
            TVector<TString>{"disk-1"},
            TVector<TString>());

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(describeCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(modifyCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(finishCount, 0);

        TSSProxyClient ssProxy(*runtime);
        ssProxy.CreateVolume("disk-1");

        // Upon actor startup it re-sends VolumeConfigUpdate requests
        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(describeCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(modifyCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(finishCount, 1);
    }

    Y_UNIT_TEST(ShouldRetryVolumeConfigUpdateAfterTimeout)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
            });

        const int diskRegistryVolumeConfigUpdatePeriodMs = 1000;
        auto runtime = TTestRuntimeBuilder()
            .With([] {
                auto config = CreateDefaultStorageConfig();

                // disable timeout
                config.SetDiskRegistryVolumeConfigUpdatePeriod(
                    diskRegistryVolumeConfigUpdatePeriodMs);

                return config;
            }())
            .WithAgents({agent1})
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        diskRegistry.AllocateDisk("disk-1", 10_GB);

        TSSProxyClient ssProxy(*runtime);
        ssProxy.CreateVolume("disk-1");

        diskRegistry.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);

        // Expect intercept two EvModifySchemeRequest messages.
        // On first we reply with error. diskRegistry will retry it after
        // DiskRegistryVolumeConfigUpdatePeriod.
        ui32 modifyCount = 0;
        runtime->SetEventFilter([&](auto& runtime, auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvModifySchemeRequest:
                    std::unique_ptr<TEvSSProxy::TEvModifySchemeResponse>
                        response;
                    if (modifyCount == 0) {
                        response.reset(new TEvSSProxy::TEvModifySchemeResponse(
                            MakeError(E_REJECTED)));
                    } else {
                        response.reset(
                            new TEvSSProxy::TEvModifySchemeResponse());
                    }
                    runtime.Schedule(
                        new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release()),
                        TDuration::Seconds(0));
                    modifyCount++;
                    return true;
            }
            return false;
        });

        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            1, // version
            TVector<TString>{"disk-1"},
            TVector<TString>());
        runtime->DispatchEvents(
            {},
            TDuration::MilliSeconds(
                diskRegistryVolumeConfigUpdatePeriodMs + 100));
        auto response =
            diskRegistry.RecvAlterPlacementGroupMembershipResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(modifyCount, 2);
    }

    Y_UNIT_TEST(ShouldRespectVolumeConfigVersion)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({agent1})
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        diskRegistry.AllocateDisk("disk-1", 10_GB);

        TSSProxyClient ssProxy(*runtime);
        ssProxy.CreateVolume("disk-1");

        diskRegistry.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);

        TDuration responseDelay;
        ui64 finishCount = 0;

        runtime->SetEventFilter([&] (auto& runtime, auto& event) {
            bool handled = false;
            switch (event->GetTypeRewrite()) {
            case TEvSSProxy::EvModifySchemeRequest:
                runtime.Schedule(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        new TEvSSProxy::TEvModifySchemeResponse()),
                    responseDelay);
                handled = true;
                break;

            case TEvDiskRegistryPrivate::EvFinishVolumeConfigUpdateRequest:
                finishCount++;
                break;
            }

            return handled;
        });

        // set up ABA configuration

        responseDelay = TDuration::Seconds(1);
        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            1, // version
            TVector<TString>{"disk-1"},
            TVector<TString>());

        responseDelay = TDuration::Seconds(2);
        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            2, // version
            TVector<TString>{},
            TVector<TString>{"disk-1"});

        responseDelay = TDuration::Seconds(3);
        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            3, // version
            TVector<TString>{"disk-1"},
            TVector<TString>());

        // wait for the 1st and 2nd update

        runtime->AdvanceCurrentTime(TDuration::Seconds(2));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(finishCount, 0);

        // wait for the 3rd update

        responseDelay = TDuration::Seconds(0);
        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(finishCount, 1);
    }

    Y_UNIT_TEST(ShouldUpdateKnownDevicesAfterConfig)
    {
        const TVector agents { CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        })};

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);

        runtime->DispatchEvents({}, UpdateCountersInterval);

        auto diskRegistryGroup = runtime->GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        auto freeBytes = diskRegistryGroup
            ->GetSubgroup("pool", "default")
            ->GetSubgroup("kind", "default")
            ->GetCounter("FreeBytes");

        auto agentsInOnlineState = diskRegistryGroup
            ->GetCounter("AgentsInOnlineState");

        auto devicesInOnlineState = diskRegistryGroup
            ->GetCounter("DevicesInOnlineState");

        auto unknownDevices = diskRegistryGroup
            ->GetCounter("UnknownDevices");

        auto dirtyDevices = diskRegistryGroup
            ->GetCounter("DirtyDevices");

        UNIT_ASSERT_VALUES_EQUAL(0, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, unknownDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyDevices->Val());

        TAutoPtr<IEventHandle> secureEraseDeviceResponse;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvDiskAgent::EvSecureEraseDeviceResponse) {
                    secureEraseDeviceResponse = event.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        runtime->DispatchEvents({}, UpdateCountersInterval);

        UNIT_ASSERT_VALUES_EQUAL(0, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, unknownDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyDevices->Val());

        runtime->DispatchEvents({ .CustomFinalCondition = [&] {
            return !!secureEraseDeviceResponse;
        }});

        runtime->Send(secureEraseDeviceResponse.Release());

        runtime->DispatchEvents({ .FinalEvents = {
                TDispatchOptions::TFinalEventCondition(
                    TEvDiskRegistryPrivate::EvCleanupDevicesResponse)
            }
        });

        runtime->DispatchEvents({}, UpdateCountersInterval);

        UNIT_ASSERT_VALUES_EQUAL(10_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, unknownDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyDevices->Val());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
