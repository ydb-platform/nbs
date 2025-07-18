#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

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

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    void ShouldMigrateDeviceImpl(bool rebootTablet)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, agents);

        const auto devices = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return TVector {
                msg.GetDevices(0).GetDeviceUUID(),
                msg.GetDevices(1).GetDeviceUUID()
            };
        }();

        auto getTarget = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.MigrationsSize());
            const auto& m = msg.GetMigrations(0);
            UNIT_ASSERT_VALUES_EQUAL(devices[0], m.GetSourceDeviceId());
            UNIT_ASSERT(!m.GetTargetDevice().GetDeviceUUID().empty());

            return m.GetTargetDevice().GetDeviceUUID();
        };

        diskRegistry.ChangeDeviceState(devices[0], NProto::DEVICE_STATE_WARNING);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        const auto target = getTarget();

        if (rebootTablet) {
            diskRegistry.RebootTablet();
            diskRegistry.WaitReady();

            RegisterAgents(*runtime, 2);
            WaitForAgents(*runtime, 2);
        }

        UNIT_ASSERT_VALUES_EQUAL(target, getTarget());

        diskRegistry.ChangeDeviceState(target, NProto::DEVICE_STATE_WARNING);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        const auto newTarget = getTarget();
        UNIT_ASSERT_VALUES_UNEQUAL(target, newTarget);

        {
            diskRegistry.SendFinishMigrationRequest("disk-1", devices[0], target);
            auto response = diskRegistry.RecvFinishMigrationResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        diskRegistry.FinishMigration("disk-1", devices[0], newTarget);

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(newTarget, msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }
    }

    Y_UNIT_TEST(ShouldMigrateDevice)
    {
        ShouldMigrateDeviceImpl(false);
    }

    Y_UNIT_TEST(ShouldMigrateDeviceAfterReboot)
    {
        ShouldMigrateDeviceImpl(true);
    }

    Y_UNIT_TEST(ShouldCancelMigrationAfterDiskDeallocation)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, agents);

        const auto devices = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return TVector {
                msg.GetDevices(0).GetDeviceUUID(),
                msg.GetDevices(1).GetDeviceUUID()
            };
        }();

        diskRegistry.ChangeDeviceState(devices[0], NProto::DEVICE_STATE_WARNING);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.MigrationsSize());
            const auto& m = msg.GetMigrations(0);
            UNIT_ASSERT_VALUES_EQUAL(devices[0], m.GetSourceDeviceId());
            UNIT_ASSERT(!m.GetTargetDevice().GetDeviceUUID().empty());
        }

        {
            diskRegistry.SendAllocateDiskRequest("disk-2", 20_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        diskRegistry.MarkDiskForCleanup("disk-1");
        diskRegistry.DeallocateDisk("disk-1");
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto response = diskRegistry.AllocateDisk("disk-2", 30_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(3, msg.DevicesSize());
            for (auto& d: msg.GetDevices()) {
                UNIT_ASSERT_VALUES_UNEQUAL(devices[0], d.GetDeviceUUID());
            }
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }
    }

    Y_UNIT_TEST(ShouldMigrateWithRespectToPlacementGroup)
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

        diskRegistry.UpdateConfig(CreateRegistryConfig({agents[0]}));

        RegisterAgent(*runtime, 0);
        WaitForAgent(*runtime, 0);
        WaitForSecureErase(*runtime, {agents[0]});

        diskRegistry.CreatePlacementGroup(
            "pg",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);

        auto device1 = [&] {
            auto response = diskRegistry.AllocateDisk(
                "disk-1", 10_GB, DefaultLogicalBlockSize, "pg");
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("rack-1", msg.GetDevices(0).GetRack());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return msg.GetDevices(0).GetDeviceUUID();
        }();

        {
            auto response = diskRegistry.AllocateDisk("dummy", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("rack-1", msg.GetDevices(0).GetRack());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }

        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(1, {agents[0], agents[1]}));

        RegisterAgent(*runtime, 1);
        WaitForAgent(*runtime, 1);
        WaitForSecureErase(*runtime, {agents[1]});

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-2", 10_GB, DefaultLogicalBlockSize, "pg");
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("rack-2", msg.GetDevices(0).GetRack());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }

        diskRegistry.UpdateConfig(CreateRegistryConfig(2, agents));
        RegisterAgent(*runtime, 2);
        WaitForAgent(*runtime, 2);
        WaitForSecureErase(*runtime, {agents[2]});

        diskRegistry.ChangeDeviceState(device1, NProto::DEVICE_STATE_WARNING);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        auto target1 = [&] {
            auto response = diskRegistry.AllocateDisk(
                "disk-1", 10_GB, DefaultLogicalBlockSize, "pg");
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(device1, msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.MigrationsSize());
            const auto& m = msg.GetMigrations(0);

            UNIT_ASSERT_VALUES_EQUAL(device1, m.GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL("rack-3", m.GetTargetDevice().GetRack());

            return m.GetTargetDevice().GetDeviceUUID();
        }();

        {
            diskRegistry.SendAllocateDiskRequest(
                "disk-3", 10_GB, DefaultLogicalBlockSize, "pg");
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        diskRegistry.FinishMigration("disk-1", device1, target1);

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1", 10_GB, DefaultLogicalBlockSize, "pg");
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(target1, msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }

        {
            diskRegistry.SendAllocateDiskRequest(
                "disk-3", 10_GB, DefaultLogicalBlockSize, "pg");
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        diskRegistry.MarkDiskForCleanup("dummy");
        diskRegistry.DeallocateDisk("dummy");
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-3", 10_GB, DefaultLogicalBlockSize, "pg");
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("rack-1", msg.GetDevices(0).GetRack());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }
    }

    Y_UNIT_TEST(ShouldCancelMigrationFromBrokenAgent)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        // register agent-1
        RegisterAgent(*runtime, 0);
        WaitForAgent(*runtime, 0);
        WaitForSecureErase(*runtime, {agents[0]});

        const auto devices = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return TVector {
                msg.GetDevices(0).GetDeviceUUID(),
                msg.GetDevices(1).GetDeviceUUID()
            };
        }();

        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", devices[1]);

        // register agent-2
        RegisterAgent(*runtime, 1);
        WaitForAgent(*runtime, 1);
        WaitForSecureErase(*runtime, {agents[1]});

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_WARNING);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.MigrationsSize());
            const auto& m1 = msg.GetMigrations(0);
            const auto& m2 = msg.GetMigrations(1);

            TVector sources {
                m1.GetSourceDeviceId(),
                m2.GetSourceDeviceId()
            };

            Sort(sources);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", sources[0]);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", sources[1]);

            TVector targets {
                m1.GetTargetDevice().GetDeviceUUID(),
                m2.GetTargetDevice().GetDeviceUUID()
            };

            Sort(targets);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", targets[0]);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", targets[1]);
        }

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, msg.GetIOMode());
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto response = diskRegistry.AllocateDisk("disk-2", 20_GB);
            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }
    }

    Y_UNIT_TEST(ShouldCancelMigrationFromBrokenDevice)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAndWaitForAgent(*runtime, 0, 2);

        auto allocateDisk = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            return msg;
        };

        const auto devices = [&] {
            auto msg = allocateDisk();
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return TVector {
                msg.GetDevices(0).GetDeviceUUID(),
                msg.GetDevices(1).GetDeviceUUID()
            };
        }();

        UNIT_ASSERT(devices[0].StartsWith("uuid-1."));
        UNIT_ASSERT(devices[1].StartsWith("uuid-1."));

        RegisterAndWaitForAgent(*runtime, 1, 2);

        // start migration from agent-1 to agent-2
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_WARNING);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        const auto targets = [&] {
            auto msg = allocateDisk();

            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, msg.MigrationsSize());
            SortBy(*msg.MutableMigrations(), [] (auto& x) {
                return x.GetSourceDeviceId();
            });

            const auto& m1 = msg.GetMigrations(0);
            UNIT_ASSERT_VALUES_EQUAL(devices[0], m1.GetSourceDeviceId());
            UNIT_ASSERT(!m1.GetTargetDevice().GetDeviceUUID().empty());
            const auto& m2 = msg.GetMigrations(1);
            UNIT_ASSERT_VALUES_EQUAL(devices[1], m2.GetSourceDeviceId());
            UNIT_ASSERT(!m2.GetTargetDevice().GetDeviceUUID().empty());

            return TVector {
                m1.GetTargetDevice().GetDeviceUUID(),
                m2.GetTargetDevice().GetDeviceUUID()
            };
        }();

        UNIT_ASSERT(targets[0].StartsWith("uuid-2."));
        UNIT_ASSERT(targets[1].StartsWith("uuid-2."));

        // cancel migration for uuid-1.1
        diskRegistry.ChangeDeviceState(devices[0], NProto::DEVICE_STATE_ERROR);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto msg = allocateDisk();

            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.MigrationsSize());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetMigrations(0).GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                targets[1],
                msg.GetMigrations(0).GetTargetDevice().GetDeviceUUID());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_ERROR_READ_ONLY, msg.GetIOMode());
        }

        {
            diskRegistry.SendFinishMigrationRequest("disk-1", devices[0], targets[0]);
            auto response = diskRegistry.RecvFinishMigrationResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            diskRegistry.SendFinishMigrationRequest("disk-1", devices[1], targets[1]);
            auto response = diskRegistry.RecvFinishMigrationResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        WaitForAgents(*runtime, agents.size());

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto msg = allocateDisk();

            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(targets[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_ERROR_READ_ONLY, msg.GetIOMode());
        }
    }

    Y_UNIT_TEST(ShouldHandleMultipleCmsRequests)
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

        auto addAgent = [&] (int i) {
            RegisterAgent(*runtime, i);
            WaitForAgent(*runtime, i);
            WaitForSecureErase(*runtime, {agents[i]});
        };

        addAgent(0);

        const auto devices = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return TVector {
                msg.GetDevices(0).GetDeviceUUID(),
                msg.GetDevices(1).GetDeviceUUID()
            };
        }();

        addAgent(1);
        addAgent(2);

        auto getTargets = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, msg.MigrationsSize());
            SortBy(*msg.MutableMigrations(), [] (auto& x) {
                return x.GetSourceDeviceId();
            });

            const auto& m1 = msg.GetMigrations(0);
            UNIT_ASSERT_VALUES_EQUAL(devices[0], m1.GetSourceDeviceId());
            UNIT_ASSERT(!m1.GetTargetDevice().GetDeviceUUID().empty());
            const auto& m2 = msg.GetMigrations(1);
            UNIT_ASSERT_VALUES_EQUAL(devices[1], m2.GetSourceDeviceId());
            UNIT_ASSERT(!m2.GetTargetDevice().GetDeviceUUID().empty());

            return TVector {
                m1.GetTargetDevice().GetDeviceUUID(),
                m2.GetTargetDevice().GetDeviceUUID()
            };
        };

        auto changeAgentState = [&] {
            diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_WARNING);
            runtime->AdvanceCurrentTime(TDuration::Seconds(20));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        };

        changeAgentState();

        auto targets = getTargets();
        UNIT_ASSERT_VALUES_EQUAL(2, targets.size());

        changeAgentState();

        const auto newTargets = getTargets();
        UNIT_ASSERT_VALUES_EQUAL(2, newTargets.size());
        UNIT_ASSERT_VALUES_EQUAL(targets[0], newTargets[0]);
        UNIT_ASSERT_VALUES_EQUAL(targets[1], newTargets[1]);
    }

    Y_UNIT_TEST(ShouldNotifyVolumeAboutNewMigration)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB),
            }),
            CreateAgentConfig("agent-3", {
                Device("dev-1", "uuid-3.1", "rack-3", 10_GB),
                Device("dev-3", "uuid-3.2", "rack-3", 10_GB),
            }),
            CreateAgentConfig("agent-4", {
                Device("dev-1", "uuid-4.1", "rack-4", 10_GB),
                Device("dev-4", "uuid-4.2", "rack-4", 10_GB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        size_t cleanDevices = 0;
        size_t reallocateCount = 0;

        runtime->SetObserverFunc( [&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskAgent::EvSecureEraseDeviceResponse: {
                    auto* msg = event->Get<TEvDiskAgent::TEvSecureEraseDeviceResponse>();
                    cleanDevices += !HasError(msg->GetError());
                    break;
                }
                case TEvVolume::EvReallocateDiskResponse: {
                    auto* msg = event->Get<TEvVolume::TEvReallocateDiskResponse>();
                    reallocateCount += !HasError(msg->GetError());
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        auto addAgent = [&] (int i) {
            const size_t prevCleanDevices = cleanDevices;
            const size_t deviceCount = agents[i].DevicesSize();

            RegisterAgent(*runtime, i);
            WaitForAgent(*runtime, i);

            runtime->AdvanceCurrentTime(TDuration::Seconds(5));

            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return cleanDevices - prevCleanDevices >= deviceCount;
            };
            runtime->DispatchEvents(options);
        };

        auto getDevices = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(4, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return TVector {
                msg.GetDevices(0).GetDeviceUUID(),
                msg.GetDevices(1).GetDeviceUUID(),
                msg.GetDevices(2).GetDeviceUUID(),
                msg.GetDevices(3).GetDeviceUUID()
            };
        };

        auto changeAgentState = [&] (const auto* name) {
            const size_t prevReallocateCount = reallocateCount;

            diskRegistry.ChangeAgentState(name, NProto::AGENT_STATE_WARNING);
            runtime->AdvanceCurrentTime(TDuration::Seconds(20));

            // wait for notification
            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return reallocateCount > prevReallocateCount;
            };
            runtime->DispatchEvents(options);
        };

        addAgent(0);
        addAgent(1);

        const auto devices = getDevices();

        auto getTargets = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(4, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(devices[0], msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[1], msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[2], msg.GetDevices(2).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(devices[3], msg.GetDevices(3).GetDeviceUUID());

            UNIT_ASSERT_GE(devices.size(), msg.MigrationsSize());

            SortBy(*msg.MutableMigrations(), [] (auto& x) {
                return x.GetSourceDeviceId();
            });

            TVector<TString> r;

            for (size_t i = 0; i != msg.MigrationsSize(); ++i) {
                auto& m = msg.GetMigrations(i);
                UNIT_ASSERT(!m.GetTargetDevice().GetDeviceUUID().empty());
                r.push_back(m.GetTargetDevice().GetDeviceUUID());
            }

            Sort(r);

            return r;
        };

        addAgent(2);
        changeAgentState("agent-1");

        const auto targets = getTargets();
        UNIT_ASSERT_VALUES_EQUAL(2, targets.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", targets[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.2", targets[1]);

        addAgent(3);
        changeAgentState("agent-2");

        const auto newTargets = getTargets();
        UNIT_ASSERT_VALUES_EQUAL(4, newTargets.size());

        UNIT_ASSERT_VALUES_EQUAL(targets[0], newTargets[0]);
        UNIT_ASSERT_VALUES_EQUAL(targets[1], newTargets[1]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-4.1", newTargets[2]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-4.2", newTargets[3]);
    }

    Y_UNIT_TEST(ShouldReleaseMigrationTargets)
    {
        const auto agentConfig = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", 10_GB),
            Device("dev-3", "uuid-1.3", "rack-1", 10_GB),
            Device("dev-4", "uuid-1.4", "rack-1", 10_GB),
        });

        THashSet<TString> acquiredDevices;

        auto agent = CreateTestDiskAgent(agentConfig);

        agent->HandleAcquireDevicesImpl = [&] (
            const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
            const TActorContext& ctx)
        {
            const auto& record = ev->Get()->Record;

            auto code = S_OK;
            for (const auto& uuid: record.GetDeviceUUIDs()) {
                if (acquiredDevices.contains(uuid)) {
                    code = E_BS_INVALID_SESSION;
                    break;
                }
            }

            if (code == S_OK) {
                for (const auto& uuid: record.GetDeviceUUIDs()) {
                    acquiredDevices.insert(uuid);
                }
            }

            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvDiskAgent::TEvAcquireDevicesResponse>(
                    MakeError(code)
                )
            );
            return true;
        };

        agent->HandleReleaseDevicesImpl = [&] (
            const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
            const TActorContext& ctx)
        {
            Y_UNUSED(ctx);

            const auto& record = ev->Get()->Record;
            for (const auto& uuid: record.GetDeviceUUIDs()) {
                acquiredDevices.erase(uuid);
            }
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig({ agentConfig }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, { agentConfig });

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 30_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(3, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            diskRegistry.ChangeDeviceState(
                msg.GetDevices(0).GetDeviceUUID(),
                NProto::DEVICE_STATE_WARNING);
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto response = diskRegistry.AcquireDisk("disk-1", "session-1");
            const auto& record = response->Record;
            UNIT_ASSERT_C(!HasError(record), record.GetError());
            UNIT_ASSERT_VALUES_EQUAL(4, record.DevicesSize());
        }

        UNIT_ASSERT_VALUES_EQUAL(4, acquiredDevices.size());

        diskRegistry.ReleaseDisk("disk-1", "session-1");

        UNIT_ASSERT_VALUES_EQUAL(0, acquiredDevices.size());

        {
            auto response = diskRegistry.AcquireDisk("disk-1", "session-2");
            const auto& record = response->Record;
            UNIT_ASSERT_C(!HasError(record), record.GetError());
            UNIT_ASSERT_VALUES_EQUAL(4, record.DevicesSize());
        }

        UNIT_ASSERT_VALUES_EQUAL(4, acquiredDevices.size());
    }

    Y_UNIT_TEST(ShouldCancelMigrationFromLostDevices)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB, 4_KB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB, 4_KB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB, 4_KB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB, 4_KB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, agents);

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            for (auto& d: msg.GetDevices()) {
                diskRegistry.ChangeDeviceState(d.GetDeviceUUID(), NProto::DEVICE_STATE_WARNING);
            }
            runtime->AdvanceCurrentTime(TDuration::Seconds(20));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        }

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(2, msg.MigrationsSize());
        }

        diskRegistry.RegisterAgent([] {
            auto config = CreateAgentConfig("agent-1", {});
            config.SetNodeId(42);
            return config;
        }());

        runtime->AdvanceCurrentTime(TDuration::MilliSeconds(500));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }
    }

    Y_UNIT_TEST(ShouldRestartMigrationInCaseOfBrokenTragets)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1.1", "uuid-1.1", "rack-1", 10_GB, 4_KB),
                Device("dev-1.2", "uuid-1.2", "rack-1", 10_GB, 4_KB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-2.1", "uuid-2.1", "rack-2", 10_GB, 4_KB),
                Device("dev-2.2", "uuid-2.2", "rack-2", 10_GB, 4_KB),
            }),
            CreateAgentConfig("agent-3", {
                Device("dev-3.1", "uuid-3.1", "rack-3", 10_GB, 4_KB),
                Device("dev-3.2", "uuid-3.2", "rack-3", 10_GB, 4_KB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        auto registerAndWaitForAgent = [&] (int index) {
            RegisterAndWaitForAgent(*runtime, index, agents[index].DevicesSize());
        };

        registerAndWaitForAgent(0);

        const auto sources = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            TVector<TString> devices;

            for (auto& d: msg.GetDevices()) {
                devices.push_back(d.GetDeviceUUID());
            }

            Sort(devices);

            return devices;
        }();

        UNIT_ASSERT_VALUES_EQUAL(2, sources.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", sources[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", sources[1]);

        registerAndWaitForAgent(1);

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_WARNING);
        for (auto& uuid: sources) {
            diskRegistry.SendChangeDeviceStateRequest(uuid, NProto::DEVICE_STATE_WARNING);
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        auto getTargets = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            TVector<TString> targets;
            targets.reserve(msg.MigrationsSize());

            for (const auto& m: msg.GetMigrations()) {
                targets.push_back(m.GetTargetDevice().GetDeviceUUID());
            }

            Sort(targets);

            return targets;
        };

        const auto targets = getTargets();
        UNIT_ASSERT_VALUES_EQUAL(2, targets.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", targets[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", targets[1]);

        registerAndWaitForAgent(2);

        diskRegistry.RegisterAgent([]{
            auto config = CreateAgentConfig("agent-2", {});
            config.SetNodeId(42);
            return config;
        }());
        runtime->AdvanceCurrentTime(TDuration::MilliSeconds(500));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        const auto newTargets = getTargets();
        UNIT_ASSERT_VALUES_EQUAL(2, targets.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", newTargets[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.2", newTargets[1]);
    }

    Y_UNIT_TEST(ShouldMigrateDiskWith64KBlockSize)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1.1", "uuid-1.1", "rack-1", 93_GB, 4_KB),
                Device("dev-1.2", "uuid-1.2", "rack-1", 93_GB, 4_KB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With([] {
                auto config = CreateDefaultStorageConfig();
                config.SetAllocationUnitNonReplicatedSSD(93);
                return config;
            }())
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAndWaitForAgents(*runtime, agents);

        const auto source = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 93_GB, 64_KB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            return msg.GetDevices(0).GetDeviceUUID();
        }();

        diskRegistry.ChangeDeviceState(source, NProto::DEVICE_STATE_WARNING);

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        auto response = diskRegistry.AllocateDisk("disk-1", 93_GB, 64_KB);

        const auto& msg = response->Record;
        UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, msg.MigrationsSize());

        const auto& m = msg.GetMigrations(0);
        const auto& target = m.GetTargetDevice();

        UNIT_ASSERT_VALUES_EQUAL(source, m.GetSourceDeviceId());

        UNIT_ASSERT_VALUES_EQUAL(64_KB, target.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(93_GB / 64_KB, target.GetBlocksCount());
    }

    Y_UNIT_TEST(ShouldAllowMigrationForDiskWithUnavailableState)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB, 4_KB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB, 4_KB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB, 4_KB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB, 4_KB),
            }),
            CreateAgentConfig("agent-3", {
                Device("dev-1", "uuid-3.1", "rack-3", 10_GB, 4_KB),
                Device("dev-2", "uuid-3.2", "rack-3", 10_GB, 4_KB),
            }),
            CreateAgentConfig("agent-4", {
                Device("dev-1", "uuid-4.1", "rack-4", 10_GB, 4_KB),
                Device("dev-2", "uuid-4.2", "rack-4", 10_GB, 4_KB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        // agent-1 & agent-2
        RegisterAndWaitForAgent(*runtime, 0, 2);
        RegisterAndWaitForAgent(*runtime, 1, 2);

        const auto sources = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return TVector<NProto::TDeviceConfig>{
                msg.GetDevices().begin(),
                msg.GetDevices().end()
            };
        }();

        // agent-3 & agent-4
        RegisterAndWaitForAgent(*runtime, 2, 2);
        RegisterAndWaitForAgent(*runtime, 3, 2);

        auto getMigrationsSize = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(4, msg.DevicesSize());

            return msg.MigrationsSize();
        };

        UNIT_ASSERT_VALUES_EQUAL(4, sources.size());

        diskRegistry.ChangeDeviceState(
            sources[0].GetDeviceUUID(),
            NProto::DEVICE_STATE_WARNING);

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(1, getMigrationsSize());

        const auto unavailableAgentId = [&] {
            auto* device = FindIfPtr(sources, [&] (const auto& d) {
                return d.GetNodeId() != sources[0].GetNodeId();
            });

            UNIT_ASSERT(device);
            return device->GetAgentId();
        }();

        diskRegistry.ChangeAgentState(
            unavailableAgentId,
            NProto::AGENT_STATE_UNAVAILABLE);

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(1, getMigrationsSize());

        for (auto& s: sources) {
            diskRegistry.ChangeDeviceState(
                s.GetDeviceUUID(),
                NProto::DEVICE_STATE_WARNING);
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(2, getMigrationsSize());

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        // register only alive agents
        for (size_t i = 0; i != agents.size(); ++i) {
            if (agents[i].GetAgentId() != unavailableAgentId) {
                RegisterAgent(*runtime, i);
            }
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(2, getMigrationsSize());
    }

    Y_UNIT_TEST(ShouldDeferReleaseMigrationDevices)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB, 4_KB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB, 4_KB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB, 4_KB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB, 4_KB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        // agent-1
        RegisterAndWaitForAgent(*runtime, 0, 2);

        const auto sources = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            return TVector<NProto::TDeviceConfig>{
                msg.GetDevices().begin(),
                msg.GetDevices().end()
            };
        }();

        // agent-2
        RegisterAndWaitForAgent(*runtime, 1, 2);

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_WARNING);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // migration in progress
        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.MigrationsSize());
        }

        ui32 cleanDevices = 0;
        TAutoPtr<IEventHandle> reallocRequest;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistryPrivate::EvSecureEraseResponse: {
                        auto* msg = event->Get<TEvDiskRegistryPrivate::TEvSecureEraseResponse>();
                        cleanDevices += msg->CleanDevices;
                        break;
                    }

                    case TEvVolume::EvReallocateDiskResponse: {
                        reallocRequest = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // cancel migration
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_ONLINE);
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, cleanDevices);

        // wait for realloc
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return reallocRequest != nullptr;
            };
            runtime->DispatchEvents(options);
        }

        // can't allocate new disk
        {
            diskRegistry.SendAllocateDiskRequest("disk-2", 20_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        runtime->Send(reallocRequest.Release());
        runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // wait for secure erase
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return cleanDevices == 2;
            };
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, cleanDevices);

        diskRegistry.AllocateDisk("disk-2", 20_GB);
    }

    Y_UNIT_TEST(ShouldForceMigrateDevice)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, agents);

        const auto devices = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());

            SortBy(*msg.MutableDevices(), TByUUID());

            return TVector {
                msg.GetDevices(0).GetDeviceUUID(),
                msg.GetDevices(1).GetDeviceUUID()
            };
        }();

        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", devices[1]);

        auto responseMigration = diskRegistry.StartForceMigration(
            "disk-1",
            "uuid-1.1",
            "uuid-2.1");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, responseMigration->GetStatus());

        diskRegistry.FinishMigration("disk-1", "uuid-1.1", "uuid-2.1");

        auto responseDescribe = diskRegistry.DescribeDisk("disk-1");

        auto& diskDevices = *responseDescribe->Record.MutableDevices();
        SortBy(diskDevices, TByUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            diskDevices[0].GetDeviceUUID(), "uuid-1.2");
        UNIT_ASSERT_VALUES_EQUAL(
            diskDevices[1].GetDeviceUUID(), "uuid-2.1");
    }

    Y_UNIT_TEST(ShouldSendEnableDeviceWhenSwithingOnline)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1.1", "uuid-1.1", "rack-1", 93_GB, 4_KB),
                Device("dev-1.2", "uuid-1.2", "rack-1", 93_GB, 4_KB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With([] {
                auto config = CreateDefaultStorageConfig();
                config.SetAllocationUnitNonReplicatedSSD(93);
                return config;
            }())
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAndWaitForAgents(*runtime, agents);

        TString enabledDeviceUUID;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskAgent::EvEnableAgentDeviceRequest: {
                        auto& msg = *event->Get<
                            TEvDiskAgent::TEvEnableAgentDeviceRequest>();
                        enabledDeviceUUID = msg.Record.GetDeviceUUID();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        diskRegistry.ChangeDeviceState("uuid-1.1", NProto::DEVICE_STATE_WARNING);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL("", enabledDeviceUUID);

        diskRegistry.ChangeDeviceState("uuid-1.1", NProto::DEVICE_STATE_ERROR);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL("", enabledDeviceUUID);

        diskRegistry.ChangeDeviceState("uuid-1.1", NProto::DEVICE_STATE_ONLINE);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", enabledDeviceUUID);
    }

    Y_UNIT_TEST(ShouldTrackMaxMigrationTime)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB),
                Device("dev-3", "uuid-1.3", "rack-1", 10_GB),
                Device("dev-4", "uuid-1.4", "rack-1", 10_GB),
                Device("dev-5", "uuid-1.5", "rack-1", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        auto maxMigrationTime = runtime->GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry")
            ->GetCounter("MaxMigrationTime");

        UNIT_ASSERT_VALUES_EQUAL(0, maxMigrationTime->Val());

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAndWaitForAgents(*runtime, agents);

        const TVector devices = [&] {
            auto response = diskRegistry.AllocateDisk("vol0", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.DevicesSize());

            return TVector {
                response->Record.GetDevices(0).GetDeviceUUID(),
                response->Record.GetDevices(1).GetDeviceUUID(),
                diskRegistry.AllocateDisk("vol1", 10_GB)
                    ->Record.GetDevices(0).GetDeviceUUID()
            };
        }();

        UNIT_ASSERT_VALUES_EQUAL(0, maxMigrationTime->Val());

        const auto migrationTs0 = runtime->GetCurrentTime();

        // start migration of the first device of vol0
        diskRegistry.ChangeDeviceState(devices[0], NProto::DEVICE_STATE_WARNING);

        runtime->AdvanceCurrentTime(15min);

        {
            const auto dt = runtime->GetCurrentTime() - migrationTs0;
            runtime->DispatchEvents({}, 10ms);  // wait for update

            const auto maxTime = TDuration::Seconds(maxMigrationTime->Val());

            UNIT_ASSERT_LT_C(TDuration::Zero(), maxTime, maxTime);
            UNIT_ASSERT_LE_C(maxTime, UpdateCountersInterval + dt, maxTime);
        }

        // start migration of the second device of vol0
        diskRegistry.ChangeDeviceState(devices[1], NProto::DEVICE_STATE_WARNING);

        runtime->AdvanceCurrentTime(20min);

        {
            const auto dt = runtime->GetCurrentTime() - migrationTs0;
            runtime->DispatchEvents({}, 10ms);  // wait for update

            const auto maxTime = TDuration::Seconds(maxMigrationTime->Val());

            UNIT_ASSERT_LT_C(TDuration::Zero(), maxTime, maxTime);
            UNIT_ASSERT_LE_C(maxTime, UpdateCountersInterval + dt, maxTime);
        }

        const auto migrationTs1 = runtime->GetCurrentTime();

        // start migration of vol1
        diskRegistry.ChangeDeviceState(devices[2], NProto::DEVICE_STATE_WARNING);

        runtime->AdvanceCurrentTime(20min);

        {
            const auto dt = runtime->GetCurrentTime() - migrationTs0;
            runtime->DispatchEvents({}, 10ms);  // wait for update

            const auto maxTime = TDuration::Seconds(maxMigrationTime->Val());

            UNIT_ASSERT_LT_C(TDuration::Zero(), maxTime, maxTime);
            UNIT_ASSERT_LE_C(maxTime, UpdateCountersInterval + dt, maxTime);
        }

        // first device of vol0 is online, but MaxMigrationTime is still
        // tracks vol0's migration (because of the second device).
        diskRegistry.ChangeDeviceState(devices[0], NProto::DEVICE_STATE_ONLINE);

        runtime->AdvanceCurrentTime(20min);

        {
            const auto dt = runtime->GetCurrentTime() - migrationTs0;
            runtime->DispatchEvents({}, 10ms);  // wait for update

            const auto maxTime = TDuration::Seconds(maxMigrationTime->Val());

            UNIT_ASSERT_LT_C(TDuration::Zero(), maxTime, maxTime);
            UNIT_ASSERT_LE_C(maxTime, UpdateCountersInterval + dt, maxTime);
        }

        // both vol0's devices are online so MaxMigrationTime is now tracks
        // migration of vol1
        diskRegistry.ChangeDeviceState(devices[1], NProto::DEVICE_STATE_ONLINE);
        runtime->AdvanceCurrentTime(15min);

        {
            const auto dt = runtime->GetCurrentTime() - migrationTs1;
            runtime->DispatchEvents({}, 10ms);  // wait for update

            const auto maxTime = TDuration::Seconds(maxMigrationTime->Val());

            UNIT_ASSERT_LT_C(TDuration::Zero(), maxTime, maxTime);
            UNIT_ASSERT_LE_C(maxTime, UpdateCountersInterval + dt, maxTime);
        }

        const TString targetId = [&] {
            auto response = diskRegistry.AllocateDisk("vol1", 10_GB);
            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.MigrationsSize());

            return msg.GetMigrations(0).GetTargetDevice().GetDeviceUUID();
        }();
        diskRegistry.FinishMigration("vol1", devices[2], targetId);

        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(0, maxMigrationTime->Val());

        runtime->AdvanceCurrentTime(8h);

        // start a new migration for vol1

        const auto migrationTs2 = runtime->GetCurrentTime();
        diskRegistry.ChangeDeviceState(targetId, NProto::DEVICE_STATE_WARNING);
        runtime->AdvanceCurrentTime(UpdateCountersInterval);

        {
            const auto dt = runtime->GetCurrentTime() - migrationTs2;
            runtime->DispatchEvents({}, 10ms);  // wait for update

            const auto maxTime = TDuration::Seconds(maxMigrationTime->Val());

            UNIT_ASSERT_LT_C(TDuration::Zero(), maxTime, maxTime);
            UNIT_ASSERT_LE_C(maxTime, UpdateCountersInterval + dt, maxTime);
        }
    }

    Y_UNIT_TEST(ShouldResetMigrationTime)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB),
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

        runtime->UpdateCurrentTime(TInstant::FromValue(1700000000000000));   // 2023-11-14T22:13:20
        runtime->DispatchEvents({}, 10ms);

        const auto uuid = diskRegistry.AllocateDisk("disk-1", 10_GB)
            ->Record.GetDevices(0).GetDeviceUUID();

        {
            auto response = diskRegistry.BackupDiskRegistryState(true);
            auto& backup = *response->Record.MutableBackup();
            UNIT_ASSERT_VALUES_EQUAL(1, backup.DisksSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", backup.GetDisks(0).GetDiskId());

            // set the migration start timestamp to a weird value
            backup.MutableDisks(0)->SetMigrationStartTs(42);

            diskRegistry.RestoreDiskRegistryState(
                std::move(backup),
                true    // force
            );

            diskRegistry.RebootTablet();
            diskRegistry.WaitReady();
            diskRegistry.SetWritableState(true);
            RegisterAgent(*runtime, 0);
        }

        const auto actualMigrationStartTs = runtime->GetCurrentTime();
        diskRegistry.ChangeDeviceState(uuid, NProto::DEVICE_STATE_WARNING);

        runtime->AdvanceCurrentTime(UpdateCountersInterval * 2);
        runtime->DispatchEvents({}, 10ms);

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 10_GB);
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.MigrationsSize());
        }

        const auto maxMigrationTime = runtime->GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry")
            ->GetCounter("MaxMigrationTime")
            ->Val();

        UNIT_ASSERT_LT(0, maxMigrationTime);
        UNIT_ASSERT_GE(
            runtime->GetCurrentTime() - actualMigrationStartTs,
            TDuration::MicroSeconds(maxMigrationTime)
        );
    }

    Y_UNIT_TEST(ShouldLimitSizeOfDeviceMigrationBatch)
    {
        const size_t agentWithDiskCount = 2;
        const size_t agentCount = 2 * agentWithDiskCount;
        const size_t devicesPerAgent = 128;
        const size_t devicesPerDisk = 32;
        const size_t disksPerAgent = devicesPerAgent / devicesPerDisk;
        const ui64 deviceSize = 93_GB;
        const ui64 diskSize = deviceSize * devicesPerDisk;
        const ui32 migrationsBatchSize = 8;

        TVector<NProto::TAgentConfig> agents;
        agents.reserve(agentCount);

        for (ui32 i = 0; i != agentCount; ++i) {
            auto& agent = agents.emplace_back(CreateAgentConfig(
                Sprintf("node-%04d.nbs.node.foo.bar.net", 1 + i),
                {}));

            auto& devices = *agent.MutableDevices();
            for (ui32 j = 0; j != devicesPerAgent; ++j) {
                auto device = Device(
                    Sprintf("/dev/disk/by-partlabel/NBSNVME%02d", j % 32),
                    Sprintf("uuid-%d-%d", i, j),
                    "rack",
                    deviceSize);
                device.SetSerialNumber("SERIAL_NUMBER_0123456789");
                devices.Add(std::move(device));
            }
        }

        auto config = CreateDefaultStorageConfig();
        config.SetAllocationUnitNonReplicatedSSD(93);
        config.SetMaxDevicesToErasePerDeviceNameForDefaultPoolKind(
            devicesPerAgent * agentCount);
        config.SetMaxNonReplicatedDeviceMigrationPercentageInProgress(100);
        config.SetMaxNonReplicatedDeviceMigrationBatchSize(migrationsBatchSize);

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With(config)
            .Build();

        // prepare Disk Registry

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAgents(*runtime, agents.size());
        WaitForAgents(*runtime, agents.size());

        runtime->AdvanceCurrentTime(15s);
        runtime->DispatchEvents({}, 10ms);

        runtime->AdvanceCurrentTime(20s);
        runtime->DispatchEvents(
            {.CustomFinalCondition = [&]
             {
                 auto response = diskRegistry.BackupDiskRegistryState(false);
                 return !response->Record.MutableBackup()->DirtyDevicesSize();
             }});

        // create disks

        for (ui32 i = 0; i != agentWithDiskCount; ++i) {
            const auto& agent = agents[i];
            for (ui32 j = 0; j != disksPerAgent; ++j) {
                auto request = diskRegistry.CreateAllocateDiskRequest(
                    Sprintf("disk-%d-%d", i, j),
                    diskSize);
                *request->Record.MutableAgentIds()->Add() = agent.GetAgentId();
                diskRegistry.SendRequest(std::move(request));
                auto response = diskRegistry.RecvAllocateDiskResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }

        // start migration

        TAutoPtr<IEventHandle> startMigrationRequest;

        runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvStartMigrationRequest)
                {
                    UNIT_ASSERT(!startMigrationRequest);
                    startMigrationRequest = event.Release();
                    return true;
                }

                return false;
            });

        // send a CMS request to remove the agents
        {
            TVector<NProto::TAction> requests;
            requests.reserve(agentWithDiskCount);

            for (ui32 i = 0; i != agentWithDiskCount; ++i) {
                auto& action = requests.emplace_back();
                action.SetHost(agents[i].GetAgentId());
                action.SetType(NProto::TAction::REMOVE_HOST);
            }

            diskRegistry.CmsAction(std::move(requests));
        }

        runtime->DispatchEvents({}, 10ms);

        ui32 startedMigrationCount = 0;
        runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvStartMigrationResponse)
                {
                    auto* msg = event->Get<
                        TEvDiskRegistryPrivate::TEvStartMigrationResponse>();
                    UNIT_ASSERT_VALUES_EQUAL(
                        migrationsBatchSize,
                        msg->StartedDeviceMigrationsCount);

                    startedMigrationCount += msg->StartedDeviceMigrationsCount;
                }
                return false;
            });

        runtime->Send(startMigrationRequest.Release());

        while (startedMigrationCount != agentWithDiskCount * devicesPerAgent) {
            runtime->AdvanceCurrentTime(15s);
            runtime->DispatchEvents({}, 10ms);
        }

        // check if all disks have a proper migration list

        for (ui32 i = 0; i != agentWithDiskCount; ++i) {
            for (ui32 j = 0; j != disksPerAgent; ++j) {
                auto response = diskRegistry.AllocateDisk(
                    Sprintf("disk-%d-%d", i, j),
                    diskSize);
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
                UNIT_ASSERT_VALUES_EQUAL(
                    devicesPerDisk,
                    response->Record.MigrationsSize());
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
