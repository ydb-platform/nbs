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

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 LOCAL_DEVICE_SIZE = 99999997952;   // ~ 93.13 GiB

////////////////////////////////////////////////////////////////////////////////

auto GetBackup(TDiskRegistryClient& dr) -> NProto::TDiskRegistryStateBackup
{
    auto response = dr.BackupDiskRegistryState(false   // localDB
    );

    return response->Record.GetBackup();
}

auto GetDirtyDeviceCount(TDiskRegistryClient& dr)
{
    return GetBackup(dr).DirtyDevicesSize();
}

auto GetSuspendedDeviceCount(TDiskRegistryClient& dr)
{
    return GetBackup(dr).SuspendedDevicesSize();
}

auto MakeLocalDevice(const auto* name, const auto* uuid)
{
    return Device(name, uuid) |
           WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL) |
           WithTotalSize(LOCAL_DEVICE_SIZE);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldAllocateDisk)
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

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("dev-1", msg.GetDevices(0).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", msg.GetDevices(1).GetDeviceName());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlocksCount());

            UNIT_ASSERT(msg.GetDevices(0).GetNodeId() != 0);

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                msg.GetDevices(1).GetNodeId());
        }

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 30_GB);

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("dev-1", msg.GetDevices(0).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", msg.GetDevices(1).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", msg.GetDevices(2).GetDeviceName());

            UNIT_ASSERT(msg.GetDevices(0).GetNodeId() != 0);

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                msg.GetDevices(1).GetNodeId());

            // from different nodes
            UNIT_ASSERT_VALUES_UNEQUAL(
                msg.GetDevices(1).GetNodeId(),
                msg.GetDevices(2).GetNodeId());
        }
    }

    Y_UNIT_TEST(ShouldTakeDeviceOverridesIntoAccount)
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

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        TString uuid0;
        TString uuid1;

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("dev-1", msg.GetDevices(0).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", msg.GetDevices(1).GetDeviceName());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlocksCount());

            UNIT_ASSERT(msg.GetDevices(0).GetNodeId() != 0);

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                msg.GetDevices(1).GetNodeId());

            uuid0 = msg.GetDevices(0).GetDeviceUUID();
            uuid1 = msg.GetDevices(1).GetDeviceUUID();
        }

        TVector<NProto::TDeviceOverride> deviceOverrides;
        deviceOverrides.emplace_back();
        deviceOverrides.back().SetDiskId("disk-1");
        deviceOverrides.back().SetDevice(uuid0);
        deviceOverrides.back().SetBlocksCount(9_GB / DefaultBlockSize);
        deviceOverrides.emplace_back();
        deviceOverrides.back().SetDiskId("disk-1");
        deviceOverrides.back().SetDevice(uuid1);
        deviceOverrides.back().SetBlocksCount(9_GB / DefaultBlockSize);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(
                1,
                {agent1, agent2},
                deviceOverrides
            )
        );

        auto testRealloc = [&] {
            auto response = diskRegistry.AllocateDisk("disk-1", 28_GB);

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("dev-1", msg.GetDevices(0).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", msg.GetDevices(1).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", msg.GetDevices(2).GetDeviceName());

            UNIT_ASSERT(msg.GetDevices(0).GetNodeId() != 0);

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                msg.GetDevices(1).GetNodeId());

            // from different nodes
            UNIT_ASSERT_VALUES_UNEQUAL(
                msg.GetDevices(1).GetNodeId(),
                msg.GetDevices(2).GetNodeId());

            // overridden device sizes should take effect
            UNIT_ASSERT_VALUES_EQUAL(
                9_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                9_GB / DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(2).GetBlocksCount());
        };

        testRealloc();

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        testRealloc();
    }

    Y_UNIT_TEST(ShouldAllocateWithCorrectBlockSize)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB, 4_KB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB, 1_KB),
                Device("dev-3", "uuid-3", "rack-1", 10_GB, 4_KB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-4", "uuid-4", "rack-1", 10_GB, 8_KB),
                Device("dev-5", "uuid-5", "rack-1", 10_GB, 1_KB),
                Device("dev-6", "uuid-6", "rack-1", 10_GB, 8_KB)
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
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB, 1_KB);

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(msg.DevicesSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", msg.GetDevices(1).GetDeviceUUID());
        }

        {
            diskRegistry.SendAllocateDiskRequest("disk-2", 10_GB, 1_KB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        {
            auto response = diskRegistry.AllocateDisk("disk-3", 40_GB, 8_KB);

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(msg.DevicesSize(), 4);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", msg.GetDevices(2).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-6", msg.GetDevices(3).GetDeviceUUID());
        }
    }

    Y_UNIT_TEST(ShouldDescribeDisk)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
            Device("dev-3", "uuid-3", "rack-1", 10_GB)
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-4", "uuid-4", "rack-1", 10_GB),
            Device("dev-5", "uuid-5", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        diskRegistry.AllocateDisk("disk-1", 20_GB);
        diskRegistry.AllocateDisk("disk-2", 30_GB);

        {
            auto response = diskRegistry.DescribeDisk("disk-1");
            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(msg.DevicesSize(), 2);

            UNIT_ASSERT_VALUES_EQUAL(msg.GetDevices(0).GetDeviceName(), "dev-1");
            UNIT_ASSERT_VALUES_EQUAL(msg.GetDevices(1).GetDeviceName(), "dev-2");
        }

        {
            auto response = diskRegistry.DescribeDisk("disk-2");
            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(msg.DevicesSize(), 3);

            UNIT_ASSERT_VALUES_EQUAL(msg.GetDevices(0).GetDeviceName(), "dev-3");
            UNIT_ASSERT_VALUES_EQUAL(msg.GetDevices(1).GetDeviceName(), "dev-4");
            UNIT_ASSERT_VALUES_EQUAL(msg.GetDevices(2).GetDeviceName(), "dev-5");
        }
    }

    Y_UNIT_TEST(ShouldSupportPlacementGroups)
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

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        TSSProxyClient ssProxy(*runtime);
        ssProxy.CreateVolume("disk-1");
        ssProxy.CreateVolume("disk-2");
        ssProxy.CreateVolume("disk-3");
        ssProxy.CreateVolume("disk-4");

        diskRegistry.AllocateDisk("disk-1", 10_GB);
        diskRegistry.AllocateDisk("disk-2", 10_GB);
        diskRegistry.AllocateDisk("disk-3", 10_GB);
        diskRegistry.AllocateDisk("disk-4", 10_GB);

        diskRegistry.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0
        );
        diskRegistry.AlterPlacementGroupMembership(
            "group-1",
            1,
            TVector<TString>{"disk-1"},
            TVector<TString>()
        );

        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            2,
            TVector<TString>{"disk-3"},
            TVector<TString>()
        );

        {
            auto response = diskRegistry.RecvAlterPlacementGroupMembershipResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.DisksImpossibleToAddSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-3", response->Record.GetDisksImpossibleToAdd(0));
        }

        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            2,
            TVector<TString>{"disk-2"},
            TVector<TString>()
        );

        {
            auto response = diskRegistry.RecvAlterPlacementGroupMembershipResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = diskRegistry.ListPlacementGroups();
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GroupIdsSize());
            UNIT_ASSERT_VALUES_EQUAL("group-1", response->Record.GetGroupIds(0));
        }

        {
            auto response = diskRegistry.DescribePlacementGroup("group-1");
            const auto& group = response->Record.GetGroup();
            UNIT_ASSERT_VALUES_EQUAL("group-1", group.GetGroupId());
            UNIT_ASSERT_VALUES_EQUAL(3, group.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(2, group.DiskIdsSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", group.GetDiskIds(0));
            UNIT_ASSERT_VALUES_EQUAL("disk-2", group.GetDiskIds(1));
        }

        diskRegistry.DestroyPlacementGroup("group-1");

        {
            auto response = diskRegistry.ListPlacementGroups();
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GroupIdsSize());
        }
    }

    Y_UNIT_TEST(ShouldAllocateInPlacementGroup)
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

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        diskRegistry.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0
        );

        diskRegistry.AllocateDisk(
            "disk-1",
            10_GB,
            DefaultLogicalBlockSize,
            "group-1"
        );

        diskRegistry.AllocateDisk(
            "disk-2",
            10_GB,
            DefaultLogicalBlockSize,
            "group-1"
        );

        diskRegistry.SendAllocateDiskRequest(
            "disk-3",
            10_GB,
            DefaultLogicalBlockSize,
            "group-1"
        );

        {
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        diskRegistry.AllocateDisk("disk-3", 10_GB);
        diskRegistry.AllocateDisk("disk-4", 10_GB);

        diskRegistry.SendAllocateDiskRequest("disk-5", 10_GB);

        {
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        {
            auto response = diskRegistry.DescribePlacementGroup("group-1");
            const auto& group = response->Record.GetGroup();
            UNIT_ASSERT_VALUES_EQUAL("group-1", group.GetGroupId());
            UNIT_ASSERT_VALUES_EQUAL(3, group.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(2, group.DiskIdsSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", group.GetDiskIds(0));
            UNIT_ASSERT_VALUES_EQUAL("disk-2", group.GetDiskIds(1));
        }

        // TODO: test placement group persistence (call RebootTablet during test)
    }

    Y_UNIT_TEST(ShouldNotAllocateMoreThanMaxDisksInPlacementGroup)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2", "rack-2", 10_GB),
            }),
            CreateAgentConfig("agent-3", {
                Device("dev-1", "uuid-3", "rack-3", 10_GB),
            }),
            CreateAgentConfig("agent-4", {
                Device("dev-1", "uuid-4", "rack-4", 10_GB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*runtime, 4);
        WaitForAgents(*runtime, 4);
        WaitForSecureErase(*runtime, agents);

        diskRegistry.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0
        );

        diskRegistry.AllocateDisk(
            "disk-1",
            10_GB,
            DefaultLogicalBlockSize,
            "group-1"
        );

        diskRegistry.AllocateDisk(
            "disk-2",
            10_GB,
            DefaultLogicalBlockSize,
            "group-1"
        );

        diskRegistry.AllocateDisk(
            "disk-3",
            10_GB,
            DefaultLogicalBlockSize,
            "group-1"
        );

        TVector<TString> destroyedDiskIds;
        TAutoPtr<IEventHandle> destroyVolumeRequest;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvService::EvDestroyVolumeRequest
                        && event->Recipient == MakeStorageServiceId())
                {
                    auto* msg = event->Get<TEvService::TEvDestroyVolumeRequest>();
                    UNIT_ASSERT(msg->Record.GetDestroyIfBroken());
                    destroyedDiskIds.push_back(msg->Record.GetDiskId());
                    destroyVolumeRequest = event.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        diskRegistry.SendAllocateDiskRequest(
            "disk-4",
            10_GB,
            DefaultLogicalBlockSize,
            "group-1"
        );

        {
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_RESOURCE_EXHAUSTED, response->GetStatus());
        }

        {
            auto response = diskRegistry.ListBrokenDisks();
            UNIT_ASSERT_VALUES_EQUAL(1, response->DiskIds.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-4", response->DiskIds[0]);
        }

        auto wait = [&] (auto dt) {
            runtime->AdvanceCurrentTime(dt);
            runtime->DispatchEvents({}, 10ms);
        };

        wait(4s);
        UNIT_ASSERT(!destroyVolumeRequest);

        wait(1s);
        UNIT_ASSERT(destroyVolumeRequest);
        runtime->Send(destroyVolumeRequest.Release());

        wait(1s);
        UNIT_ASSERT_VALUES_EQUAL(1, destroyedDiskIds.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", destroyedDiskIds[0]);

        {
            auto response = diskRegistry.ListBrokenDisks();
            UNIT_ASSERT_VALUES_EQUAL(0, response->DiskIds.size());
        }

        {
            auto response = diskRegistry.DescribePlacementGroup("group-1");
            const auto& group = response->Record.GetGroup();
            UNIT_ASSERT_VALUES_EQUAL("group-1", group.GetGroupId());
            UNIT_ASSERT_VALUES_EQUAL(4, group.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(3, group.DiskIdsSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", group.GetDiskIds(0));
            UNIT_ASSERT_VALUES_EQUAL("disk-2", group.GetDiskIds(1));
            UNIT_ASSERT_VALUES_EQUAL("disk-3", group.GetDiskIds(2));
        }

        {
            NProto::TPlacementGroupSettings settings;
            settings.SetMaxDisksInGroup(4);
            diskRegistry.UpdatePlacementGroupSettings(
                "group-1",
                4,
                std::move(settings));
        }

        diskRegistry.SendAllocateDiskRequest(
            "disk-4",
            10_GB,
            DefaultLogicalBlockSize,
            "group-1"
        );

        {
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = diskRegistry.DescribePlacementGroup("group-1");
            const auto& group = response->Record.GetGroup();
            UNIT_ASSERT_VALUES_EQUAL("group-1", group.GetGroupId());
            UNIT_ASSERT_VALUES_EQUAL(6, group.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(4, group.DiskIdsSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", group.GetDiskIds(0));
            UNIT_ASSERT_VALUES_EQUAL("disk-2", group.GetDiskIds(1));
            UNIT_ASSERT_VALUES_EQUAL("disk-3", group.GetDiskIds(2));
            UNIT_ASSERT_VALUES_EQUAL("disk-4", group.GetDiskIds(3));
        }
    }

    Y_UNIT_TEST(ShouldPreventAllocation)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);

        auto tryAllocate = [&] (auto name) {
            diskRegistry.SendAllocateDiskRequest(name, 10_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            return response->GetStatus();
        };

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));
        diskRegistry.SetWritableState(false);

        {
            diskRegistry.SendRegisterAgentRequest(agents[0]);
            auto response = diskRegistry.RecvRegisterAgentResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_REJECTED);
        }

        diskRegistry.SetWritableState(true);
        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.SetWritableState(false);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, tryAllocate("disk-1"));

        diskRegistry.SetWritableState(true);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, tryAllocate("disk-1"));

        diskRegistry.SetWritableState(false);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, tryAllocate("disk-2"));

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, tryAllocate("disk-2"));
    }

    Y_UNIT_TEST(ShouldNotDeallocateUnmarkedDisk)
    {
        const TVector agents {CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        })};

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, agents);

        diskRegistry.AllocateDisk("disk-0", 10_GB);
        diskRegistry.AllocateDisk("disk-1", 10_GB);

        diskRegistry.MarkDiskForCleanup("disk-1");

        auto deallocate = [&] (auto name) {
            diskRegistry.SendDeallocateDiskRequest(name);
            return diskRegistry.RecvDeallocateDiskResponse()->GetStatus();
        };

        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, deallocate("disk-0"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, deallocate("disk-1"));

        diskRegistry.MarkDiskForCleanup("disk-0");

        UNIT_ASSERT_VALUES_EQUAL(S_OK, deallocate("disk-0"));

        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, deallocate("disk-0"));
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, deallocate("disk-1"));
    }

    Y_UNIT_TEST(ShouldAwaitSchemaResponseForAlterDisk)
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

        bool gotAlterPlacementGroupMembershipResponse = false;
        runtime->SetEventFilter([&] (auto& runtime, auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvModifySchemeRequest:
                    // Delay TEvModifySchemeResponse for 2 seconds.
                    runtime.Schedule(
                        new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            new TEvSSProxy::TEvModifySchemeResponse()),
                        2s);
                    return true;
                    break;
                case TEvService::EvAlterPlacementGroupMembershipResponse:
                    gotAlterPlacementGroupMembershipResponse = true;
                    break;
            }
            return false;
        });

        // The response of AlterPlacementGroupMembership will be delayed for 2 sec.
        // We won't get an answer in a second.
        diskRegistry.SendAlterPlacementGroupMembershipRequest(
            "group-1",
            1, // version
            TVector<TString>{"disk-1"},
            TVector<TString>());

        runtime->AdvanceCurrentTime(1s);
        UNIT_ASSERT_VALUES_EQUAL(gotAlterPlacementGroupMembershipResponse, false);

        // We should get response in two seconds.
        runtime->DispatchEvents({}, 2s);
        auto response =
            diskRegistry.RecvAlterPlacementGroupMembershipResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(gotAlterPlacementGroupMembershipResponse, true);
    }

    Y_UNIT_TEST(ShouldAwaitSchemaResponseForDestroyPlacementGroup)
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
        diskRegistry.AlterPlacementGroupMembership(
            "group-1",
            1, // version
            TVector<TString>{"disk-1"},
            TVector<TString>());

        bool gotDestroyPlacementGroupResponse = false;
        runtime->SetEventFilter([&] (auto& runtime, auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvModifySchemeRequest:
                    // Delay TEvModifySchemeResponse for 2 seconds.
                    runtime.Schedule(
                        new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            new TEvSSProxy::TEvModifySchemeResponse()),
                        2s);
                    return true;
                case TEvService::EvDestroyPlacementGroupResponse:
                    gotDestroyPlacementGroupResponse = true;
                    break;
            }
            return false;
        });

        // The response of DestroyPlacementGroup will be delayed for 2 sec.
        // We won't get an answer in a second.
        diskRegistry.SendDestroyPlacementGroupRequest("group-1");
        runtime->AdvanceCurrentTime(1s);
        UNIT_ASSERT_VALUES_EQUAL(gotDestroyPlacementGroupResponse, false);

        // We should get response in two seconds.
        runtime->DispatchEvents({}, 2s);
        auto response =
            diskRegistry.RecvDestroyPlacementGroupResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(gotDestroyPlacementGroupResponse, true);
    }

    Y_UNIT_TEST(ShouldDestroyEmptyPlacementGroup)
    {
        auto runtime = TTestRuntimeBuilder().Build();
        TDiskRegistryClient diskRegistry(*runtime);

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);

        diskRegistry.DestroyPlacementGroup("group-1");
    }

    Y_UNIT_TEST(ShouldAllocateMirroredDisk)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
            Device("dev-3", "uuid-3", "rack-1", 10_GB),
            // XXX needed to prioritize this agent in the allocation algorithm
            Device("dev-4", "uuid-extra1-1", "rack-1", 10_GB),
            Device("dev-5", "uuid-extra1-2", "rack-1", 10_GB),
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-4", "rack-2", 10_GB),
            Device("dev-2", "uuid-5", "rack-2", 10_GB),
            Device("dev-3", "uuid-6", "rack-2", 10_GB),
            // XXX needed to prioritize this agent in the allocation algorithm
            Device("dev-4", "uuid-extra2-1", "rack-2", 10_GB),
            Device("dev-5", "uuid-extra2-2", "rack-2", 10_GB),
        });

        const auto agent3 = CreateAgentConfig("agent-3", {
            Device("dev-1", "uuid-7", "rack-3", 10_GB),
            Device("dev-2", "uuid-8", "rack-3", 10_GB),
            Device("dev-3", "uuid-9", "rack-3", 10_GB),
        });

        const auto replaceFreezePeriod = TDuration::Days(1);

        auto config = CreateDefaultStorageConfig();
        config.SetAutomaticallyReplacedDevicesFreezePeriod(
            replaceFreezePeriod.MilliSeconds());

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2, agent3 })
            .With(std::move(config))
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, { agent1, agent2, agent3 }));

        RegisterAgents(*runtime, 3);
        WaitForAgents(*runtime, 3);
        // XXX this free-func approach is broken - we can only spot the events
        // that are sent after we call this func, but some secure erase events
        // can happen before that
        WaitForSecureErase(*runtime, { agent1, agent2, agent3 });
        // in our case we are only able to spot the last agent
        // WaitForSecureErase(*runtime, { agent3 });

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            SortBy(*msg.MutableReplicas(0)->MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_UNEQUAL(
                0,
                msg.GetDevices(0).GetNodeId());

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                msg.GetDevices(1).GetNodeId());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_UNEQUAL(
                0,
                msg.GetReplicas(0).GetDevices(0).GetNodeId());

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetReplicas(0).GetDevices(0).GetNodeId(),
                msg.GetReplicas(0).GetDevices(1).GetNodeId());
        }

        {
            diskRegistry.SendAllocateDiskRequest("disk-1", 30_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_INVALID_STATE,
                response->GetStatus());
        }

        {
            diskRegistry.SendAllocateDiskRequest(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                // XXX should be S_ALREADY,
                S_OK,
                response->GetStatus());
        }

        {
            diskRegistry.SendAllocateDiskRequest(
                "disk-1",
                30_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetStatus());

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                msg.GetDevices(2).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            SortBy(*msg.MutableReplicas(0)->MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetReplicas(0).GetDevices(2).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(2).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(2).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(2).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(2).GetBlocksCount());
        }

        TVector<NProto::TDeviceConfig> dirtyDevices;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() ==
                        TEvDiskRegistryPrivate::EvSecureEraseRequest)
                {
                    auto* msg =
                        event->Get<TEvDiskRegistryPrivate::TEvSecureEraseRequest>();
                    dirtyDevices.insert(
                        dirtyDevices.end(),
                        msg->DirtyDevices.begin(),
                        msg->DirtyDevices.end());
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // triggering device replacements
        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        // changing state back to online to allow erasing for this agent's devices
        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_ONLINE);

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                30_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-7",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-8",
                msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-9",
                msg.GetDevices(2).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.DeviceReplacementUUIDsSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-7",
                msg.GetDeviceReplacementUUIDs(0));
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-8",
                msg.GetDeviceReplacementUUIDs(1));
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-9",
                msg.GetDeviceReplacementUUIDs(2));

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            SortBy(*msg.MutableReplicas(0)->MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetReplicas(0).GetDevices(2).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(2).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(2).GetBlocksCount());

            UNIT_ASSERT_VALUES_UNEQUAL(
                0,
                msg.GetDevices(0).GetNodeId());

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                msg.GetDevices(1).GetNodeId());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(2).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(2).GetBlocksCount());

            UNIT_ASSERT_VALUES_UNEQUAL(
                0,
                msg.GetReplicas(0).GetDevices(0).GetNodeId());

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetReplicas(0).GetDevices(0).GetNodeId(),
                msg.GetReplicas(0).GetDevices(1).GetNodeId());
        }

        {
            auto response = diskRegistry.DescribeDisk("disk-1");

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-7",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-8",
                msg.GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-9",
                msg.GetDevices(2).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.DeviceReplacementUUIDsSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-7",
                msg.GetDeviceReplacementUUIDs(0));
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-8",
                msg.GetDeviceReplacementUUIDs(1));
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-9",
                msg.GetDeviceReplacementUUIDs(2));

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            SortBy(*msg.MutableReplicas(0)->MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetReplicas(0).GetDevices(2).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(2).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(2).GetBlocksCount());

            UNIT_ASSERT_VALUES_UNEQUAL(
                0,
                msg.GetDevices(0).GetNodeId());

            // from same node
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                msg.GetDevices(1).GetNodeId());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(1).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(2).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(1).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetReplicas(0).GetDevices(2).GetBlocksCount());

            UNIT_ASSERT_VALUES_UNEQUAL(
                0,
                msg.GetReplicas(0).GetDevices(0).GetNodeId());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, dirtyDevices.size());

        runtime->AdvanceCurrentTime(replaceFreezePeriod);
        runtime->DispatchEvents({}, 1s);

        // replaced devices should be deleted from AutomaticallyReplacedDevices
        // and thus they are allowed to be erased now

        UNIT_ASSERT_VALUES_EQUAL(3, dirtyDevices.size());
        SortBy(dirtyDevices, [] (const auto& d) {
            return d.GetDeviceUUID();
        });
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2", dirtyDevices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", dirtyDevices[2].GetDeviceUUID());
    }

    Y_UNIT_TEST(ShouldDeallocateDiskSync)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2")
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1"),
                Device("dev-2", "uuid-2.2")
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .With([] {
                auto config = CreateDefaultStorageConfig();

                // disable timeout
                config.SetNonReplicatedSecureEraseTimeout(Max<ui32>());

                return config;
            }())
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAndWaitForAgents(*runtime, agents);

        {
            auto request = diskRegistry.CreateAllocateDiskRequest("vol0", 20_GB);
            *request->Record.AddAgentIds() = agents[0].GetAgentId();

            diskRegistry.SendRequest(std::move(request));

            auto response = diskRegistry.RecvAllocateDiskResponse();

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", msg.GetDevices(1).GetDeviceUUID());
        }

        diskRegistry.ChangeDeviceState("uuid-1.1", NProto::DEVICE_STATE_WARNING);

        {
            auto request = diskRegistry.CreateAllocateDiskRequest("vol0", 20_GB);
            diskRegistry.SendRequest(std::move(request));

            auto response = diskRegistry.RecvAllocateDiskResponse();

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.MigrationsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.1",
                msg.GetMigrations(0).GetSourceDeviceId());

            UNIT_ASSERT_VALUES_EQUAL(
                agents[1].GetAgentId(),
                msg.GetMigrations(0).GetTargetDevice().GetAgentId());
        }

        auto tryRecvDeallocResponse = [&] {
            TAutoPtr<NActors::IEventHandle> handle;
            runtime->GrabEdgeEventRethrow<
                TEvDiskRegistry::TEvDeallocateDiskResponse>(handle, WaitTimeout);

            std::unique_ptr<TEvDiskRegistry::TEvDeallocateDiskResponse> ptr;

            if (handle) {
                ptr.reset(handle->Release<TEvDiskRegistry::TEvDeallocateDiskResponse>()
                    .Release());
            }

            return ptr;
        };

        TVector<std::unique_ptr<IEventHandle>> secureEraseDeviceRequests;

        auto oldOfn = runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvDiskAgent::EvSecureEraseDeviceRequest) {
                    event->DropRewrite();
                    secureEraseDeviceRequests.push_back(std::unique_ptr<IEventHandle> {
                        event.Release()
                    });

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        diskRegistry.MarkDiskForCleanup("vol0");

        // first request
        diskRegistry.SendDeallocateDiskRequest(
            "vol0",
            true    // sync
        );

        UNIT_ASSERT(tryRecvDeallocResponse() == nullptr);

        // second request
        diskRegistry.SendDeallocateDiskRequest(
            "vol0",
            true    // sync
        );

        UNIT_ASSERT(tryRecvDeallocResponse() == nullptr);

        runtime->DispatchEvents([&] {
            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return secureEraseDeviceRequests.size() == 3;
            };
            return options;
        }());

        UNIT_ASSERT(tryRecvDeallocResponse() == nullptr);

        runtime->SetObserverFunc(oldOfn);

        while (secureEraseDeviceRequests.size() != 1) {
            runtime->AdvanceCurrentTime(5min);
            runtime->Send(secureEraseDeviceRequests.back().release());

            secureEraseDeviceRequests.pop_back();

            runtime->AdvanceCurrentTime(5min);
            runtime->DispatchEvents({}, 10ms);
        }

        UNIT_ASSERT(tryRecvDeallocResponse() == nullptr);

        runtime->Send(secureEraseDeviceRequests.back().release(), 1);

        UNIT_ASSERT(tryRecvDeallocResponse() != nullptr);
        UNIT_ASSERT(tryRecvDeallocResponse() != nullptr);
    }

    Y_UNIT_TEST(ShouldWaitSecureEraseAfterRegularDeallocation)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2")
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .With([] {
                auto config = CreateDefaultStorageConfig();

                // disable timeout
                config.SetNonReplicatedSecureEraseTimeout(Max<ui32>());

                return config;
            }())
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("vol0", 20_GB);

        auto tryRecvDeallocResponse = [&] {
            TAutoPtr<NActors::IEventHandle> handle;
            runtime->GrabEdgeEventRethrow<
                TEvDiskRegistry::TEvDeallocateDiskResponse>(handle, WaitTimeout);

            std::unique_ptr<TEvDiskRegistry::TEvDeallocateDiskResponse> ptr;

            if (handle) {
                ptr.reset(handle->Release<TEvDiskRegistry::TEvDeallocateDiskResponse>()
                    .Release());
            }

            return ptr;
        };

        TVector<std::unique_ptr<IEventHandle>> secureEraseDeviceRequests;

        auto oldOfn = runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvDiskAgent::EvSecureEraseDeviceRequest) {
                    event->DropRewrite();
                    secureEraseDeviceRequests.push_back(std::unique_ptr<IEventHandle> {
                        event.Release()
                    });

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        diskRegistry.MarkDiskForCleanup("vol0");

        // first request (async)
        diskRegistry.DeallocateDisk(
            "vol0",
            false    // sync
        );

        runtime->DispatchEvents({
            .CustomFinalCondition = [&] {
                return secureEraseDeviceRequests.size() == 2;
            }
        });

        // second request (sync)
        diskRegistry.SendDeallocateDiskRequest(
            "vol0",
            true    // sync
        );

        UNIT_ASSERT(!tryRecvDeallocResponse());

        runtime->SetObserverFunc(oldOfn);

        runtime->AdvanceCurrentTime(5min);
        runtime->Send(secureEraseDeviceRequests.back().release());

        secureEraseDeviceRequests.pop_back();

        runtime->AdvanceCurrentTime(5min);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT(!tryRecvDeallocResponse());

        runtime->Send(secureEraseDeviceRequests.back().release());

        auto response = tryRecvDeallocResponse();
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldDestroyBrokenDisks)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", { Device("dev-1", "uuid-1") })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, agents);

        TVector<TString> destroyedDiskIds;
        TAutoPtr<IEventHandle> destroyVolumeRequest;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvService::EvDestroyVolumeRequest
                    && event->Recipient == MakeStorageServiceId())
            {
                auto* msg = event->Get<TEvService::TEvDestroyVolumeRequest>();
                UNIT_ASSERT(msg->Record.GetDestroyIfBroken());
                destroyedDiskIds.push_back(msg->Record.GetDiskId());
                destroyVolumeRequest = event.Release();
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.SendAllocateDiskRequest("vol0", 1000_GB);

        {
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus(),
                response->GetError());
        }

        {
            auto response = diskRegistry.ListBrokenDisks();
            UNIT_ASSERT_VALUES_EQUAL(1, response->DiskIds.size());
            UNIT_ASSERT_VALUES_EQUAL("vol0", response->DiskIds[0]);
        }

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents(TDispatchOptions {
            .CustomFinalCondition = [&] {
                return !!destroyVolumeRequest;
            }});

        UNIT_ASSERT_VALUES_EQUAL(1, destroyedDiskIds.size());
        UNIT_ASSERT_VALUES_EQUAL("vol0", destroyedDiskIds[0]);

        diskRegistry.SendAllocateDiskRequest("vol1", 1000_GB);

        {
            auto response = diskRegistry.ListBrokenDisks();
            UNIT_ASSERT_VALUES_EQUAL(2, response->DiskIds.size());
            Sort(response->DiskIds);
            UNIT_ASSERT_VALUES_EQUAL("vol0", response->DiskIds[0]);
            UNIT_ASSERT_VALUES_EQUAL("vol1", response->DiskIds[1]);
        }

        runtime->Send(destroyVolumeRequest.Release());
        runtime->DispatchEvents({}, 10ms);

        {
            auto response = diskRegistry.ListBrokenDisks();
            UNIT_ASSERT_VALUES_EQUAL(1, response->DiskIds.size());
            UNIT_ASSERT_VALUES_EQUAL("vol1", response->DiskIds[0]);
        }
    }

    Y_UNIT_TEST(ShouldHandleDestroyVolumeError)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", { Device("dev-1", "uuid-1") })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, agents);

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvService::EvDestroyVolumeResponse) {
                auto* msg = event->Get<TEvService::TEvDestroyVolumeResponse>();
                *msg->Record.MutableError() =
                    MakeError(E_REJECTED, "transient error");
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.SendAllocateDiskRequest("vol0", 1000_GB);

        {
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus(),
                response->GetError());
        }

        {
            auto response = diskRegistry.ListBrokenDisks();
            UNIT_ASSERT_VALUES_EQUAL(1, response->DiskIds.size());
            UNIT_ASSERT_VALUES_EQUAL("vol0", response->DiskIds[0]);
        }

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 1s);

        {
            auto response = diskRegistry.ListBrokenDisks();
            UNIT_ASSERT_VALUES_EQUAL(1, response->DiskIds.size());
            UNIT_ASSERT_VALUES_EQUAL("vol0", response->DiskIds[0]);
        }

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 1s);

        {
            auto response = diskRegistry.ListBrokenDisks();
            UNIT_ASSERT_VALUES_EQUAL(0, response->DiskIds.size());
        }
    }

    Y_UNIT_TEST(ShouldRespondWithTryAgainWhenCanAllocateAfterSecureErase)
    {
        const TVector agents{
            CreateAgentConfig(
                "agent-1",
                {
                    MakeLocalDevice("NVMELOCAL01", "uuid-1"),
                    MakeLocalDevice("NVMELOCAL02", "uuid-2"),
                    MakeLocalDevice("NVMELOCAL03", "uuid-3"),
                }),
        };

        auto runtime =
            TTestRuntimeBuilder()
                .With(
                    []
                    {
                        auto config = CreateDefaultStorageConfig();
                        // disable secure erase timeout
                        config.SetNonReplicatedSecureEraseTimeout(Max<ui32>());
                        config.SetNonReplicatedDontSuspendDevices(true);
                        config.SetLocalDiskAsyncDeallocation(true);

                        return config;
                    }())
                .WithAgents(agents)
                .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            [&]
            {
                auto config = CreateRegistryConfig(0, agents);

                auto* ssd = config.AddDevicePoolConfigs();
                ssd->SetName("local-ssd");
                ssd->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                ssd->SetAllocationUnit(LOCAL_DEVICE_SIZE);

                return config;
            }());

        RegisterAgents(*runtime, agents.size());
        WaitForAgents(*runtime, agents.size());

        UNIT_ASSERT_VALUES_EQUAL(0, GetSuspendedDeviceCount(diskRegistry));
        UNIT_ASSERT_VALUES_EQUAL(3, GetDirtyDeviceCount(diskRegistry));
        WaitForSecureErase(*runtime, GetDirtyDeviceCount(diskRegistry));

        // Disk allocation to mark all devices as dirty
        const TString makeDirty = "make_dirty";
        const ui64 diskSize = LOCAL_DEVICE_SIZE * 3;
        {
            auto request =
                diskRegistry.CreateAllocateDiskRequest(makeDirty, diskSize);

            request->Record.SetStorageMediaKind(
                NProto::STORAGE_MEDIA_SSD_LOCAL);

            diskRegistry.SendRequest(std::move(request));

            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(3, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "NVMELOCAL01",
                msg.GetDevices(0).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(
                "NVMELOCAL02",
                msg.GetDevices(1).GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(
                "NVMELOCAL03",
                msg.GetDevices(2).GetDeviceName());
        }

        // Intercept all secure erase requests to keep devices dirty
        TVector<std::unique_ptr<IEventHandle>> secureEraseDeviceRequests;
        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvSecureEraseDeviceRequest)
                {
                    event->DropRewrite();
                    secureEraseDeviceRequests.push_back(
                        std::unique_ptr<IEventHandle>{event.Release()});

                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // Disk deallocation to mark all devices as dirty
        {
            diskRegistry.MarkDiskForCleanup(makeDirty);

            auto response = diskRegistry.DeallocateDisk(
                makeDirty,
                false   // sync
            );

            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // Try to allocate disk with dirty devices. We'll get a E_TRY_AGAIN
        const TString diskId = "local0";
        auto tryAllocateDisk = [&](bool shouldFail)
        {
            auto request =
                diskRegistry.CreateAllocateDiskRequest(diskId, diskSize);
            request->Record.SetStorageMediaKind(
                NProto::STORAGE_MEDIA_SSD_LOCAL);
            *request->Record.AddAgentIds() = agents[0].GetAgentId();

            diskRegistry.SendRequest(std::move(request));

            auto response = diskRegistry.RecvAllocateDiskResponse();
            if (shouldFail) {
                UNIT_ASSERT(HasError(response->GetError()));
                UNIT_ASSERT_EQUAL(E_TRY_AGAIN, response->GetStatus());
                return;
            }
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        };
        {
            auto request =
                diskRegistry.CreateAllocateDiskRequest(diskId, diskSize);
            request->Record.SetStorageMediaKind(
                NProto::STORAGE_MEDIA_SSD_LOCAL);
            *request->Record.AddAgentIds() = agents[0].GetAgentId();

            diskRegistry.SendRequest(std::move(request));

            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT(HasError(response->GetError()));
            UNIT_ASSERT_EQUAL(E_TRY_AGAIN, response->GetStatus());
        }

        // Secure erase all devices except one
        runtime->DispatchEvents(
            [&]
            {
                TDispatchOptions options;
                options.CustomFinalCondition = [&]
                {
                    return secureEraseDeviceRequests.size() == 3;
                };
                return options;
            }());

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        UNIT_ASSERT_EQUAL(secureEraseDeviceRequests.size(), 3UL);
        while (secureEraseDeviceRequests.size() != 1) {
            runtime->AdvanceCurrentTime(5min);
            runtime->Send(secureEraseDeviceRequests.back().release());

            secureEraseDeviceRequests.pop_back();

            runtime->AdvanceCurrentTime(5min);
            runtime->DispatchEvents({}, 10ms);

            tryAllocateDisk(true);
        }

        // Still no allocation
        tryAllocateDisk(true);

        // Secure erase last device
        runtime->Send(secureEraseDeviceRequests.back().release());

        // Adavance time a little
        runtime->DispatchEvents({}, 10ms);

        // When the last dirty device is clean, we can allocate disk
        tryAllocateDisk(false);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
