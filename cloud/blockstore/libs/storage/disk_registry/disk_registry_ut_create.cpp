#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_logbroker.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldCreateDiskFromDevices)
    {
        const ui32 nativeBlockSize = 512;
        const ui64 adjustedBlockCount = 10_GB / nativeBlockSize;
        const ui64 unAdjustedBlockCount = adjustedBlockCount + 100;

        auto deviceConfig = [&] (auto name, auto uuid) {
            NProto::TDeviceConfig config;

            config.SetDeviceName(name);
            config.SetDeviceUUID(uuid);
            config.SetBlockSize(nativeBlockSize);
            config.SetBlocksCount(adjustedBlockCount);
            config.SetUnadjustedBlockCount(unAdjustedBlockCount);

            return config;
        };

        const TVector agents {
            CreateAgentConfig("agent-1", {
                deviceConfig("dev-1", "uuid-1.1"),
                deviceConfig("dev-2", "uuid-1.2"),
                deviceConfig("dev-3", "uuid-1.3")
            }),
            CreateAgentConfig("agent-2", {
                deviceConfig("dev-1", "uuid-2.1"),
                deviceConfig("dev-2", "uuid-2.2"),
                deviceConfig("dev-3", "uuid-2.3")
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        auto device = [] (auto agentId, auto name) {
            NProto::TDeviceConfig config;
            config.SetAgentId(agentId);
            config.SetDeviceName(name);
            return config;
        };

        const TString busyDeviceId = [&] {
            auto request = diskRegistry.CreateAllocateDiskRequest("disk-1", 10_GB);
            *request->Record.MutableAgentIds()->Add() = "agent-1";
            diskRegistry.SendRequest(std::move(request));
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            return response->Record.GetDevices(0).GetDeviceUUID();
        }();

        {
            diskRegistry.SendCreateDiskFromDevicesRequest(
                "foo",
                4_KB,
                TVector {
                    device("agent-1", "dev-1"),
                    device("agent-1", "dev-2")
                }
            );
            auto response = diskRegistry.RecvCreateDiskFromDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
            UNIT_ASSERT_C(
                response->GetErrorReason().Contains("is allocated for"),
                response->GetErrorReason());
        }

        diskRegistry.CreateDiskFromDevices(
            "foo",
            4_KB,
            TVector {
                device("agent-2", "dev-1"),
                device("agent-2", "dev-2")
            });

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        diskRegistry.MarkDiskForCleanup("disk-1");
        diskRegistry.DeallocateDisk("disk-1");

        {
            diskRegistry.SendCreateDiskFromDevicesRequest(
                "bar",
                4_KB,
                TVector {
                    device("agent-1", "dev-1"),
                    device("agent-1", "dev-2")
                },
                false    // force
            );
            auto response = diskRegistry.RecvCreateDiskFromDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());

            UNIT_ASSERT_VALUES_EQUAL(
                2 * (unAdjustedBlockCount * 512 / 4_KB),
                response->Record.GetBlockCount());
        }

        {
            diskRegistry.SendCreateDiskFromDevicesRequest(
                "bar",
                4_KB,
                TVector {
                    device("agent-1", "dev-1"),
                    device("agent-1", "dev-2")
                },
                false    // force
            );
            auto response = diskRegistry.RecvCreateDiskFromDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response->GetStatus(),
                response->GetErrorReason());

            UNIT_ASSERT_VALUES_EQUAL(
                2 * (unAdjustedBlockCount * 512 / 4_KB),
                response->Record.GetBlockCount());
        }

        {
            diskRegistry.SendCreateDiskFromDevicesRequest(
                "bar",
                4_KB,
                TVector {
                    device("agent-1", "dev-1"),
                    device("agent-1", "dev-3")
                },
                false    // force
            );
            auto response = diskRegistry.RecvCreateDiskFromDevicesResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = diskRegistry.AllocateDisk("bar", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());

            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.1",
                response->Record.GetDevices(0).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.2",
                response->Record.GetDevices(1).GetDeviceUUID());
        }
    }

    Y_UNIT_TEST(ShouldCreateDiskFromSuspendedDevices)
    {
        constexpr ui64 localSSDSize = 100'000'000'000; // ~ 93.13 GiB
        constexpr ui64 localSSDBlockCount =
            2 * (localSSDSize / DefaultLogicalBlockSize);

        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2")
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1")
                    | WithTotalSize(localSSDSize, 512)
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-2.2")
                    | WithTotalSize(localSSDSize, 512)
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        auto checkState = [&] (auto func) {
            // check local DB
            func(true, diskRegistry.BackupDiskRegistryState(true)
                ->Record.GetBackup());

            // check dyn state
            func(false, diskRegistry.BackupDiskRegistryState(false)
                ->Record.GetBackup());
        };

        auto createConfig = [i = 0] (TVector<NProto::TAgentConfig> agents) mutable {
            auto config = CreateRegistryConfig(i++, agents);

            auto* ssd = config.AddDevicePoolConfigs();
            ssd->SetName("local-ssd");
            ssd->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
            ssd->SetAllocationUnit(localSSDSize);

            return config;
        };

        auto allocateSSD = [&] (TString diskId) {
            const ui64 size = localSSDBlockCount * DefaultLogicalBlockSize;

            auto request = diskRegistry.CreateAllocateDiskRequest(diskId, size);

            request->Record.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);

            diskRegistry.SendRequest(std::move(request));

            return diskRegistry.RecvAllocateDiskResponse()->GetStatus();
        };

        diskRegistry.UpdateConfig(createConfig({agents[0]}));


        // register and wait for agent-1
        RegisterAgent(*runtime, 0);
        WaitForAgents(*runtime, 0);
        WaitForSecureErase(*runtime, {agents[0]});

        checkState([&] (bool isDB, auto&& backup) {
            Y_UNUSED(isDB);
            UNIT_ASSERT_VALUES_EQUAL(1, backup.AgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(0, backup.DirtyDevicesSize());
        });

        diskRegistry.UpdateConfig(createConfig(agents));

        // register and wait for agent-2
        RegisterAgent(*runtime, 1);
        WaitForAgents(*runtime, 1);

        checkState([&] (bool isDB, auto&& backup) {
            Y_UNUSED(isDB);
            UNIT_ASSERT_VALUES_EQUAL(2, backup.AgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(2, backup.SuspendedDevicesSize());
        });

        UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, allocateSSD("vol0"));

        {
            TVector<NProto::TDeviceConfig> devices;

            auto& dev1 = devices.emplace_back();
            dev1.SetAgentId("agent-2");
            dev1.SetDeviceName("dev-1");

            auto& dev2 = devices.emplace_back();
            dev2.SetAgentId("agent-2");
            dev2.SetDeviceName("dev-2");

            diskRegistry.SendCreateDiskFromDevicesRequest("ssd0", 4_KB, devices);

            auto response = diskRegistry.RecvCreateDiskFromDevicesResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());

            UNIT_ASSERT_VALUES_EQUAL(
                localSSDBlockCount,
                response->Record.GetBlockCount());
        }

        checkState([&] (bool isDB, auto&& backup) {
            Y_UNUSED(isDB);

            UNIT_ASSERT_VALUES_EQUAL(2, backup.AgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(0, backup.SuspendedDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, backup.DirtyDevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(1, backup.DisksSize());
            const auto& disk = backup.GetDisks(0);
            UNIT_ASSERT_VALUES_EQUAL("ssd0", disk.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(2, disk.DeviceUUIDsSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", disk.GetDeviceUUIDs(0));
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", disk.GetDeviceUUIDs(1));
        });

        checkState([&] (bool isDB, auto&& backup) {
            Y_UNUSED(isDB);

            UNIT_ASSERT_VALUES_EQUAL(2, backup.AgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(0, backup.SuspendedDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, backup.DirtyDevicesSize());
        });

        diskRegistry.CleanupDisks();

        {
            auto response = diskRegistry.DescribeDisk("ssd0");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                response->Record.GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                localSSDBlockCount,
                response->Record.GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL("", response->Record.GetCloudId());
            UNIT_ASSERT_VALUES_EQUAL("", response->Record.GetFolderId());

            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2.1",
                response->Record.GetDevices(0).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2.2",
                response->Record.GetDevices(1).GetDeviceUUID());
        }

        UNIT_ASSERT_VALUES_EQUAL(S_OK, allocateSSD("ssd0"));

        {
            auto response = diskRegistry.DescribeDisk("ssd0");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());

            UNIT_ASSERT_VALUES_EQUAL("yc.nbs", response->Record.GetCloudId());
            UNIT_ASSERT_VALUES_EQUAL("yc.nbs.tests", response->Record.GetFolderId());
        }

        UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, allocateSSD("vol0"));

        diskRegistry.MarkDiskForCleanup("ssd0");
        diskRegistry.DeallocateDisk("ssd0");
        WaitForSecureErase(*runtime, {agents[1]});

        checkState([&] (bool isDB, auto&& backup) {
            Y_UNUSED(isDB);

            UNIT_ASSERT_VALUES_EQUAL(agents.size(), backup.AgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(0, backup.SuspendedDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, backup.DirtyDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, backup.DisksSize());
        });

        UNIT_ASSERT_VALUES_EQUAL(S_OK, allocateSSD("vol0"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
