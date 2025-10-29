#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto GetBackup(TDiskRegistryClient& dr)
    -> NProto::TDiskRegistryStateBackup
{
    auto response = dr.BackupDiskRegistryState(
        false // localDB
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldAllocateOnTargetAgentId)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-3", "rack-1", 10_GB),
                Device("dev-2", "uuid-4", "rack-1", 10_GB)
            })};

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        auto getDiskCount = [&] {
            return GetBackup(diskRegistry).DisksSize();
        };

        auto allocateAt = [&] (TString diskId, TString agentId) {
            auto request = diskRegistry.CreateAllocateDiskRequest(diskId, 10_GB);
            *request->Record.MutableAgentIds()->Add() = agentId;
            diskRegistry.SendRequest(std::move(request));
            return diskRegistry.RecvAllocateDiskResponse();
        };

        {
            auto response = allocateAt("disk-1", "unknown");
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0, getDiskCount());
        }

        {
            auto response = allocateAt("disk-1", "agent-1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("agent-1", msg.GetDevices(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(1, getDiskCount());
        }

        {
            auto response = allocateAt("disk-2", "agent-1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("agent-1", msg.GetDevices(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(2, getDiskCount());
        }

        {
            auto response = allocateAt("disk-3", "agent-1");
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(2, getDiskCount());
        }

        {
            auto response = allocateAt("disk-3", "agent-2");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(3, getDiskCount());

            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("agent-2", msg.GetDevices(0).GetAgentId());
        }
    }

    Y_UNIT_TEST(ShouldQueryAvailableStorage)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1"),
                Device("dev-2", "uuid-2") | WithPool("foo", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-3", "uuid-3"),
                Device("dev-4", "uuid-4") | WithPool("foo", NProto::DEVICE_POOL_KIND_LOCAL)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-5"),
                Device("dev-2", "uuid-6")
            }),
            CreateAgentConfig("agent-3", {
                Device("dev-1", "uuid-7") | WithPool("bar", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-8") | WithPool("bar", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-3", "uuid-9")
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig([&] {
            auto config = CreateRegistryConfig(0, agents);

            auto* foo = config.AddDevicePoolConfigs();
            foo->SetName("foo");
            foo->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
            foo->SetAllocationUnit(3_GB);

            auto* bar = config.AddDevicePoolConfigs();
            bar->SetName("bar");
            bar->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
            bar->SetAllocationUnit(4_GB);

            return config;
        }());


        RegisterAgents(*runtime, agents.size());
        WaitForAgents(*runtime, agents.size());

        UNIT_ASSERT_VALUES_EQUAL(4, GetSuspendedDeviceCount(diskRegistry));
        WaitForSecureErase(*runtime, GetDirtyDeviceCount(diskRegistry));

        auto query = [&] (TVector<TString> ids, const TString& pool) {
            auto response = diskRegistry.QueryAvailableStorage(
                ids,
                pool,
                NProto::STORAGE_POOL_KIND_LOCAL);

            auto& msg = response->Record;
            SortBy(*msg.MutableAvailableStorage(), [] (auto& info) {
                return info.GetAgentId();
            });
            return msg;
        };

        {
            auto msg = query({"agent-1", "agent-2", "agent-3"}, {});

            UNIT_ASSERT_VALUES_EQUAL(3, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-2", msg.GetAvailableStorage(1).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-3", msg.GetAvailableStorage(2).GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(2).GetChunkSize());

            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(2).GetChunkCount());
        }

        diskRegistry.ResumeDevice("agent-1", "dev-2", /*dryRun=*/false);
        UNIT_ASSERT_VALUES_EQUAL(4, GetSuspendedDeviceCount(diskRegistry));

        WaitForSecureErase(*runtime, 1);

        UNIT_ASSERT_VALUES_EQUAL(3, GetSuspendedDeviceCount(diskRegistry));

        {
            auto msg = query({"agent-1", "agent-2", "agent-3"}, {});

            UNIT_ASSERT_VALUES_EQUAL(3, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-2", msg.GetAvailableStorage(1).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-3", msg.GetAvailableStorage(2).GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(
                3_GB, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(2).GetChunkSize());

            UNIT_ASSERT_VALUES_EQUAL(
                1, msg.GetAvailableStorage(0).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(2).GetChunkCount());
        }

        diskRegistry.ResumeDevice("agent-1", "dev-4", /*dryRun=*/false);
        diskRegistry.ResumeDevice("agent-3", "dev-1", /*dryRun=*/false);
        diskRegistry.ResumeDevice("agent-3", "dev-2", /*dryRun=*/false);

        WaitForSecureErase(*runtime, GetDirtyDeviceCount(diskRegistry));

        UNIT_ASSERT_VALUES_EQUAL(0, GetSuspendedDeviceCount(diskRegistry));

        {
            auto msg = query({"agent-1", "agent-2", "agent-3", "agent-2"}, "foo");

            UNIT_ASSERT_VALUES_EQUAL(3, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-2", msg.GetAvailableStorage(1).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-3", msg.GetAvailableStorage(2).GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(
                3_GB, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(2).GetChunkSize());

            UNIT_ASSERT_VALUES_EQUAL(
                2, msg.GetAvailableStorage(0).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(2).GetChunkCount());
        }

        {
            auto msg = query({"agent-1", "agent-2", "agent-3"}, "bar");

            UNIT_ASSERT_VALUES_EQUAL(3, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-2", msg.GetAvailableStorage(1).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-3", msg.GetAvailableStorage(2).GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                4_GB, msg.GetAvailableStorage(2).GetChunkSize());

            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                2, msg.GetAvailableStorage(2).GetChunkCount());
        }

        {
            auto msg = query({"agent-1", "agent-2", "agent-3"}, "unknown");

            UNIT_ASSERT_VALUES_EQUAL(3, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-2", msg.GetAvailableStorage(1).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-3", msg.GetAvailableStorage(2).GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(2).GetChunkSize());

            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(2).GetChunkCount());
        }

        {
            auto msg = query({"agent-1"}, "bar");

            UNIT_ASSERT_VALUES_EQUAL(1, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkCount());
        }

        {
            auto msg = query({"agent-2"}, "foo");

            UNIT_ASSERT_VALUES_EQUAL(1, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-2", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(0).GetChunkCount());
        }

        {
            auto msg = query({"agent-1", "agent-2", "agent-3"}, {});

            UNIT_ASSERT_VALUES_EQUAL(3, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-2", msg.GetAvailableStorage(1).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-3", msg.GetAvailableStorage(2).GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(
                3_GB, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                4_GB, msg.GetAvailableStorage(2).GetChunkSize());

            UNIT_ASSERT_VALUES_EQUAL(
                2, msg.GetAvailableStorage(0).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                2, msg.GetAvailableStorage(2).GetChunkCount());
        }

        // allocate local ssd on agent-1
        {
            auto request = diskRegistry.CreateAllocateDiskRequest("disk-1", 3_GB);
            request->Record.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);
            *request->Record.MutableAgentIds()->Add() = "agent-1";
            diskRegistry.SendRequest(std::move(request));
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            auto& d = msg.GetDevices(0);
            UNIT_ASSERT_VALUES_EQUAL("agent-1", d.GetAgentId());
            UNIT_ASSERT("uuid-2" == d.GetDeviceUUID()
                     || "uuid-4" == d.GetDeviceUUID());
        }

        {
            auto msg = query({"agent-1"}, {});
            UNIT_ASSERT_VALUES_EQUAL(1, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                3_GB, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                2, msg.GetAvailableStorage(0).GetChunkCount());
        }

        diskRegistry.MarkDiskForCleanup("disk-1");
        diskRegistry.DeallocateDisk("disk-1");

        // dirty device...
        {
            auto msg = query({"agent-1"}, {});
            UNIT_ASSERT_VALUES_EQUAL(1, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                3_GB, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                2, msg.GetAvailableStorage(0).GetChunkCount());
        }

        // wait for celanup
        {
            TDispatchOptions options;
            options.FinalEvents = {
                TDispatchOptions::TFinalEventCondition(
                    TEvDiskRegistryPrivate::EvSecureEraseResponse)
            };
            runtime->DispatchEvents(options);
        }

        {
            auto msg = query({"agent-1"}, {});
            UNIT_ASSERT_VALUES_EQUAL(1, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                3_GB, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                2, msg.GetAvailableStorage(0).GetChunkCount());
        }

        diskRegistry.ChangeAgentState("agent-3", NProto::AGENT_STATE_WARNING);
        diskRegistry.ChangeDeviceState("uuid-4", NProto::DEVICE_STATE_ERROR);

        {
            auto msg = query({"agent-1", "agent-3"}, {});
            UNIT_ASSERT_VALUES_EQUAL(2, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-3", msg.GetAvailableStorage(1).GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(
                3_GB, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                4_GB, msg.GetAvailableStorage(1).GetChunkSize());

            UNIT_ASSERT_VALUES_EQUAL(
                1, msg.GetAvailableStorage(0).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                2, msg.GetAvailableStorage(1).GetChunkCount());
        }

        {
            auto msg = query({"agent-1", "agent-3"}, "foo");
            UNIT_ASSERT_VALUES_EQUAL(2, msg.AvailableStorageSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-1", msg.GetAvailableStorage(0).GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                "agent-3", msg.GetAvailableStorage(1).GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(
                3_GB, msg.GetAvailableStorage(0).GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkSize());

            UNIT_ASSERT_VALUES_EQUAL(
                1, msg.GetAvailableStorage(0).GetChunkCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0, msg.GetAvailableStorage(1).GetChunkCount());
        }
    }

    Y_UNIT_TEST(ShouldNotSuspendDevices)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1"),
                Device("dev-2", "uuid-2") | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-3", "uuid-3"),
                Device("dev-4", "uuid-4") | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-5"),
                Device("dev-2", "uuid-6")
            }),
            CreateAgentConfig("agent-3", {
                Device("dev-1", "uuid-7") | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-8") | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-3", "uuid-9")
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .With([] {
                auto config = CreateDefaultStorageConfig();
                config.SetNonReplicatedDontSuspendDevices(true);

                return config;
            }())
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig([&] {
            auto config = CreateRegistryConfig(0, agents);

            auto* ssd = config.AddDevicePoolConfigs();
            ssd->SetName("local-ssd");
            ssd->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
            ssd->SetAllocationUnit(8_GB);

            return config;
        }());


        RegisterAgents(*runtime, agents.size());
        WaitForAgents(*runtime, agents.size());

        UNIT_ASSERT_VALUES_EQUAL(0, GetSuspendedDeviceCount(diskRegistry));
        UNIT_ASSERT_VALUES_EQUAL(9, GetDirtyDeviceCount(diskRegistry));
        WaitForSecureErase(*runtime, GetDirtyDeviceCount(diskRegistry));

        auto allocateSSD = [&] (TString diskId) {
            auto request = diskRegistry.CreateAllocateDiskRequest(diskId, 16_GB);
            request->Record.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);
            diskRegistry.SendRequest(std::move(request));
            return diskRegistry.RecvAllocateDiskResponse();
        };

        UNIT_ASSERT_VALUES_EQUAL(S_OK, allocateSSD("local0")->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, allocateSSD("local1")->GetStatus());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
