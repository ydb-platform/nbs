#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_logbroker.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
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

#define EXPECT_DISK_STATE(                                                     \
    response, expectedDeviceCount, expectedIoMode, expectedMuteIoErrors)       \
{                                                                              \
    const auto& record = response->Record;                                     \
    UNIT_ASSERT_VALUES_EQUAL(expectedDeviceCount, record.DevicesSize());       \
    UNIT_ASSERT_VALUES_EQUAL(expectedMuteIoErrors, record.GetMuteIOErrors());  \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        EVolumeIOMode_Name(expectedIoMode),                                    \
        EVolumeIOMode_Name(record.GetIOMode()));                               \
}                                                                              \
// EXPECT_DISK_STATE

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldRecoverStateOnReboot)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
            Device("dev-3", "uuid-3", "rack-1", 10_GB),
            Device("dev-4", "uuid-4", "rack-1", 10_GB),
            Device("dev-5", "uuid-5", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        diskRegistry.AllocateDisk("disk-1", 20_GB);
        diskRegistry.AllocateDisk("disk-2", 20_GB);
        diskRegistry.AllocateDisk("disk-3", 10_GB);

        diskRegistry.MarkDiskForCleanup("disk-2");
        diskRegistry.DeallocateDisk("disk-2");

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        diskRegistry.AcquireDisk("disk-1", "session-1");
        diskRegistry.AcquireDisk("disk-3", "session-2");

        diskRegistry.SendAcquireDiskRequest("disk-2", "session-3");
        auto response = diskRegistry.RecvAcquireDiskResponse();
        UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldCleanupDisks)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        TSSProxyClient ss(*runtime);

        ss.CreateVolume("vol");
        ss.CreateVolume("nonrepl-vol");

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB);
        diskRegistry.AllocateDisk("nonrepl-garbage", 10_GB);

        UNIT_ASSERT(diskRegistry.Exists("nonrepl-vol"));
        UNIT_ASSERT(diskRegistry.Exists("nonrepl-garbage"));

        diskRegistry.MarkDiskForCleanup("nonrepl-vol");
        diskRegistry.MarkDiskForCleanup("nonrepl-garbage");

        diskRegistry.CleanupDisks();

        UNIT_ASSERT(diskRegistry.Exists("nonrepl-vol"));
        UNIT_ASSERT(!diskRegistry.Exists("nonrepl-garbage"));
    }

    Y_UNIT_TEST(ShouldCleanupMirroredDisks)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-2", 10_GB),
            Device("dev-2", "uuid-4", "rack-2", 10_GB),
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

        TSSProxyClient ss(*runtime);

        ss.CreateVolume("vol");
        ss.CreateVolume("mirrored-vol");

        diskRegistry.AllocateDisk(
            "mirrored-vol",
            10_GB,
            DefaultLogicalBlockSize,
            "", // placementGroupId
            0,  // placementPartitionIndex
            "", // cloudId
            "", // folderId
            1   // replicaCount
        );
        diskRegistry.AllocateDisk(
            "mirrored-garbage",
            10_GB,
            DefaultLogicalBlockSize,
            "", // placementGroupId
            0,  // placementPartitionIndex
            "", // cloudId
            "", // folderId
            1   // replicaCount
        );

        // checking that our volumes are actually mirrored
        {
            auto response = diskRegistry.DescribeDisk("mirrored-vol");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
        }

        {
            auto response = diskRegistry.DescribeDisk("mirrored-garbage");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
        }

        UNIT_ASSERT(diskRegistry.Exists("mirrored-vol"));
        UNIT_ASSERT(diskRegistry.Exists("mirrored-garbage"));

        diskRegistry.MarkDiskForCleanup("mirrored-vol");
        diskRegistry.MarkDiskForCleanup("mirrored-garbage");

        diskRegistry.CleanupDisks();

        UNIT_ASSERT(diskRegistry.Exists("mirrored-vol"));
        UNIT_ASSERT(!diskRegistry.Exists("mirrored-garbage"));
    }

    Y_UNIT_TEST(ShouldReplaceLaggingDevices)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-3", "rack-2", 10_GB),
                Device("dev-2", "uuid-4", "rack-2", 10_GB),
            });

        auto runtime =
            TTestRuntimeBuilder().WithAgents({agent1, agent2}).Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        TSSProxyClient ss(*runtime);
        ss.CreateVolume("mirrored-vol");
        diskRegistry.AllocateDisk(
            "mirrored-vol",
            10_GB,
            DefaultLogicalBlockSize,
            "",   // placementGroupId
            0,    // placementPartitionIndex
            "",   // cloudId
            "",   // folderId
            1,    // replicaCount
            NProto::STORAGE_MEDIA_SSD_MIRROR2);

        // Send volume reallocations.
        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        {
            auto response = diskRegistry.DescribeDisk("mirrored-vol");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", r.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                r.GetReplicas(0).GetDevices(0).GetDeviceUUID());
        }

        TVector<TString> notifiedDiskIds;
        runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvVolume::EvReallocateDiskRequest &&
                    event->Recipient == MakeVolumeProxyServiceId())
                {
                    auto* msg =
                        event->Get<TEvVolume::TEvReallocateDiskRequest>();
                    notifiedDiskIds.push_back(msg->Record.GetDiskId());
                }

                return false;
            });

        // Add lagging device.
        TVector<NProto::TLaggingDevice> laggingDevices;
        NProto::TLaggingDevice laggingDevice;
        laggingDevice.SetDeviceUUID("uuid-3");
        laggingDevice.SetRowIndex(0);
        laggingDevices.push_back(std::move(laggingDevice));
        auto response = diskRegistry.AddOutdatedLaggingDevices(
            "mirrored-vol",
            std::move(laggingDevices));
        UNIT_ASSERT_SUCCESS(response->GetError());

        // We should send a reallocate request.
        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT_VALUES_EQUAL(1, notifiedDiskIds.size());
        UNIT_ASSERT_VALUES_EQUAL("mirrored-vol", notifiedDiskIds[0]);

        {
            auto response = diskRegistry.DescribeDisk("mirrored-vol");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", r.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                r.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetDeviceReplacementUUIDs().size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                r.GetDeviceReplacementUUIDs()[0]);
        }
    }

    Y_UNIT_TEST(ShouldReplaceLaggingDevicesAfterDiskReallocation)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-3", "rack-2", 10_GB),
                Device("dev-2", "uuid-4", "rack-2", 10_GB),
            });

        NProto::TStorageServiceConfig config = CreateDefaultStorageConfig();
        config.SetDiskRegistryDisksNotificationTimeout(1);
        auto runtime = TTestRuntimeBuilder()
                           .With(config)
                           .WithAgents({agent1, agent2})
                           .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        TSSProxyClient ss(*runtime);
        ss.CreateVolume("mirrored-vol");
        diskRegistry.AllocateDisk(
            "mirrored-vol",
            10_GB,
            DefaultLogicalBlockSize,
            "",   // placementGroupId
            0,    // placementPartitionIndex
            "",   // cloudId
            "",   // folderId
            1,    // replicaCount
            NProto::STORAGE_MEDIA_SSD_MIRROR2);

        // Send volume reallocations.
        runtime->DispatchEvents({}, 10ms);

        {
            auto response = diskRegistry.DescribeDisk("mirrored-vol");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", r.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                r.GetReplicas(0).GetDevices(0).GetDeviceUUID());
        }

        ui32 reallocateCount = 0;
        runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvReallocateDiskRequest: {
                        auto* msg =
                            event->Get<TEvVolume::TEvReallocateDiskRequest>();
                        UNIT_ASSERT_VALUES_EQUAL(
                            "mirrored-vol",
                            msg->Record.GetDiskId());
                        ++reallocateCount;
                        break;
                    }

                    case TEvVolume::EvReallocateDiskResponse: {
                        auto* msg =
                            event->Get<TEvVolume::TEvReallocateDiskResponse>();
                        UNIT_ASSERT_VALUES_EQUAL(
                            0,
                            msg->Record.OutdatedLaggingDevicesSize());

                        static bool onceGuard = false;
                        if (onceGuard) {
                            break;
                        }
                        onceGuard = true;

                        auto* laggingDevice =
                            msg->Record.AddOutdatedLaggingDevices();
                        laggingDevice->SetDeviceUUID("uuid-1");
                        laggingDevice->SetRowIndex(0);
                        break;
                    }
                }
                return false;
            });

        // Trigger the reallocation. It should add "uuid-1" to outdated lagging
        // devices.
        diskRegistry.ChangeDeviceState(
            "uuid-1",
            NProto::EDeviceState::DEVICE_STATE_WARNING);

        // We should do 2 reallocations: one for changing deivce state and one
        // for the outdated lagging devices. Wait for both here.
        runtime->DispatchEvents({}, 100ms);
        UNIT_ASSERT_VALUES_EQUAL(2, reallocateCount);

        // "uuid-1" should be replaced with "uuid-2".
        {
            auto response = diskRegistry.DescribeDisk("mirrored-vol");
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", r.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                r.GetReplicas(0).GetDevices(0).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, r.GetDeviceReplacementUUIDs().size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                r.GetDeviceReplacementUUIDs()[0]);
        }
    }

    Y_UNIT_TEST(ShouldRepairVolume)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        TSSProxyClient ss(*runtime);

        ss.CreateVolume("vol");
        ss.CreateVolume("nonrepl-vol-1");
        ss.CreateVolume("nonrepl-vol-2");
        diskRegistry.AllocateDisk("nonrepl-vol-2", 10_GB);

        UNIT_ASSERT(!diskRegistry.Exists("nonrepl-vol-1"));

        diskRegistry.CleanupDisks();

        // UNIT_ASSERT(diskRegistry.Exists("nonrepl-vol-1"));
    }

    Y_UNIT_TEST(ShouldNotCleanupUnmarkedDisks)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        TSSProxyClient ss(*runtime);

        ss.CreateVolume("vol");
        ss.CreateVolume("nonrepl-vol");

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB);
        diskRegistry.AllocateDisk("nonrepl-garbage", 10_GB);

        diskRegistry.CleanupDisks();

        UNIT_ASSERT(diskRegistry.Exists("nonrepl-vol"));
        UNIT_ASSERT(diskRegistry.Exists("nonrepl-garbage"));
    }

    Y_UNIT_TEST(ShouldNotCleanupAliveDisks)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
            Device("dev-3", "uuid-3", "rack-1", 10_GB),
            Device("dev-4", "uuid-4", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        TSSProxyClient ss(*runtime);

        ss.CreateVolume("nonrepl-vol-11");
        ss.CreateVolume("nonrepl-vol-x");
        ss.CreateVolume("nonrepl-vol-2");
        ss.CreateVolume("nonrepl-vol-10");

        diskRegistry.AllocateDisk("nonrepl-vol-2", 10_GB);
        diskRegistry.AllocateDisk("nonrepl-vol-10", 10_GB);
        diskRegistry.AllocateDisk("nonrepl-vol-x", 10_GB);
        diskRegistry.AllocateDisk("nonrepl-vol-11", 10_GB);

        size_t toRemove = 0;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistry::EvDeallocateDiskRequest:
                    toRemove++;
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.CleanupDisks();

        UNIT_ASSERT_VALUES_EQUAL(0, toRemove);
    }

    Y_UNIT_TEST(ShouldCleanDirtyDevicesAfterReboot)
    {
        const auto agentConfig = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto agent = CreateTestDiskAgent(agentConfig);
        agent->HandleSecureEraseDeviceImpl = [] (const auto&, const auto&) {
            // ignore
            return true;
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agentConfig }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);

        {
            // can't allocate new disk: all devices are dirty
            diskRegistry.SendAllocateDiskRequest("disk-2", 20_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_DISK_ALLOCATION_FAILED);
        }

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        {
            // still can't allocate new disk
            diskRegistry.SendAllocateDiskRequest("disk-2", 20_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_DISK_ALLOCATION_FAILED);
        }
    }

    Y_UNIT_TEST(ShouldRetryCleanDevices)
    {
        const auto agentConfig = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agentConfig })
            .Build();

        int dev1Requests = 0;
        int dev2Requests = 0;

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig({ agentConfig }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agentConfig});

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskAgent::EvSecureEraseDeviceRequest: {
                    auto& msg = *event->Get<TEvDiskAgent::TEvSecureEraseDeviceRequest>();
                    dev2Requests += msg.Record.GetDeviceUUID() == "uuid-2";

                    if (msg.Record.GetDeviceUUID() == "uuid-1") {
                        ++dev1Requests;
                        if (dev1Requests == 1) {
                            auto response = std::make_unique<TEvDiskAgent::TEvSecureEraseDeviceResponse>(
                                MakeError(E_REJECTED));

                            runtime->Send(
                                new IEventHandle(
                                    event->Sender,
                                    event->Recipient,
                                    response.release(),
                                    0, // flags
                                    event->Cookie));

                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.AllocateDisk("disk-1", 20_GB);
        diskRegistry.MarkDiskForCleanup("disk-1");
        diskRegistry.DeallocateDisk("disk-1");

        UNIT_ASSERT_VALUES_EQUAL(0, dev1Requests);
        UNIT_ASSERT_VALUES_EQUAL(0, dev2Requests);

        runtime->AdvanceCurrentTime(6s);
        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT_VALUES_EQUAL(1, dev1Requests);
        UNIT_ASSERT_VALUES_EQUAL(1, dev2Requests);

        runtime->AdvanceCurrentTime(6s);
        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT_VALUES_EQUAL(2, dev1Requests);
        UNIT_ASSERT_VALUES_EQUAL(1, dev2Requests);

        TVector<NProto::TDeviceConfig> devices;
        auto& device = devices.emplace_back();
        device.SetNodeId(runtime->GetNodeId(0));
        device.SetDeviceUUID("uuid-1");
        diskRegistry.SecureErase(devices);

        UNIT_ASSERT_VALUES_EQUAL(dev1Requests, 3);
        UNIT_ASSERT_VALUES_EQUAL(dev2Requests, 1);
    }

    Y_UNIT_TEST(ShouldSecureErase)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent1 }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        TVector<NProto::TDeviceConfig> devices;

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(0));
            device.SetDeviceUUID("uuid-1");
        }

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(0));
            device.SetDeviceUUID("uuid-2");
        }

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(0));
            device.SetDeviceUUID("uuid-3");
        }

        TVector<TString> cleanDevices;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvCleanupDevicesRequest: {
                    auto& msg = *event->Get<TEvDiskRegistryPrivate::TEvCleanupDevicesRequest>();
                    cleanDevices = msg.Devices;
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.SecureErase(devices);

        UNIT_ASSERT_VALUES_EQUAL(cleanDevices.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(cleanDevices[0], "uuid-1");
        UNIT_ASSERT_VALUES_EQUAL(cleanDevices[1], "uuid-2");
        UNIT_ASSERT_VALUES_EQUAL(cleanDevices[2], "uuid-3");
    }

    Y_UNIT_TEST(ShouldHandleUndeliveredSecureEraseRequests)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-3", "uuid-3", "rack-1", 10_GB),
                Device("dev-4", "uuid-4", "rack-1", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, agents);

        KillAgent(*runtime, 0);
        runtime->DispatchEvents({}, 10ms);

        // secure erase

        TVector<TString> cleanDevices;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvCleanupDevicesRequest: {
                    auto& msg = *event->Get<TEvDiskRegistryPrivate::TEvCleanupDevicesRequest>();
                    cleanDevices = msg.Devices;
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TVector<NProto::TDeviceConfig> devices;

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(0));
            device.SetDeviceUUID("uuid-1");
        }

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(0));
            device.SetDeviceUUID("uuid-2");
        }

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(1));
            device.SetDeviceUUID("uuid-3");
        }

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(1));
            device.SetDeviceUUID("uuid-4");
        }

        diskRegistry.SecureErase(devices);

        Sort(cleanDevices);
        UNIT_ASSERT_VALUES_EQUAL(2, cleanDevices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", cleanDevices[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-4", cleanDevices[1]);
    }

    Y_UNIT_TEST(ShouldHandleSecureEraseRequestTimeout)
    {
        const auto agentConfig1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        const auto agentConfig2 = CreateAgentConfig("agent-2", {
            Device("dev-3", "uuid-3", "rack-1", 10_GB),
            Device("dev-4", "uuid-4", "rack-1", 10_GB)
        });

        auto agent = CreateTestDiskAgent(agentConfig1);
        agent->HandleSecureEraseDeviceImpl = [] (const auto&, const auto&) {
            // ignore
            return true;
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent, CreateTestDiskAgent(agentConfig2) })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, { agentConfig1, agentConfig2 }));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);

        // secure erase

        TVector<TString> cleanDevices;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvCleanupDevicesRequest: {
                    auto& msg = *event->Get<TEvDiskRegistryPrivate::TEvCleanupDevicesRequest>();
                    cleanDevices = msg.Devices;
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TVector<NProto::TDeviceConfig> devices;

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(0));
            device.SetDeviceUUID("uuid-1");
        }

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(0));
            device.SetDeviceUUID("uuid-2");
        }

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(1));
            device.SetDeviceUUID("uuid-3");
        }

        {
            auto& device = devices.emplace_back();
            device.SetNodeId(runtime->GetNodeId(1));
            device.SetDeviceUUID("uuid-4");
        }

        diskRegistry.SecureErase(devices, 1s);

        Sort(cleanDevices);
        UNIT_ASSERT_VALUES_EQUAL(2, cleanDevices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", cleanDevices[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-4", cleanDevices[1]);
    }

    Y_UNIT_TEST(ShouldUpdateNodeId)
    {
        auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent }));

        agent.SetNodeId(42);
        diskRegistry.RegisterAgent(agent);
        diskRegistry.CleanupDevices(TVector<TString>{"uuid-1"});

        diskRegistry.AllocateDisk("disk-1", 10_GB);
        diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");

        auto response = diskRegistry.RecvAcquireDiskResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("not delivered", response->GetErrorReason());

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);

        {
            TDiskRegistryClient diskRegistry(*runtime);
            diskRegistry.WaitReady();
            diskRegistry.AcquireDisk("disk-1", "session-1");
        }
    }

    Y_UNIT_TEST(ShouldNotifyDisks)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
            Device("dev-3", "uuid-3", "rack-1", 10_GB),
            Device("dev-4", "uuid-4", "rack-1", 10_GB),
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-5", "rack-2", 10_GB),
            Device("dev-2", "uuid-6", "rack-2", 10_GB),
            Device("dev-3", "uuid-7", "rack-2", 10_GB),
            Device("dev-4", "uuid-8", "rack-2", 10_GB),
        });

        const auto newAgent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
            Device("dev-3", "uuid-3", "rack-1", 10_GB),
            Device("dev-4", "uuid-4", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2, newAgent1 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(
            0,
            {
                agent1,
                agent2,
            }
        ));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        diskRegistry.AllocateDisk("disk-1", 10_GB, DefaultLogicalBlockSize);
        diskRegistry.AllocateDisk("disk-2", 40_GB, DefaultLogicalBlockSize);
        diskRegistry.AllocateDisk("disk-3", 10_GB, DefaultLogicalBlockSize);
        diskRegistry.AllocateDisk("disk-4", 20_GB, DefaultLogicalBlockSize);

        TVector<TString> notifiedDiskIds;
        TList<std::unique_ptr<IEventHandle>> notifications;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvVolume::EvReallocateDiskRequest
                        && event->Recipient == MakeVolumeProxyServiceId())
                {
                    auto* msg = event->Get<TEvVolume::TEvReallocateDiskRequest>();
                    notifiedDiskIds.push_back(msg->Record.GetDiskId());
                    notifications.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        RegisterAgent(*runtime, 2);

        {
            auto response = diskRegistry.ListDisksToNotify();
            UNIT_ASSERT_VALUES_EQUAL(3, response->DiskIds.size());
            Sort(response->DiskIds);
            UNIT_ASSERT_VALUES_EQUAL("disk-1", response->DiskIds[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", response->DiskIds[1]);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", response->DiskIds[2]);
        }

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);
        UNIT_ASSERT_VALUES_EQUAL(3, notifications.size());
        for (auto& notification: notifications) {
            runtime->Send(notification.release());
        }

        UNIT_ASSERT_VALUES_EQUAL(3, notifiedDiskIds.size());
        Sort(notifiedDiskIds.begin(), notifiedDiskIds.end());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", notifiedDiskIds[0]);
        UNIT_ASSERT_VALUES_EQUAL("disk-3", notifiedDiskIds[1]);
        UNIT_ASSERT_VALUES_EQUAL("disk-4", notifiedDiskIds[2]);

        runtime->AdvanceCurrentTime(1s);
        runtime->DispatchEvents({}, 10ms);

        {
            auto response = diskRegistry.ListDisksToNotify();
            UNIT_ASSERT_VALUES_EQUAL(0, response->DiskIds.size());
        }
    }

    Y_UNIT_TEST(ShouldRetryTimedOutNotifications)
    {
        TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
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

        diskRegistry.AllocateDisk("disk-1", 20_GB);

        int requests = 0;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvVolume::EvReallocateDiskRequest
                        && event->Recipient == MakeVolumeProxyServiceId())
                {
                    ++requests;
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_WARNING);

        UNIT_ASSERT_VALUES_EQUAL(1, requests);

        {
            auto response = diskRegistry.ListDisksToNotify();
            UNIT_ASSERT_VALUES_EQUAL(1, response->DiskIds.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, requests);

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(1, requests);

        runtime->AdvanceCurrentTime(30s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(2, requests);

        {
            auto response = diskRegistry.ListDisksToNotify();
            UNIT_ASSERT_VALUES_EQUAL(1, response->DiskIds.size());
        }
    }

    Y_UNIT_TEST(ShouldReplaceDevice)
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

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent1, agent2 }));

        RegisterAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        TVector<TString> devices;

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1", 30_GB, DefaultLogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(3, r.DevicesSize());
            devices = {
                r.GetDevices(0).GetDeviceUUID(),
                r.GetDevices(1).GetDeviceUUID(),
                r.GetDevices(2).GetDeviceUUID()
            };
            Sort(devices);
        }

        diskRegistry.ReplaceDevice("disk-1", devices[0]);

        TVector<TString> newDevices;

        {
            auto response = diskRegistry.DescribeDisk("disk-1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(3, r.DevicesSize());
            newDevices = {
                r.GetDevices(0).GetDeviceUUID(),
                r.GetDevices(1).GetDeviceUUID(),
                r.GetDevices(2).GetDeviceUUID()
            };
            Sort(newDevices);
        }

        TVector<TString> diff;

        std::set_difference(
            devices.cbegin(),
            devices.cend(),
            newDevices.cbegin(),
            newDevices.cend(),
            std::back_inserter(diff));

        UNIT_ASSERT_VALUES_EQUAL(1, diff.size());
        UNIT_ASSERT_VALUES_EQUAL(devices[0], diff[0]);

        {
            diskRegistry.SendAllocateDiskRequest(
                "disk-2", 10_GB, DefaultLogicalBlockSize);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldRejectDisconnectedAgent)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent }));

        auto sender = runtime->AllocateEdgeActor(0);

        auto registerAgent = [&] {
            auto pipe = runtime->ConnectToPipe(
                TestTabletId,
                sender,
                0,
                NKikimr::GetPipeConfigWithRetries());

            auto request = std::make_unique<TEvDiskRegistry::TEvRegisterAgentRequest>();
            *request->Record.MutableAgentConfig() = agent;
            request->Record.MutableAgentConfig()->SetNodeId(runtime->GetNodeId(0));

            auto pipeEv = new IEventHandle(pipe, sender, request.release(), 0, 0);
            pipeEv->Rewrite(NKikimr::TEvTabletPipe::EvSend, pipe);

            runtime->Send(pipeEv, 0, true);

            return pipe;
        };

        auto disconnectAgent = [&](auto pipe) {
            runtime->Send(new IEventHandle(
                pipe, sender, new TEvTabletPipe::TEvShutdown()), 0, true);
        };

        auto pipe = registerAgent();

        runtime->AdvanceCurrentTime(60s);
        runtime->DispatchEvents({}, 10ms);
        disconnectAgent(pipe);

        runtime->AdvanceCurrentTime(1s);
        runtime->DispatchEvents({}, 10ms);

        pipe = registerAgent();

        runtime->AdvanceCurrentTime(13s);
        runtime->DispatchEvents({}, 10ms);

        disconnectAgent(pipe);

        runtime->AdvanceCurrentTime(2s);
        runtime->DispatchEvents({}, 10ms);

        runtime->AdvanceCurrentTime(1s);
        runtime->DispatchEvents({}, 10ms);

        pipe = registerAgent();

        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        {
            diskRegistry.CleanupDevices(TVector<TString>{"uuid-1"});

            diskRegistry.SendAllocateDiskRequest("nonrepl-vol-1", 10_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldRejectDisconnectedAgentDynamically)
    {
        // timings are a bit off in this test due to TestActorSystems defects

        TVector<NProto::TAgentConfig> agents;
        for (ui32 i = 0; i < 5; ++i) {
            agents.push_back(CreateAgentConfig(
                Sprintf("agent-%u", i),
                {
                    Device(
                        "dev-1",
                        Sprintf("uuid-%u", i),
                        Sprintf("rack-%u", i),
                        10_GB),
                }));
        }

        auto config = CreateDefaultStorageConfig();
        config.SetNonReplicatedAgentMinTimeout(10'000); // 10s
        config.SetNonReplicatedAgentMaxTimeout(50'000); // 50s
        config.SetNonReplicatedAgentDisconnectRecoveryInterval(100'000); // 100s
        config.SetNonReplicatedAgentTimeoutGrowthFactor(2);

        auto runtime =
            TTestRuntimeBuilder().WithAgents(agents).With(config).Build();
        // cooldown logic in TAgentList doesn't work with Now() < cooldownTimeout
        // see TAgentList::OnAgentDisconnected
        runtime->AdvanceCurrentTime(1h);

        TString agentId;
        NProto::EAgentState agentState = NProto::AGENT_STATE_ONLINE;

        runtime->SetObserverFunc( [&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistry::EvChangeAgentStateRequest: {
                    auto& msg = *event->Get<TEvDiskRegistry::TEvChangeAgentStateRequest>();
                    agentId = msg.Record.GetAgentId();
                    agentState = msg.Record.GetAgentState();
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        runtime->SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        TVector<TActorId> agentActors;
        for (ui32 i = 0; i < agents.size(); ++i) {
            agentActors.push_back(runtime->AllocateEdgeActor(i));
        }

        auto registerAgent = [&] (ui32 agentNo) {
            auto pipe = runtime->ConnectToPipe(
                TestTabletId,
                agentActors[agentNo],
                agentNo,
                NKikimr::GetPipeConfigWithRetries());

            auto request =
                std::make_unique<TEvDiskRegistry::TEvRegisterAgentRequest>();
            auto& requestConfig = *request->Record.MutableAgentConfig();
            requestConfig = agents[agentNo];
            requestConfig.SetNodeId(runtime->GetNodeId(agentNo));

            auto pipeEv = new IEventHandle(
                pipe,
                agentActors[agentNo],
                request.release(),
                0,
                0);
            pipeEv->Rewrite(NKikimr::TEvTabletPipe::EvSend, pipe);

            runtime->Send(pipeEv, agentNo, true);
            runtime->DispatchEvents({}, 10ms);

            return pipe;
        };

        auto disconnectAgent = [&](TActorId pipe, ui32 agentNo) {
            runtime->Send(new IEventHandle(
                pipe,
                agentActors[agentNo],
                new TEvTabletPipe::TEvShutdown()), agentNo, true);
            runtime->DispatchEvents({}, 10ms);
        };

        auto pipe = registerAgent(0);
        disconnectAgent(pipe, 0);

        runtime->AdvanceCurrentTime(6s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_C(agentId.empty(), agentId);

        runtime->AdvanceCurrentTime(4s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(agents[0].GetAgentId(), agentId);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::AGENT_STATE_UNAVAILABLE),
            static_cast<ui32>(agentState));

        agentId = "";
        agentState = NProto::AGENT_STATE_ONLINE;

        pipe = registerAgent(1);

        disconnectAgent(pipe, 1);

        runtime->AdvanceCurrentTime(11s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(TString(), agentId);

        runtime->AdvanceCurrentTime(9s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(agents[1].GetAgentId(), agentId);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::AGENT_STATE_UNAVAILABLE),
            static_cast<ui32>(agentState));

        agentId = "";
        agentState = NProto::AGENT_STATE_ONLINE;

        pipe = registerAgent(2);

        runtime->AdvanceCurrentTime(200s);

        disconnectAgent(pipe, 2);

        // disconnect timeout should've been completely recovered
        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(agents[2].GetAgentId(), agentId);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::AGENT_STATE_UNAVAILABLE),
            static_cast<ui32>(agentState));

        {
            TDiskRegistryClient diskRegistry(*runtime);
            diskRegistry.WaitReady();
            diskRegistry.SetWritableState(true);

            agentId = "";
            agentState = NProto::AGENT_STATE_ONLINE;

            pipe = registerAgent(3);

            runtime->AdvanceCurrentTime(200s);

            diskRegistry.UpdateDiskRegistryAgentListParams(
                TVector<TString>{agents[3].GetAgentId()},
                100s,
                500s,
                1000s);
            runtime->DispatchEvents({}, 10ms);
            runtime->AdvanceCurrentTime(1s);
            // diskRegistry.RebootTablet();
            // diskRegistry.WaitReady();

            disconnectAgent(pipe, 3);

            runtime->AdvanceCurrentTime(6s);
            runtime->DispatchEvents({}, 10ms);

            UNIT_ASSERT_VALUES_EQUAL(TString(), agentId);

            runtime->AdvanceCurrentTime(5s);
            runtime->DispatchEvents({}, 10ms);

            UNIT_ASSERT_VALUES_EQUAL(TString(), agentId);

            runtime->AdvanceCurrentTime(90s);
            runtime->DispatchEvents({}, 10ms);

            UNIT_ASSERT_VALUES_EQUAL(agents[3].GetAgentId(), agentId);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::AGENT_STATE_UNAVAILABLE),
                static_cast<ui32>(agentState));

            runtime->AdvanceCurrentTime(1000s);
            runtime->DispatchEvents({}, 10ms);

            agentId = "";
            agentState = NProto::AGENT_STATE_ONLINE;

            pipe = registerAgent(4);

            runtime->AdvanceCurrentTime(200s);
            runtime->DispatchEvents({}, 10ms);

            disconnectAgent(pipe, 4);

            runtime->AdvanceCurrentTime(6s);
            runtime->DispatchEvents({}, 10ms);

            UNIT_ASSERT_VALUES_EQUAL(TString(), agentId);

            runtime->AdvanceCurrentTime(10s);
            runtime->DispatchEvents({}, 10ms);

            UNIT_ASSERT_VALUES_EQUAL(agents[4].GetAgentId(), agentId);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::AGENT_STATE_UNAVAILABLE),
                static_cast<ui32>(agentState));
        }
    }

    Y_UNIT_TEST(ShouldRejectDisconnectedAgentAfterTransitionFromReadOnlyState)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent }));

        TString agentId;
        NProto::EAgentState agentState = NProto::AGENT_STATE_ONLINE;

        runtime->SetObserverFunc( [&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistry::EvChangeAgentStateRequest: {
                    auto& msg = *event->Get<TEvDiskRegistry::TEvChangeAgentStateRequest>();
                    agentId = msg.Record.GetAgentId();
                    agentState = msg.Record.GetAgentState();
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        auto sender = runtime->AllocateEdgeActor(0);

        auto registerAgent = [&] {
            auto pipe = runtime->ConnectToPipe(
                TestTabletId,
                sender,
                0,
                NKikimr::GetPipeConfigWithRetries());

            auto request = std::make_unique<TEvDiskRegistry::TEvRegisterAgentRequest>();
            *request->Record.MutableAgentConfig() = agent;
            request->Record.MutableAgentConfig()->SetNodeId(runtime->GetNodeId(0));

            auto pipeEv = new IEventHandle(pipe, sender, request.release(), 0, 0);
            pipeEv->Rewrite(NKikimr::TEvTabletPipe::EvSend, pipe);

            runtime->Send(pipeEv, 0, true);

            return pipe;
        };

        auto disconnectAgent = [&](auto pipe) {
            runtime->Send(new IEventHandle(
                pipe, sender, new TEvTabletPipe::TEvShutdown()), 0, true);
        };

        auto pipe = registerAgent();
        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        diskRegistry.SetWritableState(false);

        disconnectAgent(pipe);

        // 1st dispatch is needed to trigger EvServerDisconnected
        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        // 2nd dispatch is needed to trigger EvAgentConnectionLost
        runtime->AdvanceCurrentTime(60s);
        runtime->DispatchEvents({}, 10ms);

        diskRegistry.SetWritableState(true);

        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL("agent-1", agentId);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::AGENT_STATE_UNAVAILABLE),
            static_cast<int>(agentState));
    }

    Y_UNIT_TEST(ShouldFailCleanupIfDiskRegistryRestarts)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        TSSProxyClient ss(*runtime);

        ss.CreateVolume("vol");
        ss.CreateVolume("nonrepl-vol");
        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB);
        diskRegistry.AllocateDisk("nonrepl-garbage", 10_GB);

        UNIT_ASSERT(diskRegistry.Exists("nonrepl-vol"));
        UNIT_ASSERT(diskRegistry.Exists("nonrepl-garbage"));

        diskRegistry.MarkDiskForCleanup("nonrepl-garbage");

        runtime->SetObserverFunc( [&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeVolumeRequest: {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.SendCleanupDisksRequest();

        runtime->DispatchEvents({}, 10ms);
        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        auto response = diskRegistry.RecvCleanupDisksResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            response->GetStatus(),
            E_REJECTED);
    }

    Y_UNIT_TEST(ShouldNotReuseBrokenDevice)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent1 }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", msg.GetDevices(1).GetDeviceUUID());
        }

        diskRegistry.ChangeDeviceState("uuid-1.1", NProto::DEVICE_STATE_WARNING);
        diskRegistry.MarkDiskForCleanup("disk-1");
        diskRegistry.DeallocateDisk("disk-1");

        // wait for cleanup devices
        {
            TDispatchOptions options;
            options.FinalEvents = {
                TDispatchOptions::TFinalEventCondition(
                    TEvDiskRegistryPrivate::EvCleanupDevicesResponse)
            };

            runtime->DispatchEvents(options);
        }

        {
            diskRegistry.SendAllocateDiskRequest("disk-2", 20_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_DISK_ALLOCATION_FAILED);
        }
    }

    Y_UNIT_TEST(ShouldPublishDiskState)
    {
        TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB),
                Device("dev-3", "uuid-1.3", "rack-1", 10_GB)
            })
        };

        NProto::TStorageServiceConfig config;
        // disable recycling
        config.SetNonReplicatedDiskRecyclingPeriod(Max<ui32>());
        config.SetAllocationUnitNonReplicatedSSD(10);

        auto logbrokerService = std::make_shared<TTestLogbrokerService>();

        auto runtime = TTestRuntimeBuilder()
            .With(config)
            .With(logbrokerService)
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("vol0", 30_GB);

        UNIT_ASSERT_VALUES_EQUAL(0, logbrokerService->GetItemCount());

        auto removeHost = [&] (auto host) {
            NProto::TAction action;
            action.SetHost(host);
            action.SetType(NProto::TAction::REMOVE_HOST);

            diskRegistry.CmsAction(TVector<NProto::TAction>{ action });
        };

        auto waitForItems = [&] {
            // speed up test
            runtime->AdvanceCurrentTime(10s);

            if (logbrokerService->GetItemCount()) {
                return;
            }

            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return logbrokerService->GetItemCount();
            };

            runtime->DispatchEvents(options);
        };

        // warning
        removeHost("agent-1");
        waitForItems();

        ui64 maxSeqNo = 0;
        {
            auto items = logbrokerService->ExtractItems();
            UNIT_ASSERT_VALUES_EQUAL(1, items.size());

            auto& item = items[0];
            maxSeqNo = item.SeqNo;

            UNIT_ASSERT_VALUES_EQUAL("vol0", item.DiskId);
            UNIT_ASSERT_EQUAL(
                NProto::DISK_STATE_ONLINE, // migration
                item.State);
            UNIT_ASSERT_VALUES_UNEQUAL("", item.Message);
        }

        diskRegistry.ChangeDeviceState("uuid-1.2", NProto::DEVICE_STATE_ERROR);
        waitForItems();

        {
            auto items = logbrokerService->ExtractItems();
            UNIT_ASSERT_VALUES_EQUAL(1, items.size());
            auto& item = items[0];

            UNIT_ASSERT_LT(maxSeqNo, item.SeqNo);
            maxSeqNo = item.SeqNo;

            UNIT_ASSERT_VALUES_EQUAL("vol0", item.DiskId);
            UNIT_ASSERT_EQUAL(
                NProto::DISK_STATE_ERROR,
                item.State);
            UNIT_ASSERT_VALUES_EQUAL("", item.Message);
        }

        // unavailable
        runtime->AdvanceCurrentTime(24h);
        removeHost("agent-1");
        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        // waste some time
        runtime->AdvanceCurrentTime(1min);
        runtime->DispatchEvents(TDispatchOptions(), 10ms);

        // vol0 already broken
        UNIT_ASSERT_VALUES_EQUAL(0, logbrokerService->GetItemCount());
    }

    Y_UNIT_TEST(ShouldRestoreCountersAfterReboot)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({agent1})
            .With([] {
                auto config = CreateDefaultStorageConfig();
                config.SetDiskRegistryCountersHost("test");

                return config;
            }())
            .Build();

        auto getOnlineDisks = [&] {
            return runtime->GetAppData(0).Counters
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "disk_registry")
                ->GetSubgroup("host", "test")
                ->GetCounter("DisksInOnlineState")
                ->Val();
        };

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        UNIT_ASSERT_VALUES_EQUAL(0, getOnlineDisks());

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        diskRegistry.AllocateDisk("disk-1", 10_GB);

        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(TDispatchOptions(), 10ms);

        UNIT_ASSERT_VALUES_EQUAL(1, getOnlineDisks());

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(1, getOnlineDisks());
    }

    Y_UNIT_TEST(ShouldSkipSecureEraseForUnavailableDevices)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
                Device("dev-3", "uuid-3", "rack-1", 10_GB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-4", "rack-2", 10_GB),
                Device("dev-2", "uuid-5", "rack-2", 10_GB),
                Device("dev-3", "uuid-6", "rack-2", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAgents(*runtime, agents.size());
        WaitForAgents(*runtime, agents.size());
        WaitForSecureErase(*runtime, agents);

        diskRegistry.AllocateDisk("disk-1", 60_GB);

        diskRegistry.ChangeDeviceState("uuid-2", NProto::DEVICE_STATE_ERROR);
        diskRegistry.ChangeAgentState("agent-2", NProto::AGENT_STATE_UNAVAILABLE);

        TVector<TString> toErase;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvSecureEraseRequest: {
                    auto& msg = *event->Get<TEvDiskRegistryPrivate::TEvSecureEraseRequest>();
                    for (auto& d: msg.DirtyDevices) {
                        toErase.push_back(d.GetDeviceUUID());
                    }
                    break;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        auto waitForErase = [&] {
            runtime->AdvanceCurrentTime(5s);
            TDispatchOptions options;
            options.FinalEvents = {
                TDispatchOptions::TFinalEventCondition(
                    TEvDiskRegistryPrivate::EvSecureEraseRequest)
            };
            runtime->DispatchEvents(options);
            TVector<TString> tmp;
            std::swap(toErase, tmp);
            Sort(tmp);
            return tmp;
        };

        diskRegistry.MarkDiskForCleanup("disk-1");
        diskRegistry.DeallocateDisk("disk-1");

        {
            auto uuids = waitForErase();
            UNIT_ASSERT_VALUES_EQUAL(2, uuids.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", uuids[0]);
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", uuids[1]);
        }

        diskRegistry.ChangeAgentState("agent-2", NProto::EAgentState::AGENT_STATE_ONLINE);
        {
            auto uuids = waitForErase();
            UNIT_ASSERT_VALUES_EQUAL(3, uuids.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", uuids[0]);
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", uuids[1]);
            UNIT_ASSERT_VALUES_EQUAL("uuid-6", uuids[2]);
        }

        diskRegistry.ChangeDeviceState("uuid-2", NProto::DEVICE_STATE_ONLINE);
        {
            auto uuids = waitForErase();
            UNIT_ASSERT_VALUES_EQUAL(1, uuids.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", uuids[0]);
        }
    }

    Y_UNIT_TEST(ShouldDetectBrokenDevices)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
            Device("dev-3", "uuid-3", "rack-1", 10_GB),
            Device("dev-4", "uuid-4", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);

        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        TSet<TString> diskDevices;
        TSet<TString> freeDevices { "uuid-1", "uuid-2", "uuid-3", "uuid-4" };

        {
            auto response = diskRegistry.AllocateDisk("vol0", 20_GB);
            auto& r = response->Record;
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, r.GetIOMode());
            UNIT_ASSERT_UNEQUAL(0, r.GetIOModeTs());
            UNIT_ASSERT_VALUES_EQUAL(2, r.DevicesSize());
            for (const auto& d: r.GetDevices()) {
                diskDevices.insert(d.GetDeviceUUID());
                freeDevices.erase(d.GetDeviceUUID());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(2, diskDevices.size());
        UNIT_ASSERT_VALUES_EQUAL(2, freeDevices.size());

        {
            NProto::TAgentStats agentStats;
            agentStats.SetNodeId(runtime->GetNodeId(0));
            auto& device1 = *agentStats.AddDeviceStats();
            device1.SetDeviceUUID(*diskDevices.begin());
            device1.SetErrors(1); // break the disk
            auto& device2 = *agentStats.AddDeviceStats();
            device2.SetDeviceUUID(*std::next(diskDevices.begin()));
            device2.SetErrors(0);
            auto& device3 = *agentStats.AddDeviceStats();
            device3.SetDeviceUUID(*freeDevices.begin());
            device3.SetErrors(1);
            auto& device4 = *agentStats.AddDeviceStats();
            device4.SetDeviceUUID(*std::next(freeDevices.begin()));
            device4.SetErrors(0);
            diskRegistry.UpdateAgentStats(std::move(agentStats));
        }

        runtime->DispatchEvents(TDispatchOptions(), 1s);
        {
            // can't allocate disk with two devices
            diskRegistry.SendAllocateDiskRequest("vol1", 20_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_DISK_ALLOCATION_FAILED);
        }

        diskRegistry.AllocateDisk("vol1", 10_GB);
        {
            diskRegistry.SendAllocateDiskRequest("vol3", 10_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_DISK_ALLOCATION_FAILED);
        }

        {
            auto response = diskRegistry.DescribeDisk("vol0");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, r.DevicesSize());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, r.GetState());
        }
    }

    Y_UNIT_TEST(ShouldCountPublishDiskStateErrors)
    {
        TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
            }),
        };

        auto logbrokerService = std::make_shared<TTestLogbrokerService>(
            MakeError(E_FAIL, "Test")
        );

        auto runtime = TTestRuntimeBuilder()
            .With(logbrokerService)
            .WithAgents(agents)
            .Build();

        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto publishDiskStateError =
            counters->GetCounter("AppCriticalEvents/PublishDiskStateError", true);

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("disk-1", 10_GB);

        UNIT_ASSERT_VALUES_EQUAL(0, logbrokerService->GetItemCount());
        UNIT_ASSERT_VALUES_EQUAL(0, publishDiskStateError->Val());

        {
            NProto::TAction action;
            action.SetHost("agent-1");
            action.SetType(NProto::TAction::REMOVE_HOST);

            diskRegistry.CmsAction(TVector{ action });
        }

        // speed up test
        runtime->AdvanceCurrentTime(10s);

        if (!logbrokerService->GetItemCount()) {

            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return logbrokerService->GetItemCount();
            };

            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_UNEQUAL(0, logbrokerService->GetItemCount());

        UNIT_ASSERT_VALUES_EQUAL(
            logbrokerService->GetItemCount(),
            publishDiskStateError->Val()
        );
    }

    Y_UNIT_TEST(ShouldRegisterUnavailableAgentAfterTimeout)
    {
        auto defaultConfig = CreateDefaultStorageConfig();

        auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });
        agent.SetSeqNumber(1);
        agent.SetNodeId(42);

        auto runtime = TTestRuntimeBuilder()
            .With(defaultConfig)
            .Build();

        {
            TDiskRegistryClient diskRegistry(*runtime);
            diskRegistry.WaitReady();
            diskRegistry.SetWritableState(true);

            diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent }));

            diskRegistry.SendRegisterAgentRequest(agent);
            auto response = diskRegistry.RecvRegisterAgentResponse();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        }
        runtime->DispatchEvents({}, 10ms);

        runtime->AdvanceCurrentTime(TDuration::MilliSeconds(
            defaultConfig.GetNonReplicatedAgentMinTimeout() / 2));
        runtime->DispatchEvents({}, 10ms);

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        agent.SetSeqNumber(0);
        agent.SetNodeId(100);

        {
            diskRegistry.SendRegisterAgentRequest(agent);
            auto response = diskRegistry.RecvRegisterAgentResponse();
            UNIT_ASSERT_C(FAILED(response->GetStatus()), response->GetErrorReason());
        }
        runtime->DispatchEvents({}, 10ms);

        runtime->AdvanceCurrentTime(TDuration::MilliSeconds(
            defaultConfig.GetNonReplicatedAgentMinTimeout() / 2));
        runtime->DispatchEvents({}, 10ms);

        {
            diskRegistry.SendRegisterAgentRequest(agent);
            auto response = diskRegistry.RecvRegisterAgentResponse();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldNotUnregisterNewAgentAfterKillingPreviousAgent)
    {
        auto defaultConfig = CreateDefaultStorageConfig();

        auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });
        agent.SetSeqNumber(0);
        agent.SetNodeId(42);

        auto runtime = TTestRuntimeBuilder()
            .With(defaultConfig)
            .Build();

        auto diskRegistry0 = std::make_unique<TDiskRegistryClient>(*runtime);
        diskRegistry0->WaitReady();
        diskRegistry0->SetWritableState(true);

        diskRegistry0->UpdateConfig(CreateRegistryConfig(0, { agent }));

        {
            diskRegistry0->SendRegisterAgentRequest(agent);
            auto response = diskRegistry0->RecvRegisterAgentResponse();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        }
        runtime->DispatchEvents({}, 10ms);

        TDiskRegistryClient diskRegistry1(*runtime);
        diskRegistry1.WaitReady();
        agent.SetSeqNumber(1);
        agent.SetNodeId(100);

        {
            diskRegistry1.SendRegisterAgentRequest(agent);
            auto response = diskRegistry1.RecvRegisterAgentResponse();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        }
        runtime->DispatchEvents({}, 10ms);

        diskRegistry0.reset();
        runtime->DispatchEvents({}, 10ms);

        runtime->AdvanceCurrentTime(TDuration::MilliSeconds(
            defaultConfig.GetNonReplicatedAgentMaxTimeout()));
        runtime->DispatchEvents({}, 10ms);

        agent.SetSeqNumber(0);
        agent.SetNodeId(42);
        {
            TDiskRegistryClient diskRegistry(*runtime);
            diskRegistry.WaitReady();
            diskRegistry.SendRegisterAgentRequest(agent);
            auto response = diskRegistry.RecvRegisterAgentResponse();
            UNIT_ASSERT_C(FAILED(response->GetStatus()), response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldKillPreviousAgentAfterRegisteringNewAgent)
    {
        auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });
        agent.SetSeqNumber(0);
        agent.SetNodeId(42);

        auto runtime = TTestRuntimeBuilder()
            .Build();

        size_t poisonPillCount = 0;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvents::TSystem::PoisonPill:
                    ++poisonPillCount;
                    break;
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TDiskRegistryClient diskRegistry0(*runtime);
        diskRegistry0.WaitReady();
        diskRegistry0.SetWritableState(true);

        diskRegistry0.UpdateConfig(CreateRegistryConfig(0, { agent }));

        {
            diskRegistry0.SendRegisterAgentRequest(agent);
            auto response = diskRegistry0.RecvRegisterAgentResponse();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        }
        runtime->DispatchEvents({}, 10ms);

        TDiskRegistryClient diskRegistry1(*runtime);
        diskRegistry1.WaitReady();
        agent.SetSeqNumber(1);
        agent.SetNodeId(100);

        {
            poisonPillCount = 0;
            diskRegistry1.SendRegisterAgentRequest(agent);
            auto response = diskRegistry1.RecvRegisterAgentResponse();
            UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(1, poisonPillCount);
        }
    }

    Y_UNIT_TEST(ShouldMuteIOErrorsForTempUnavailableDisk)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-3", "uuid-3", "rack-1", 10_GB),
                Device("dev-4", "uuid-4", "rack-1", 10_GB)
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

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            EXPECT_DISK_STATE(response, 4, NProto::VOLUME_IO_OK, false);
        }

        diskRegistry.ChangeAgentState(
            agents[0].GetAgentId(),
            NProto::AGENT_STATE_UNAVAILABLE);

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            EXPECT_DISK_STATE(response, 4, NProto::VOLUME_IO_OK, true);
        }
    }

    Y_UNIT_TEST(ShouldSwitchToReadOnlyOnAgentFailure)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-3", "uuid-3", "rack-1", 10_GB),
                Device("dev-4", "uuid-4", "rack-1", 10_GB)
            })
        };

        auto config = CreateDefaultStorageConfig();
        config.SetNonReplicatedDiskSwitchToReadOnlyTimeout(
            TDuration{5s}.MilliSeconds());
        auto runtime = TTestRuntimeBuilder()
            .With(config)
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        TMaybe<TActorId> sender, recipient, serverId;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                const auto eventType = event->GetTypeRewrite();
                if (!sender && eventType == TEvTabletPipe::EvServerConnected) {
                    sender = event->Sender;
                    recipient = event->Recipient;
                    const auto& msg =
                        *event->Get<TEvTabletPipe::TEvServerConnected>();
                    serverId = msg.ServerId;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto disconnectServer = [&] {
            auto event = new TEvTabletPipe::TEvServerDisconnected(
                TestTabletId,
                *serverId,
                *serverId);
            runtime->Send(new IEventHandle(*recipient, *sender, event), 0, true);
        };

        RegisterAndWaitForAgents(*runtime, agents);
        UNIT_ASSERT(sender && recipient && serverId);

        {
            const auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            EXPECT_DISK_STATE(response, 4, NProto::VOLUME_IO_OK, false);
        }

        {
            disconnectServer();
            runtime->AdvanceCurrentTime(5s);
            runtime->DispatchEvents(
            {.FinalEvents = {TDispatchOptions::TFinalEventCondition(
                 TEvDiskRegistryPrivate::
                     EvSwitchAgentDisksToReadOnlyResponse)}});

            auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            EXPECT_DISK_STATE(
                response,
                4,
                NProto::VOLUME_IO_ERROR_READ_ONLY,
                true);
        }

        {
            RegisterAgent(*runtime, 0);
            runtime->DispatchEvents({}, 100ms);

            auto response = diskRegistry.AllocateDisk("disk-1", 40_GB);
            EXPECT_DISK_STATE(response, 4, NProto::VOLUME_IO_OK, false);
        }
    }

    Y_UNIT_TEST(ShouldSwitchToReadOnlyOnAgentFailureAfterTabletReboot)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            })
        };

        auto config = CreateDefaultStorageConfig();
        config.SetNonReplicatedDiskSwitchToReadOnlyTimeout(
            TDuration{5s}.MilliSeconds());
        config.SetDiskRegistryInitialAgentRejectionThreshold(100.0);

        auto runtime = TTestRuntimeBuilder()
            .With(config)
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        {
            const auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            EXPECT_DISK_STATE(response, 2, NProto::VOLUME_IO_OK, false);
        }

        diskRegistry.RebootTablet();

        runtime->EnableScheduleForActor(
            NKikimr::ResolveTablet(*runtime, TestTabletId));

        diskRegistry.WaitReady();

        runtime->AdvanceCurrentTime(5s);

        runtime->DispatchEvents(
            {.FinalEvents = {TDispatchOptions::TFinalEventCondition(
                 TEvDiskRegistryPrivate::
                     EvSwitchAgentDisksToReadOnlyResponse)}});

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            EXPECT_DISK_STATE(
                response,
                2,
                NProto::VOLUME_IO_ERROR_READ_ONLY,
                true);
        }
    }

    Y_UNIT_TEST(ShouldFailToUpdateAgentListParamsWithIncorrectAgentIds)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent }));

        {
            diskRegistry.SendUpdateDiskRegistryAgentListParamsRequest(
                TVector<TString>{},
                100s,
                500s,
                1000s);
            auto response =
                diskRegistry.RecvUpdateDiskRegistryAgentListParamsResponse();

            UNIT_ASSERT(response->Record.HasError());
            UNIT_ASSERT_EQUAL(E_ARGUMENT, response->Record.GetError().GetCode());
        }
        {
            diskRegistry.SendUpdateDiskRegistryAgentListParamsRequest(
                TVector<TString>{"nonexistent-agent"},
                100s,
                500s,
                1000s);
            auto response =
                diskRegistry.RecvUpdateDiskRegistryAgentListParamsResponse();

            UNIT_ASSERT(response->Record.HasError());
            UNIT_ASSERT_EQUAL(E_NOT_FOUND, response->Record.GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldSecureEraseDevicesFromDifferentPools)
    {
        // 1 .Create devices from two different pools with pool kind
        // DEVICE_POOL_KIND_LOCAL. Devices are created in the suspended state.
        // 2. Set observer for EvCleanupDevicesRequest which check that
        // all devices from one message belong to the same pool.
        // 3. Call ResumeDevice for all devices. Internally ResumeDevice
        // triggers SecureErase.

        const TString poolName1 = "pool-1";
        const auto withPool1 =
            WithPool(poolName1, NProto::DEVICE_POOL_KIND_GLOBAL);
        auto agent1 = CreateAgentConfig(
            "agent-1",
            {Device("dev-1", "uuid-1", "rack-1", 10_GB) | withPool1,
             Device("dev-2", "uuid-2", "rack-1", 10_GB) | withPool1,
             Device("dev-3", "uuid-3", "rack-1", 10_GB) | withPool1});

        const TString poolName2 = "pool-2";
        const auto withPool2 =
            WithPool(poolName2, NProto::DEVICE_POOL_KIND_GLOBAL);
        auto agent2 = CreateAgentConfig(
            "agent-2",
            {Device("dev-4", "uuid-4", "rack-1", 10_GB) | withPool2,
             Device("dev-5", "uuid-5", "rack-1", 10_GB) | withPool2,
             Device("dev-6", "uuid-6", "rack-1", 10_GB) | withPool2});

        auto runtime =
            TTestRuntimeBuilder().WithAgents({agent1, agent2}).Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            [&]
            {
                auto config = CreateRegistryConfig(0, {agent1, agent2});

                auto* pool1 = config.AddDevicePoolConfigs();
                pool1->SetName(poolName1);
                pool1->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                pool1->SetAllocationUnit(10_GB);

                auto* pool2 = config.AddDevicePoolConfigs();
                pool2->SetName(poolName2);
                pool2->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                pool2->SetAllocationUnit(10_GB);

                return config;
            }());

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);

        THashMap<TString, TString> deviceUuidToPoolName = {
            {"uuid-1", poolName1},
            {"uuid-2", poolName1},
            {"uuid-3", poolName1},
            {"uuid-4", poolName2},
            {"uuid-5", poolName2},
            {"uuid-6", poolName2},
        };

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistryPrivate::EvCleanupDevicesRequest: {
                        auto& msg = *event->Get<
                            TEvDiskRegistryPrivate::TEvCleanupDevicesRequest>();
                        TSet<TString> poolNameSet;
                        for (auto& device: msg.Devices) {
                            poolNameSet.insert(deviceUuidToPoolName[device]);
                        }
                        // each SecureErase actor handles devices only from one
                        // pool
                        UNIT_ASSERT_EQUAL(1, poolNameSet.size());
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        diskRegistry.ResumeDevice("agent-1", "dev-1", /*dryRun=*/false);
        diskRegistry.ResumeDevice("agent-2", "dev-4", /*dryRun=*/false);
        diskRegistry.ResumeDevice("agent-1", "dev-2", /*dryRun=*/false);
        diskRegistry.ResumeDevice("agent-2", "dev-5", /*dryRun=*/false);
        diskRegistry.ResumeDevice("agent-1", "dev-3", /*dryRun=*/false);
        diskRegistry.ResumeDevice("agent-2", "dev-6", /*dryRun=*/false);
        WaitForSecureErase(*runtime, 6);
    }

    Y_UNIT_TEST(ShouldSecureEraseDevicesFromDifferentPoolsIndependently)
    {
        const TString defaultPool = "";
        const TString rotPool = "rot";
        const auto withRotPool =
            WithPool(rotPool, NProto::DEVICE_POOL_KIND_GLOBAL);

        TVector agents{
            CreateAgentConfig(
                "agent-1",
                {Device("path", "uuid-1", "rack-1", 10_GB) | withRotPool,
                 Device("path", "uuid-2", "rack-1", 10_GB) | withRotPool,
                 Device("path", "uuid-3", "rack-1", 10_GB) | withRotPool}),
            CreateAgentConfig(
                "agent-2",
                {Device("path", "uuid-4", "rack-1", 10_GB),
                 Device("path", "uuid-5", "rack-1", 10_GB),
                 Device("path", "uuid-6", "rack-1", 10_GB)})};

        THashMap<TString, TString> deviceUuidToPoolName = {
            {"uuid-1", rotPool},
            {"uuid-2", rotPool},
            {"uuid-3", rotPool},
            {"uuid-4", defaultPool},
            {"uuid-5", defaultPool},
            {"uuid-6", defaultPool},
        };

        auto runtime =
            TTestRuntimeBuilder()
                .With(
                    []
                    {
                        auto config = CreateDefaultStorageConfig();

                        config
                            .SetMaxDevicesToErasePerDeviceNameForDefaultPoolKind(
                                1);
                        config
                            .SetMaxDevicesToErasePerDeviceNameForGlobalPoolKind(
                                1);

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

                auto* rot = config.AddDevicePoolConfigs();
                rot->SetName(rotPool);
                rot->SetKind(NProto::DEVICE_POOL_KIND_GLOBAL);
                rot->SetAllocationUnit(10_GB);

                return config;
            }());

        TVector<TAutoPtr<IEventHandle>> secureEraseRequests;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskAgent::EvSecureEraseDeviceRequest: {
                        secureEraseRequests.emplace_back(std::move(event));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);

        runtime->DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvSecureEraseDeviceRequest, 2}}},
            15s);
        UNIT_ASSERT_VALUES_EQUAL(2, secureEraseRequests.size());

        auto getPoolName = [&](const auto& event)
        {
            const auto& msg = *event->template Get<
                TEvDiskAgent::TEvSecureEraseDeviceRequest>();

            return deviceUuidToPoolName[msg.Record.GetDeviceUUID()];
        };

        SortBy(secureEraseRequests, getPoolName);

        UNIT_ASSERT_VALUES_EQUAL(
            defaultPool,
            getPoolName(secureEraseRequests[0]));
        UNIT_ASSERT_VALUES_EQUAL(rotPool, getPoolName(secureEraseRequests[1]));

        runtime->DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvSecureEraseDeviceRequest}}},
            15s);
        UNIT_ASSERT_VALUES_EQUAL(2, secureEraseRequests.size());

        auto sendResponse = [&](auto event)
        {
            runtime->Send(new IEventHandle(
                event->Sender,
                event->Recipient,
                new TEvDiskAgent::TEvSecureEraseDeviceResponse(),
                0,   // flags
                event->Cookie));
        };

        // send the first response for the device from the default pool
        sendResponse(std::move(secureEraseRequests[0]));

        runtime->DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvSecureEraseDeviceRequest}}},
            15s);
        UNIT_ASSERT_VALUES_EQUAL(3, secureEraseRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            defaultPool,
            getPoolName(secureEraseRequests[2]));

        runtime->DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvSecureEraseDeviceRequest}}},
            15s);
        UNIT_ASSERT_VALUES_EQUAL(3, secureEraseRequests.size());

        // send the second response for the device from the default pool
        sendResponse(std::move(secureEraseRequests[2]));

        runtime->DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvSecureEraseDeviceRequest}}},
            15s);
        UNIT_ASSERT_VALUES_EQUAL(4, secureEraseRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            defaultPool,
            getPoolName(secureEraseRequests[3]));

        // send the third response for the device from the default pool
        sendResponse(std::move(secureEraseRequests[3]));
        runtime->DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvSecureEraseDeviceRequest}}},
            15s);

        // there are no new requests - all devices from the default pool are
        // clean
        UNIT_ASSERT_VALUES_EQUAL(4, secureEraseRequests.size());

        // send the response for the device from the rot pool
        sendResponse(std::move(secureEraseRequests[1]));
        runtime->DispatchEvents(
            {.FinalEvents = {{TEvDiskAgent::EvSecureEraseDeviceRequest}}},
            15s);
        UNIT_ASSERT_VALUES_EQUAL(5, secureEraseRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(rotPool, getPoolName(secureEraseRequests[4]));
    }

    Y_UNIT_TEST(ShouldDisableIOForBrokenDevices)
    {
        auto agentConfig = CreateAgentConfig("agent-1", {
            CreateDeviceConfig({.Name = "NVMENBS01", .Id = "uuid-1", .SerialNum = "W"}),
            CreateDeviceConfig({.Name = "NVMENBS02", .Id = "uuid-2", .SerialNum = "X"}),
            CreateDeviceConfig({.Name = "NVMENBS03", .Id = "uuid-3", .SerialNum = "Y"}),
            CreateDeviceConfig({.Name = "NVMENBS04", .Id = "uuid-4", .SerialNum = "Z"}),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agentConfig })
            .Build();

        int registrationCount = 0;

        runtime->SetEventFilter([&](auto&, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvRegisterAgentResponse)
            {
                ++registrationCount;

                auto& msg =
                    *event->Get<TEvDiskRegistry::TEvRegisterAgentResponse>();

                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    msg.Record.DevicesToDisableIOSize());
            }

            return false;
        });

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agentConfig}));

        UNIT_ASSERT_VALUES_EQUAL(0, registrationCount);

        RegisterAgents(*runtime, 1);

        UNIT_ASSERT_VALUES_EQUAL(1, registrationCount);

        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agentConfig});

        // Allocate a disk on two devices.
        diskRegistry.AllocateDisk("vol0", 20_GB, DefaultLogicalBlockSize);

        THashSet<TString> allocatedDevices;
        {
            auto response = diskRegistry.AllocateDisk(
                "vol0",
                20_GB,
                DefaultLogicalBlockSize);
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, r.DevicesSize());
            allocatedDevices = {
                r.GetDevices(0).GetDeviceUUID(),
                r.GetDevices(1).GetDeviceUUID()};
        }

        // change the serial number of an allocated device
        auto* x = FindIfPtr(
            *agentConfig.MutableDevices(),
            [&](const auto& d)
            { return allocatedDevices.contains(d.GetDeviceUUID()); });
        UNIT_ASSERT(x);
        x->SetSerialNumber("A");

        // change the serial number of an unallocated device
        auto* y = FindIfPtr(
            *agentConfig.MutableDevices(),
            [&](const auto& d)
            { return !allocatedDevices.contains(d.GetDeviceUUID()); });
        UNIT_ASSERT(y);
        y->SetSerialNumber("N");

        UNIT_ASSERT_VALUES_EQUAL(1, registrationCount);

        TVector<TString> devicesToSuspendIO;

        runtime->SetEventFilter([&](auto&, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvRegisterAgentRequest)
            {
                auto& msg =
                    *event->Get<TEvDiskRegistry::TEvRegisterAgentRequest>();

                // override the config to apply the SN changes
                msg.Record.MutableAgentConfig()->MutableDevices()->CopyFrom(
                    agentConfig.GetDevices());
            }

            if (event->GetTypeRewrite() ==
                TEvDiskRegistry::EvRegisterAgentResponse)
            {
                ++registrationCount;

                auto& msg =
                    *event->Get<TEvDiskRegistry::TEvRegisterAgentResponse>();

                devicesToSuspendIO.assign(
                    msg.Record.GetDevicesToDisableIO().begin(),
                    msg.Record.GetDevicesToDisableIO().end());
            }

            return false;
        });

        RegisterAgents(*runtime, 1);
        UNIT_ASSERT_VALUES_EQUAL(2, registrationCount);

        // DevicesToDisableIO should contain only the allocated device

        UNIT_ASSERT_VALUES_EQUAL(1, devicesToSuspendIO.size());
        UNIT_ASSERT_VALUES_EQUAL(x->GetDeviceUUID(), devicesToSuspendIO[0]);

        devicesToSuspendIO.clear();
        RegisterAgents(*runtime, 1);
        UNIT_ASSERT_VALUES_EQUAL(3, registrationCount);
        UNIT_ASSERT_VALUES_EQUAL(1, devicesToSuspendIO.size());
        UNIT_ASSERT_VALUES_EQUAL(x->GetDeviceUUID(), devicesToSuspendIO[0]);
    }

    Y_UNIT_TEST(ShouldRefrainFromRejectingBigPartOfTheCluster)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {Device("dev", "uuid-1")}),
            CreateAgentConfig("agent-2", {Device("dev", "uuid-2")}),
            CreateAgentConfig("agent-3", {Device("dev", "uuid-3")}),
            CreateAgentConfig("agent-4", {Device("dev", "uuid-4")}),
            CreateAgentConfig("agent-5", {Device("dev", "uuid-5")}),
            CreateAgentConfig("agent-6", {Device("dev", "uuid-6")}),
        };

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);

        auto crits = counters->GetCounter(
            "AppCriticalEvents/"
            "DiskRegistryInitialAgentRejectionThresholdExceeded",
            true);

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With([] {
                auto config = CreateDefaultStorageConfig();
                config.SetDiskRegistryCountersHost("test");

                config.SetNonReplicatedAgentMaxTimeout(50'000);           // 50s
                config.SetDiskRegistryInitialAgentRejectionThreshold(50); // 50%

                return config;
            }())
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        UNIT_ASSERT_VALUES_EQUAL(0, crits->Val());

        runtime->AdvanceCurrentTime(1h);

        UNIT_ASSERT_VALUES_EQUAL(0, crits->Val());

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        // Only 33% of agents are reconnected
        RegisterAgents(*runtime, 2);

        UNIT_ASSERT_VALUES_EQUAL(0, crits->Val());

        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(0, crits->Val());

        runtime->AdvanceCurrentTime(30s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(0, crits->Val());

        // NonReplicatedAgentMaxTimeout expired
        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(1, crits->Val());

        // DR can still allocate a disk
        diskRegistry.AllocateDisk("vol0", 60_GB);

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(1, crits->Val());

        runtime->AdvanceCurrentTime(50s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(2, crits->Val());
    }

    Y_UNIT_TEST(ShouldListDiskStates)
    {
        TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB),
                Device("dev-3", "uuid-1.3", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-2", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-2", 10_GB),
                Device("dev-3", "uuid-2.3", "rack-2", 10_GB)
            }),
            CreateAgentConfig("agent-3", {
                Device("dev-1", "uuid-3.1", "rack-3", 10_GB),
                Device("dev-2", "uuid-3.2", "rack-3", 10_GB),
                Device("dev-3", "uuid-3.3", "rack-3", 10_GB)
            })
        };

        NProto::TStorageServiceConfig config;
        // disable recycling
        config.SetNonReplicatedDiskRecyclingPeriod(Max<ui32>());
        config.SetAllocationUnitNonReplicatedSSD(10);

        auto runtime = TTestRuntimeBuilder()
            .With(config)
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        // allocate disks
        // agent1: mirror0, nrd0, nrd1
        // agent2: mirror0, nrd2, nrd3
        // agent3: mirror0, nrd4, nrd5

        {
            auto request =
                diskRegistry.CreateAllocateDiskRequest("mirror0", 10_GB);
            request->Record.SetReplicaCount(2);
            request->Record.SetStorageMediaKind(
                NProto::STORAGE_MEDIA_SSD_MIRROR3);

            diskRegistry.SendRequest(std::move(request));
        }

        {
            int index = 0;
            for (auto& agent: agents) {
                for (int i = 0; i != 2; ++i) {
                    auto request = diskRegistry.CreateAllocateDiskRequest(
                        "nrd" + ToString(index++),
                        10_GB);

                    *request->Record.AddAgentIds() = agent.GetAgentId();

                    diskRegistry.SendRequest(std::move(request));
                    auto response = diskRegistry.RecvAllocateDiskResponse();
                    auto& msg = response->Record;
                    UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
                }
            }
        }

        auto listDiskStates = [&] {
            auto response = diskRegistry.ListDiskStates();
            auto& states = *response->Record.MutableDiskStates();
            UNIT_ASSERT_VALUES_EQUAL(7, states.size());
            SortBy(states, [](const auto& s) { return s.GetDiskId(); });
            return states;
        };

        {
            auto states = listDiskStates();

            UNIT_ASSERT_VALUES_EQUAL("mirror0", states[0].GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, states[0].GetState());
            UNIT_ASSERT_VALUES_EQUAL("", states[0].GetStateMessage());

            for (int i = 0; i != 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    "nrd" + ToString(i),
                    states[i + 1].GetDiskId());
                UNIT_ASSERT_EQUAL(
                    NProto::DISK_STATE_ONLINE,
                    states[i + 1].GetState());
                UNIT_ASSERT_VALUES_EQUAL("", states[i + 1].GetStateMessage());
            }
        }

        // mirror0, nrd2, nrd3 -> online + message
        {
            NProto::TAction action;
            action.SetHost(agents[1].GetAgentId());
            action.SetType(NProto::TAction::REMOVE_HOST);

            diskRegistry.CmsAction(TVector<NProto::TAction>{action});
        }

        {
            auto states = listDiskStates();

            UNIT_ASSERT_VALUES_EQUAL("mirror0", states[0].GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, states[0].GetState());
            UNIT_ASSERT_VALUES_UNEQUAL("", states[0].GetStateMessage());

            for (int i = 0; i != 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    "nrd" + ToString(i),
                    states[i + 1].GetDiskId());
                UNIT_ASSERT_EQUAL(
                    NProto::DISK_STATE_ONLINE,
                    states[i + 1].GetState());

                if (i == 2 || i == 3) {
                    UNIT_ASSERT_VALUES_UNEQUAL(
                        "",
                        states[i + 1].GetStateMessage());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        "",
                        states[i + 1].GetStateMessage());
                }
            }
        }

        // nrd3, nrd4 -> error

        runtime->AdvanceCurrentTime(24h);
        diskRegistry.ChangeAgentState(
            agents[1].GetAgentId(),
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        {
            auto states = listDiskStates();

            UNIT_ASSERT_VALUES_EQUAL("mirror0", states[0].GetDiskId());
            // Mirror disk can only be in online state
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, states[0].GetState());
            UNIT_ASSERT_VALUES_EQUAL("", states[0].GetStateMessage());

            for (int i = 0; i != 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    "nrd" + ToString(i),
                    states[i + 1].GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL("", states[i + 1].GetStateMessage());

                if (i == 2 || i == 3) {
                    UNIT_ASSERT_EQUAL(
                        NProto::DISK_STATE_ERROR,
                        states[i + 1].GetState());
                } else {
                    UNIT_ASSERT_EQUAL(
                        NProto::DISK_STATE_ONLINE,
                        states[i + 1].GetState());
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
