#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NDiskRegistryTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 ExternalVolumeTabletId = 42;

auto AllocateDisk(
    TDiskRegistryClient& diskRegistry,
    const TString& diskId,
    ui64 externalVolumeTabletId,
    ui64 diskSize = 10_GB)
{
    auto request = diskRegistry.CreateAllocateDiskRequest(diskId, diskSize);
    request->Record.SetExternalVolumeTabletId(externalVolumeTabletId);
    diskRegistry.SendRequest(std::move(request));

    return diskRegistry.RecvAllocateDiskResponse();
}

std::unique_ptr<TDiskRegistryTestRuntime> CreateRuntime(
    const NProto::TAgentConfig& agent)
{
    auto runtime = TTestRuntimeBuilder().WithAgents({agent}).Build();

    TDiskRegistryClient diskRegistry(*runtime);
    diskRegistry.WaitReady();
    diskRegistry.SetWritableState(true);
    diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

    RegisterAgents(*runtime, 1);
    WaitForAgents(*runtime, 1);
    WaitForSecureErase(*runtime, {agent});

    return runtime;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryExternalVolumeTest)
{
    Y_UNIT_TEST(ShouldNotifyExternalVolumeTablet)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });
        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-2", "rack-2", 10_GB),
        });

        // The third node re-registers agent-1 and triggers reallocation.
        auto runtime = TTestRuntimeBuilder()
            .WithAgents({agent1, agent2, agent1})
            .Build();
        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));
        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        auto request = diskRegistry.CreateAllocateDiskRequest(
            "fs",
            10_GB,
            DefaultLogicalBlockSize,
            {},
            0,
            "yc.nbs",
            "yc.nbs.tests",
            1,
            NProto::STORAGE_MEDIA_SSD_MIRROR2,
            ExternalVolumeTabletId);
        diskRegistry.SendRequest(std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            diskRegistry.RecvAllocateDiskResponse()->GetStatus());

        TVector<ui64> notifiedTabletIds;
        runtime->SetEventFilter([&](auto&, auto& event) {
            if (event->GetTypeRewrite() ==
                    TEvVolume::EvReallocateDiskRequest &&
                event->Recipient == MakeVolumeProxyServiceId())
            {
                const auto* msg =
                    event->template Get<TEvVolume::TEvReallocateDiskRequest>();
                UNIT_ASSERT_VALUES_EQUAL("fs", msg->Record.GetDiskId());
                notifiedTabletIds.push_back(
                    msg->Record.GetExternalVolumeTabletId());
                return true;
            }
            return false;
        });

        RegisterAgent(*runtime, 2);
        runtime->AdvanceCurrentTime(TDuration::Seconds(5));
        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return !notifiedTabletIds.empty();
        };
        UNIT_ASSERT(runtime->DispatchEvents(
            options,
            TDuration::MilliSeconds(100)));

        UNIT_ASSERT_VALUES_EQUAL(1, notifiedTabletIds.size());
        UNIT_ASSERT_VALUES_EQUAL(
            ExternalVolumeTabletId,
            notifiedTabletIds[0]);
    }

    Y_UNIT_TEST(ShouldAllocateDiskForExternalVolume)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            AllocateDisk(
                diskRegistry,
                "fs",
                ExternalVolumeTabletId)->GetStatus());

        {
            auto response =
                diskRegistry.BackupDiskRegistryState(NProto::BDRSS_LOCAL_DB);
            const auto& disks = response->Record.GetLocalDBBackup().GetDisks();
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
            UNIT_ASSERT_VALUES_EQUAL("fs", disks[0].GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(
                ExternalVolumeTabletId,
                disks[0].GetExternalVolumeTabletId());
        }

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            AllocateDisk(
                diskRegistry,
                "fs",
                ExternalVolumeTabletId)->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            AllocateDisk(
                diskRegistry,
                "fs",
                ExternalVolumeTabletId + 1)->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            AllocateDisk(diskRegistry, "fs", 0)->GetStatus());
        UNIT_ASSERT(diskRegistry.Exists("fs"));
    }

    Y_UNIT_TEST(ShouldDeallocateDiskOnlyForExternalOwner)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            AllocateDisk(
                diskRegistry,
                "fs",
                ExternalVolumeTabletId)->GetStatus());

        diskRegistry.SendMarkDiskForCleanupRequest("fs");
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            diskRegistry.RecvMarkDiskForCleanupResponse()->GetStatus());
        diskRegistry.MarkDiskForCleanup("fs", ExternalVolumeTabletId);

        diskRegistry.SendDeallocateDiskRequest("fs");
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            diskRegistry.RecvDeallocateDiskResponse()->GetStatus());
        UNIT_ASSERT(diskRegistry.Exists("fs"));

        diskRegistry.DeallocateDisk(
            "fs",
            false,
            ExternalVolumeTabletId);
        UNIT_ASSERT(!diskRegistry.Exists("fs"));
    }

    Y_UNIT_TEST(ShouldKeepExternalDiskDuringAutomaticCleanup)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            AllocateDisk(
                diskRegistry,
                "fs",
                ExternalVolumeTabletId)->GetStatus());
        diskRegistry.AllocateDisk("nbs-garbage", 10_GB);

        diskRegistry.MarkDiskForCleanup("fs", ExternalVolumeTabletId);
        diskRegistry.MarkDiskForCleanup("nbs-garbage");

        TVector<TString> described;
        runtime->SetEventFilter([&] (auto&, auto& event) {
            if (event->GetTypeRewrite() ==
                TEvSSProxy::EvDescribeVolumeRequest)
            {
                described.push_back(
                    event->template Get<
                        TEvSSProxy::TEvDescribeVolumeRequest>()->DiskId);
            }
            return false;
        });

        diskRegistry.CleanupDisks();

        UNIT_ASSERT_VALUES_EQUAL(1, described.size());
        UNIT_ASSERT_VALUES_EQUAL("nbs-garbage", described[0]);
        UNIT_ASSERT(diskRegistry.Exists("fs"));
        UNIT_ASSERT(!diskRegistry.Exists("nbs-garbage"));

        diskRegistry.DeallocateDisk("fs", false, ExternalVolumeTabletId);
        UNIT_ASSERT(!diskRegistry.Exists("fs"));
    }

    Y_UNIT_TEST(ShouldNotDestroyVolumeAfterFailedExternalAllocation)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            E_BS_DISK_ALLOCATION_FAILED,
            AllocateDisk(
                diskRegistry,
                "fs",
                ExternalVolumeTabletId,
                1000_GB)->GetStatus());
        UNIT_ASSERT(diskRegistry.ListBrokenDisks()->DiskIds.empty());
        UNIT_ASSERT(!diskRegistry.Exists("fs"));

        diskRegistry.SendAllocateDiskRequest("nbs", 1000_GB);
        UNIT_ASSERT_VALUES_EQUAL(
            E_BS_DISK_ALLOCATION_FAILED,
            diskRegistry.RecvAllocateDiskResponse()->GetStatus());
        const auto brokenDisks = diskRegistry.ListBrokenDisks()->DiskIds;
        UNIT_ASSERT_VALUES_EQUAL(1, brokenDisks.size());
        UNIT_ASSERT_VALUES_EQUAL("nbs", brokenDisks[0]);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
