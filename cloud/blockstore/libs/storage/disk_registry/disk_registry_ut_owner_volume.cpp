#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 OwnerTabletId = 42;

auto AllocateDisk(
    TDiskRegistryClient& diskRegistry,
    const TString& diskId,
    ui64 ownerVolumeTabletId,
    ui64 diskSize = 10_GB)
{
    auto request = diskRegistry.CreateAllocateDiskRequest(diskId, diskSize);
    request->Record.SetOwnerVolumeTabletId(ownerVolumeTabletId);
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

Y_UNIT_TEST_SUITE(TDiskRegistryOwnerVolumeTest)
{
    Y_UNIT_TEST(ShouldAllocateDiskForOwnerVolume)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            AllocateDisk(diskRegistry, "fs", OwnerTabletId)->GetStatus());

        {
            auto response =
                diskRegistry.BackupDiskRegistryState(NProto::BDRSS_LOCAL_DB);
            const auto& disks = response->Record.GetLocalDBBackup().GetDisks();
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
            UNIT_ASSERT_VALUES_EQUAL("fs", disks[0].GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(
                OwnerTabletId,
                disks[0].GetOwnerVolumeTabletId());
        }

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            AllocateDisk(diskRegistry, "fs", OwnerTabletId)->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            AllocateDisk(diskRegistry, "fs", OwnerTabletId + 1)->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            AllocateDisk(diskRegistry, "fs", 0)->GetStatus());
        UNIT_ASSERT(diskRegistry.Exists("fs"));
    }

    Y_UNIT_TEST(ShouldDeallocateDiskOnlyByOwnerVolume)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        AllocateDisk(diskRegistry, "fs", OwnerTabletId);

        diskRegistry.SendMarkDiskForCleanupRequest("fs");
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            diskRegistry.RecvMarkDiskForCleanupResponse()->GetStatus());
        diskRegistry.MarkDiskForCleanup("fs", OwnerTabletId);

        diskRegistry.SendDeallocateDiskRequest("fs");
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            diskRegistry.RecvDeallocateDiskResponse()->GetStatus());
        UNIT_ASSERT(diskRegistry.Exists("fs"));

        diskRegistry.DeallocateDisk("fs", false, OwnerTabletId);
        UNIT_ASSERT(!diskRegistry.Exists("fs"));
    }

    Y_UNIT_TEST(ShouldNotifyOwnerVolumeTablet)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        AllocateDisk(diskRegistry, "fs", OwnerTabletId);
        diskRegistry.AllocateDisk("nbs", 10_GB);

        THashMap<TString, ui64> notified;
        runtime->SetEventFilter([&] (auto&, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvVolume::EvReallocateDiskRequest &&
                event->Recipient == MakeVolumeProxyServiceId())
            {
                auto* msg = event->Get<TEvVolume::TEvReallocateDiskRequest>();
                notified[msg->Record.GetDiskId()] =
                    msg->Record.GetOwnerVolumeTabletId();
            }
            return false;
        });

        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::AGENT_STATE_UNAVAILABLE);

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&] { return notified.size() == 2; }});

        UNIT_ASSERT_VALUES_EQUAL(OwnerTabletId, notified.at("fs"));
        UNIT_ASSERT_VALUES_EQUAL(0, notified.at("nbs"));
    }

    Y_UNIT_TEST(ShouldCleanupExternalDiskWithoutSchemeShardCheck)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        AllocateDisk(diskRegistry, "fs", OwnerTabletId);
        diskRegistry.AllocateDisk("nbs-garbage", 10_GB);

        diskRegistry.MarkDiskForCleanup("fs", OwnerTabletId);
        diskRegistry.MarkDiskForCleanup("nbs-garbage");

        TVector<TString> described;
        runtime->SetEventFilter([&] (auto&, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvSSProxy::EvDescribeVolumeRequest) {
                described.push_back(
                    event->Get<TEvSSProxy::TEvDescribeVolumeRequest>()->DiskId);
            }
            return false;
        });

        diskRegistry.CleanupDisks();

        UNIT_ASSERT_VALUES_EQUAL(1, described.size());
        UNIT_ASSERT_VALUES_EQUAL("nbs-garbage", described[0]);
        UNIT_ASSERT(!diskRegistry.Exists("fs"));
        UNIT_ASSERT(!diskRegistry.Exists("nbs-garbage"));
    }

    Y_UNIT_TEST(ShouldNotDestroyVolumeAfterFailedExternalAllocation)
    {
        auto runtime = CreateRuntime(CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        }));
        TDiskRegistryClient diskRegistry(*runtime);

        UNIT_ASSERT_VALUES_EQUAL(
            E_BS_DISK_ALLOCATION_FAILED,
            AllocateDisk(diskRegistry, "fs", OwnerTabletId, 1000_GB)
                ->GetStatus());
        UNIT_ASSERT(diskRegistry.ListBrokenDisks()->DiskIds.empty());
        UNIT_ASSERT(!diskRegistry.Exists("fs"));

        // A native volume is still destroyed after a failed allocation.
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
