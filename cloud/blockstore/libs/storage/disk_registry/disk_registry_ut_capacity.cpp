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


namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldReturnCapacity)
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

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});
        {
            auto response = diskRegistry.GetClusterCapacity();

            auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(4, msg.CapacitySize());
            for (auto& capacity: msg.GetCapacity()) {
                if (capacity.GetKind() == NProto::STORAGE_MEDIA_HDD_NONREPLICATED) {
                    UNIT_ASSERT_VALUES_EQUAL(0, capacity.GetFree());
                    UNIT_ASSERT_VALUES_EQUAL(0, capacity.GetTotal());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(20_GB, capacity.GetFree());
                    UNIT_ASSERT_VALUES_EQUAL(20_GB, capacity.GetTotal());
                }
            }
        }
        {
            diskRegistry.AllocateDisk("disk-1", 10_GB);

            auto response = diskRegistry.GetClusterCapacity();

            auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(4, msg.CapacitySize());
            for (auto& capacity: msg.GetCapacity()) {
                if (capacity.GetKind() == NProto::STORAGE_MEDIA_HDD_NONREPLICATED) {
                    UNIT_ASSERT_VALUES_EQUAL(0, capacity.GetFree());
                    UNIT_ASSERT_VALUES_EQUAL(0, capacity.GetTotal());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(10_GB, capacity.GetFree());
                    UNIT_ASSERT_VALUES_EQUAL(20_GB, capacity.GetTotal());
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
