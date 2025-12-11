#include "service_ut.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServicePlacementTest)
{
    Y_UNIT_TEST(ShouldCreateDescribeAlterDestroyGroups)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(10);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        for (const auto& diskId: {"d1", "d2", "d3", "d4"}) {
            service.CreateVolume(
                diskId,
                10_GB / DefaultBlockSize,
                DefaultBlockSize,
                "",
                "",
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        }

        service.CreatePlacementGroup(
            "g1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);
        service.CreatePlacementGroup(
            "g2",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);
        {
            auto response = service.ListPlacementGroups();
            auto groupIds = response->Record.GetGroupIds();
            Sort(groupIds.begin(), groupIds.end());
            UNIT_ASSERT_VALUES_EQUAL(2, groupIds.size());
            UNIT_ASSERT_VALUES_EQUAL("g1", groupIds[0]);
            UNIT_ASSERT_VALUES_EQUAL("g2", groupIds[1]);
        }
        service.AlterPlacementGroupMembership(
            "g1",
            TVector<TString>{"d1", "d2"},
            TVector<TString>{});
        service.AlterPlacementGroupMembership(
            "g1",
            TVector<TString>{"d3", "d4"},
            TVector<TString>{"d2"});
        {
            auto response = service.DescribePlacementGroup("g1");
            UNIT_ASSERT_VALUES_EQUAL(
                "g1",
                response->Record.GetGroup().GetGroupId());
            auto diskIds = response->Record.GetGroup().GetDiskIds();
            Sort(diskIds.begin(), diskIds.end());
            UNIT_ASSERT_VALUES_EQUAL(3, diskIds.size());
            UNIT_ASSERT_VALUES_EQUAL("d1", diskIds[0]);
            UNIT_ASSERT_VALUES_EQUAL("d3", diskIds[1]);
            UNIT_ASSERT_VALUES_EQUAL("d4", diskIds[2]);
        }

        service.DestroyPlacementGroup("g1");
        service.DestroyPlacementGroup("g2");
    }

    Y_UNIT_TEST(ShouldCreateVolumeInPlacementGroup)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreatePlacementGroup(
            "g1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0);

        service.CreateVolume(
            "d1",
            1_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",
            "",
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::TVolumePerformanceProfile(),
            "g1");

        {
            auto response = service.DescribePlacementGroup("g1");
            UNIT_ASSERT_VALUES_EQUAL(
                "g1",
                response->Record.GetGroup().GetGroupId());
            auto diskIds = response->Record.GetGroup().GetDiskIds();
            Sort(diskIds.begin(), diskIds.end());
            UNIT_ASSERT_VALUES_EQUAL(1, diskIds.size());
            UNIT_ASSERT_VALUES_EQUAL("d1", diskIds[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.GetGroup().GetConfigVersion());
        }

        // TODO: test allocation error
    }
}

}   // namespace NCloud::NBlockStore::NStorage
