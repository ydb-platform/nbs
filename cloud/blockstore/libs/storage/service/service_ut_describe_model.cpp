#include "service_ut.h"

#include <cloud/blockstore/libs/storage/core/config.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceDescribeVolumeModelTest)
{
    Y_UNIT_TEST(ShouldDescribeVolume)
    {
        TTestEnv env;

        NProto::TStorageServiceConfig ssConfig;
        ssConfig.SetThrottlingEnabled(true);
        ssConfig.SetHDDUnitReadIops(100);
        ssConfig.SetHDDUnitReadBandwidth(30);
        ssConfig.SetHDDUnitWriteIops(200);
        ssConfig.SetHDDUnitWriteBandwidth(60);
        ssConfig.SetHDDMaxReadIops(1000);
        ssConfig.SetHDDMaxReadBandwidth(300);
        ssConfig.SetHDDMaxWriteIops(2000);
        ssConfig.SetHDDMaxWriteBandwidth(600);
        ssConfig.SetAllocationUnitHDD(1);
        ssConfig.SetFreshChannelCountHDD(1);
        ssConfig.SetFreshChannelCountSSD(1);
        ui32 nodeIdx = SetupTestEnv(env, std::move(ssConfig));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto response = service.DescribeVolumeModel(
            5_GB / DefaultBlockSize,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        );

        const auto& vm = response->Record.GetVolumeModel();
        UNIT_ASSERT_VALUES_EQUAL(
                6,
                vm.GetMixedChannelsCount() +
                vm.GetMergedChannelsCount() +
                vm.GetFreshChannelsCount());
        const auto& pp = vm.GetPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(true, pp.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(500, pp.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(150_MB, pp.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(1000, pp.GetMaxWriteIops());
        UNIT_ASSERT_VALUES_EQUAL(300_MB, pp.GetMaxWriteBandwidth());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
