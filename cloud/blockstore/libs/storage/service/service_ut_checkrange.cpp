#include "service_ut.h"

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceCheckRangeTest)
{
    Y_UNIT_TEST(ShouldCheckRange)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));
        ui64 size = 512;

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            size,
            DefaultBlockSize,
            "test_folder",
            "test_cloud");

        auto response = service.CheckRange(DefaultDiskId, 0, size, false);
        UNIT_ASSERT(response->GetStatus() == S_OK);
    }

    Y_UNIT_TEST(ShouldFailCheckRangeWithEmptyDiskId)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));
        ui64 size = 512;

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            size,
            DefaultBlockSize,
            "test_folder",
            "test_cloud");

        service.SendCheckRangeRequest(TString(), 0, size, false);
        auto response = service.RecvCheckRangeResponse();

        UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
    }

    Y_UNIT_TEST(ShouldFailCheckRangeWithZeroSize)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));
        ui64 size = 512;

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            size,
            DefaultBlockSize,
            "test_folder",
            "test_cloud");

        service.SendCheckRangeRequest(DefaultDiskId, 0, size, false);
        auto response = service.RecvCheckRangeResponse();

        UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
    }

    Y_UNIT_TEST(ShouldCalculateCheckSumsWhileCheckRange)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        ui64 blocksCount = 512;

        service.CreateVolume(
            DefaultDiskId,
            blocksCount,
            DefaultBlockSize,
            "test_folder",
            "test_cloud");

        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));


        auto response = service.CheckRange(DefaultDiskId, 0, blocksCount, true);
        UNIT_ASSERT(response->GetStatus() == S_OK);
        UNIT_ASSERT_VALUES_EQUAL(response.get()->Record.ChecksumsSize(), blocksCount);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
