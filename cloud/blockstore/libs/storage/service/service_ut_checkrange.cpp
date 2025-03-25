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
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
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

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
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

        service.SendCheckRangeRequest(DefaultDiskId, 0, 0, false);
        auto response = service.RecvCheckRangeResponse();

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
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

        auto response = service.CheckRange(DefaultDiskId, 0, blocksCount, true);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(blocksCount, response->Record.ChecksumsSize());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
