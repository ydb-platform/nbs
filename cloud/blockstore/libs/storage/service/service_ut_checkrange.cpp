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

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "test_folder",
            "test_cloud");
        ui64 size = 512;

        auto response = service.CheckRange(DefaultDiskId, 0, size);
        UNIT_ASSERT(response->GetStatus() == S_OK);
    }

    Y_UNIT_TEST(ShouldFailCheckRangeWithEmptyDiskId)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "test_folder",
            "test_cloud");
        ui64 size = 512;

        service.SendCheckRangeRequest(TString(), 0, size);
        auto response = service.RecvCheckRangeResponse();

        UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
    }

    Y_UNIT_TEST(ShouldFailCheckRangeWithZeroSize)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "test_folder",
            "test_cloud");
        ui64 size = 0;

        service.SendCheckRangeRequest(DefaultDiskId, 0, size);
        auto response = service.RecvCheckRangeResponse();

        UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
