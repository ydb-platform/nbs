#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceLinVolumeTest)
{
    Y_UNIT_TEST(ShouldFailOnInvalidArgumentVolume)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount * 2);

        {
            service.SendCreateVolumeLinkRequest("vol-1", "vol-1");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(E_ARGUMENT, response->GetError().GetCode());
        }
        {
            service.SendCreateVolumeLinkRequest("vol-1", "unknown");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(E_ARGUMENT, response->GetError().GetCode());
        }
        {
            service.SendCreateVolumeLinkRequest("unknown", "vol-1");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(E_ARGUMENT, response->GetError().GetCode());
        }
        {
            service.SendCreateVolumeLinkRequest("vol-2", "vol-1");
            auto response = service.RecvCreateVolumeLinkResponse();
            UNIT_ASSERT_C(E_ARGUMENT, response->GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldLinkVolume)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount * 2);

        service.SendCreateVolumeLinkRequest("vol-1", "vol-2");
        auto response = service.RecvCreateVolumeLinkResponse();
        UNIT_ASSERT_C(E_NOT_IMPLEMENTED, response->GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
