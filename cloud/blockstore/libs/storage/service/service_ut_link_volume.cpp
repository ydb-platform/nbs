#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceLinkVolumeTest)
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
        UNIT_ASSERT_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldUnlinkVolume)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        service.CreateVolumeLink("vol-1", "vol-2");
        {
            service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
            auto response = service.RecvDestroyVolumeLinkResponse();
            UNIT_ASSERT_EQUAL_C(
                S_OK,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }
        {
            service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
            auto response = service.RecvDestroyVolumeLinkResponse();
            UNIT_ASSERT_EQUAL_C(
                S_ALREADY,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldUnlinkVolumeWhenFollowerDestroyed)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-1", DefaultBlocksCount);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        service.CreateVolumeLink("vol-1", "vol-2");
        service.DestroyVolume("vol-2");

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
        auto response = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_EQUAL_C(
            S_OK,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldUnlinkVolumeWhenLeaderNotExists)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        size_t followerNotificationCount = 0;
        auto listenUnlinkFollower =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() ==
                TEvVolume::EvNotifyFollowerVolumeRequest)
            {
                ++followerNotificationCount;

                const auto* msg =
                    ev->Get<TEvVolume::TEvNotifyFollowerVolumeRequest>();
                UNIT_ASSERT_VALUES_EQUAL(
                    "vol-1",
                    msg->Record.GetLeaderDiskId());
                UNIT_ASSERT_EQUAL(
                    NProto::EFollowerNotificationReason::
                        FOLLOWER_NOTIFICATION_REASON_DESTROYED,
                    msg->Record.GetReason());
            }
            return false;
        };

        runtime.SetEventFilter(listenUnlinkFollower);

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol-2", DefaultBlocksCount);

        service.SendDestroyVolumeLinkRequest("vol-1", "vol-2");
        auto response = service.RecvDestroyVolumeLinkResponse();
        UNIT_ASSERT_EQUAL_C(
            S_ALREADY,
            response->GetError().GetCode(),
            FormatError(response->GetError()));

        UNIT_ASSERT_VALUES_EQUAL_C(
            2,
            followerNotificationCount,
            "Follower notification count must be 2 (one for volume proxy and "
            "one for volume)");
    }
}

}   // namespace NCloud::NBlockStore::NStorage
