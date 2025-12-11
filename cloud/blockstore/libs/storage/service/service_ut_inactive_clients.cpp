#include "service_ut.h"

#include <cloud/blockstore/libs/storage/core/config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceInactiveClientsTest)
{
    Y_UNIT_TEST(ShouldConsiderInactiveClientsUnmounted)
    {
        TTestEnv env;
        TDuration mountVolumeTimeout = TDuration::Seconds(3);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetInactiveClientsTimeout(
            mountVolumeTimeout.MilliSeconds());

        ui32 nodeIdx = SetupTestEnv(env, std::move(storageServiceConfig));

        TActorId volumeActorId;
        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume();

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        bool detectedInactiveClientsTimeout = false;
        bool detectedStartVolumeActorStopped = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvInactiveClientsTimeout: {
                        detectedInactiveClientsTimeout = true;
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped: {
                        detectedStartVolumeActorStopped = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // Give a chance to the timeout event although it shouldn't happen
        runtime.UpdateCurrentTime(
            runtime.GetCurrentTime() + mountVolumeTimeout +
            TDuration::Seconds(1));
        runtime.DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(100));

        // Should receive new session id because the volume should have been
        // unmounted via timeout
        TString newSessionId;
        {
            auto response = service.MountVolume();
            newSessionId = response->Record.GetSessionId();
        }

        UNIT_ASSERT(sessionId != newSessionId);
        UNIT_ASSERT(detectedInactiveClientsTimeout);
        UNIT_ASSERT(detectedStartVolumeActorStopped);
    }

    Y_UNIT_TEST(ShouldReturnInactiveClientsTimeoutInMountResponse)
    {
        TTestEnv env;

        TDuration mountVolumeTimeout = TDuration::Seconds(3);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetInactiveClientsTimeout(
            mountVolumeTimeout.MilliSeconds());
        storageServiceConfig.SetClientRemountPeriod(
            mountVolumeTimeout.MilliSeconds());

        ui32 nodeIdx = SetupTestEnv(env, std::move(storageServiceConfig));
        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        {
            auto response = service.MountVolume(DefaultDiskId, "foo", "bar");
            UNIT_ASSERT(
                response->Record.GetInactiveClientsTimeout() ==
                mountVolumeTimeout.MilliSeconds());

            auto sessionId = response->Record.GetSessionId();
            service.UnmountVolume(DefaultDiskId, sessionId);
        }

        {
            auto response = service.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT(
                response->Record.GetInactiveClientsTimeout() ==
                mountVolumeTimeout.MilliSeconds());

            auto sessionId = response->Record.GetSessionId();
            service.UnmountVolume(DefaultDiskId, sessionId);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
