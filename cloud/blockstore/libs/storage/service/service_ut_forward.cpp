#include "service_ut.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/blockstore/private/api/protos/volume.pb.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceForwardTest)
{
    Y_UNIT_TEST(ShouldForwardRequests)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        service1.CreateVolume();
        service1.MountVolume();

        TServiceClient service2(env.GetRuntime(), nodeIdx2);
        service2.StatVolume();

        service2.SendStatVolumeRequest("unknown");
        auto response = service2.RecvStatVolumeResponse();

        UNIT_ASSERT_C(FAILED(response->GetStatus()), response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldNotifyIfTabletRemounted)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        service.UnmountVolume(DefaultDiskId, sessionId);
        service.MountVolume();

        {
            service.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                sessionId);
            auto response = service.RecvWriteBlocksResponse();

            UNIT_ASSERT_C(response->GetStatus() == E_BS_INVALID_SESSION, response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldForwardResponseToAppropriateActorWhenUsingPipe)
    {
        TTestEnv env(1, 2, 4);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume(DefaultDiskId, 2048);

        TString sessionId;
        {
            auto response = service1.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        TServiceClient service2(runtime, nodeIdx2);

        auto NullActor = runtime.AllocateEdgeActor(nodeIdx1);
        auto FirstRequestActor = runtime.AllocateEdgeActor(nodeIdx2);
        auto SecondRequestActor = runtime.AllocateEdgeActor(nodeIdx2);

        int event_counter = 0;
        int pipe_counter = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvSend: {
                        if (event->Type == TEvService::EvWriteBlocksRequest) {
                            ++pipe_counter;
                        }
                        break;
                    }
                    case TEvService::EvWriteBlocksRequest: {
                        if (pipe_counter == 2) {
                            ++event_counter;
                            if (event_counter == 2) {
                                auto &s = const_cast<TActorId&>(event->Sender);
                                s = NullActor;
                            }
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        runtime.Send(
            new IEventHandle(
                MakeStorageServiceId(),
                FirstRequestActor,
                service2.CreateWriteBlocksRequest(
                    DefaultDiskId,
                    TBlockRange64::WithLength(0, 1024),
                    sessionId).release()),
            nodeIdx2);

        runtime.Send(
            new IEventHandle(
                MakeStorageServiceId(),
                SecondRequestActor,
                service2.CreateWriteBlocksRequest(
                    DefaultDiskId,
                    TBlockRange64::WithLength(0, 1024),
                    sessionId).release()),
            nodeIdx2);

        TAutoPtr<IEventHandle> handle1;
        runtime.GrabEdgeEventRethrow<TEvService::TEvWriteBlocksResponse>(handle1);

        TAutoPtr<IEventHandle> handle2;
        runtime.GrabEdgeEventRethrow<TEvService::TEvWriteBlocksResponse>(handle2);

        UNIT_ASSERT_C(
            (handle2->Recipient == SecondRequestActor),
            "Wrong Reply Actor");
    }

    Y_UNIT_TEST(ShouldCancelPipeRequestsIfTabletDied)
    {
        TTestEnv env(1, 2, 4);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume(DefaultDiskId, 2048);
        auto sessionId = service1.MountVolume()->Record.GetSessionId();

        TServiceClient service2(runtime, nodeIdx2);

        auto KillerActor = runtime.AllocateEdgeActor(nodeIdx1);

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvSend: {
                        if (event->Type == TEvService::EvWriteBlocksRequest) {
                            runtime.Send(
                                new IEventHandle(
                                    event->GetRecipientRewrite(),
                                    KillerActor,
                                    std::make_unique<TEvents::TEvPoisonPill>().release()),
                                nodeIdx1);
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service2.SendWriteBlocksRequest(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId
        );
        auto response = service2.RecvWriteBlocksResponse();
        UNIT_ASSERT_C(
            FAILED(response->GetStatus()),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldCancelPipeRequestsIfPipeIsBroken)
    {
        TTestEnv env(1, 2, 4);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume(DefaultDiskId, 2048);
        auto sessionId = service1.MountVolume()->Record.GetSessionId();

        TServiceClient service2(runtime, nodeIdx2);

        auto killerActor = runtime.AllocateEdgeActor(nodeIdx2);

        ui64 tabletId = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvClientConnected: {
                        auto* msg = reinterpret_cast<TEvTabletPipe::TEvClientConnected::TPtr*>(&event);
                        tabletId = (*msg)->Get()->TabletId;
                        break;
                    }
                    case TEvTabletPipe::EvSend: {
                        if (event->Type == TEvService::EvWriteBlocksRequest) {
                            runtime.Send(
                                new IEventHandle(
                                    event->Sender,
                                    killerActor,
                                    std::make_unique<TEvTabletPipe::TEvClientDestroyed>(
                                        tabletId,
                                        TActorId(),
                                        TActorId()).release()),
                                nodeIdx2);
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service2.StatVolume();

        service2.SendWriteBlocksRequest(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId
        );
        auto response = service2.RecvWriteBlocksResponse();
        UNIT_ASSERT_C(
            FAILED(response->GetStatus()),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldForbidReadWriteBlocksWithoutMounting)
    {
        TTestEnv env;

        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        service.WaitForVolume();

        {
            service.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                TString()
            );
            auto response = service.RecvWriteBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }

        {
            service.SendReadBlocksRequest(
                DefaultDiskId,
                0,
                TString()
            );
            auto response = service.RecvReadBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }

        {
            service.SendZeroBlocksRequest(
                DefaultDiskId,
                0,
                TString()
            );
            auto response = service.RecvZeroBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }
    }

    Y_UNIT_TEST(ShouldForbidReadWriteBlocksFromSecondClientWithoutMounting)
    {
        TTestEnv env;

        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");
        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        TServiceClient service2(env.GetRuntime(), nodeIdx);

        {
            service2.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                TString()
            );
            auto response = service2.RecvWriteBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }

        {
            service2.SendReadBlocksRequest(
                DefaultDiskId,
                0,
                TString()
            );
            auto response = service2.RecvReadBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }

        {
            service2.SendZeroBlocksRequest(
                DefaultDiskId,
                0,
                TString()
            );
            auto response = service2.RecvZeroBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }
    }

    Y_UNIT_TEST(ShouldAllowWriteRequestsWithReadOnlyAccessModeAndForceWriteMountFlag)
    {
        TTestEnv env;

        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        ui32 mountFlags = 0;
        SetProtoFlag(mountFlags, NProto::MF_FORCE_WRITE);

        // setting read-only volume tag

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = "read-only";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        // and using READ_ONLY mount

        auto sessionId = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags)->Record.GetSessionId();

        // but write/zero requests should still succeed since MF_FORCE_WRITE
        // overrides all checks

        {
            service.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                sessionId);
            auto response = service.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            service.SendZeroBlocksRequest(
                DefaultDiskId,
                0,
                sessionId);
            auto response = service.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldForbidReadWriteBlocksWithMountingButWithoutSessionId)
    {
        TTestEnv env;

        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.MountVolume();

        {
            service.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                TString()
            );
            auto response = service.RecvWriteBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }

        {
            service.SendReadBlocksRequest(
                DefaultDiskId,
                0,
                TString()
            );
            auto response = service.RecvReadBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }

        {
            service.SendZeroBlocksRequest(
                DefaultDiskId,
                0,
                TString()
            );
            auto response = service.RecvZeroBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_BS_INVALID_SESSION);
        }
    }

    Y_UNIT_TEST(ShouldReturnRejectedOnReadWriteBlocksWhenVolumeIsNotReady)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TActorId volumeActorId;
        bool pendingVolumeTabletStatus = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        volumeActorId = msg->VolumeActor;
                        if (pendingVolumeTabletStatus) {
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        auto sessionId = service.MountVolume()->Record.GetSessionId();

        UNIT_ASSERT(volumeActorId);

        // Kill the volume actor
        service.SendRequest(volumeActorId, std::make_unique<TEvents::TEvPoisonPill>());

        // Wait until tablet goes down
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvVolumeTabletStatus);
            runtime.DispatchEvents(options);
            UNIT_ASSERT(!volumeActorId);
        }

        pendingVolumeTabletStatus = true;

        // Volume tablet status will not be delivered to service actor now
        // so from service's perspective the volume is still rebooting i.e. not ready

        {
            service.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                sessionId);
            auto response = service.RecvWriteBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_REJECTED);
            UNIT_ASSERT(
                response->GetErrorReason().Contains("has not started yet"));
        }

        {
            service.SendReadBlocksRequest(DefaultDiskId, 0, sessionId);
            auto response = service.RecvReadBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_REJECTED);
            UNIT_ASSERT(
                response->GetErrorReason().Contains("has not started yet"));
        }

        {
            service.SendZeroBlocksRequest(DefaultDiskId, 0, sessionId);
            auto response = service.RecvZeroBlocksResponse();
            UNIT_ASSERT(response->GetStatus() == E_REJECTED);
            UNIT_ASSERT(
                response->GetErrorReason().Contains("has not started yet"));
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
