#include "service_ut.h"

#include "service_events_private.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/mount_token.h>
#include "cloud/blockstore/private/api/protos/volume.pb.h"

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/helpers.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/string/escape.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

using EChangeBindingOp = TEvService::TEvChangeVolumeBindingRequest::EChangeBindingOp;

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceMountVolumeTest)
{
    void DoTestShouldMountVolume(TTestEnv& env, ui32 nodeIdx)
    {
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        service.UnmountVolume(DefaultDiskId, sessionId);
    }

    Y_UNIT_TEST(ShouldMountVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        DoTestShouldMountVolume(env, nodeIdx);
    }

    Y_UNIT_TEST(ShouldPerformReadWriteInRepairMode)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        TString sessionId;
        {
            auto response = service.MountVolume(
                DefaultDiskId,
                "", // instanceId
                "", // token
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_REPAIR,
                NProto::VOLUME_MOUNT_LOCAL);
            sessionId = response->Record.GetSessionId();
        }

        service.WriteBlocks(DefaultDiskId, TBlockRange64::MakeOneBlock(0), sessionId, 1);
        {
            auto response = service.ReadBlocks(DefaultDiskId, 0, sessionId);

            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_EQUAL(1, blocks.BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 1),
                blocks.GetBuffers(0)
            );
        }

        service.UnmountVolume(DefaultDiskId, sessionId);
    }

    Y_UNIT_TEST(ShouldDisallowMultipleLocalVolumeMount)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");
        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        for (ui32 nodeIdx: {nodeIdx1, nodeIdx2}) {
            TServiceClient service2(env.GetRuntime(), nodeIdx);
            service2.SendMountVolumeRequest(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_LOCAL);

            auto response = service2.RecvMountVolumeResponse();
            UNIT_ASSERT_C(
                FAILED(response->GetStatus()),
                "Mount volume request unexpectedly succeeded");
        }
    }

    Y_UNIT_TEST(ShouldDisallowMultipleMountWithReadWriteAccess)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");
        service1.MountVolume(DefaultDiskId, "foo", "bar");

        for (ui32 nodeIdx: {nodeIdx1, nodeIdx2}) {
            TServiceClient service2(env.GetRuntime(), nodeIdx);
            service2.SendMountVolumeRequest(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE);

            auto response = service2.RecvMountVolumeResponse();
            UNIT_ASSERT_C(
                FAILED(response->GetStatus()),
                "Mount volume request unexpectedly succeeded");
        }
    }

    Y_UNIT_TEST(ShouldAllowReadWriteMountIfVolumeIsReadOnlyMounted)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");
        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        for (ui32 nodeIdx: {nodeIdx1, nodeIdx2}) {
            for (auto accessMode: {NProto::VOLUME_ACCESS_READ_WRITE, NProto::VOLUME_ACCESS_REPAIR}) {
                TServiceClient service2(env.GetRuntime(), nodeIdx);
                auto response = service2.MountVolume(
                    DefaultDiskId,
                    "foo",
                    "bar",
                    NProto::IPC_GRPC,
                    accessMode,
                    NProto::VOLUME_MOUNT_REMOTE);
                auto sessionId = response->Record.GetSessionId();
                service2.UnmountVolume(DefaultDiskId, sessionId);
            }
        }
    }

    void DoTestShouldKeepSessionsAfterTabletRestart(
        TTestEnv& env,
        ui32 nodeIdx1,
        ui32 nodeIdx2)
    {
        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");
        auto response = service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        TString sessionId = response->Record.GetSessionId();

        runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::Seconds(10));

        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        TServiceClient service2(runtime, nodeIdx2);
        service2.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        service1.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::MakeOneBlock(0),
            sessionId);
    }

    Y_UNIT_TEST(ShouldKeepSessionsAfterTabletRestart)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        ui32 nodeIdx2 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        DoTestShouldKeepSessionsAfterTabletRestart(env, nodeIdx1, nodeIdx2);
    }

    Y_UNIT_TEST(ShouldRemoveClientIfClientDoesNotReconnectAfterPipeReset)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(9);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        ui32 nodeIdx2 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        ui64 volumeTabletId = 0;
        auto& runtime = env.GetRuntime();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        if (!volumeTabletId) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvSSProxy::EvDescribeVolumeResponse);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT(volumeTabletId);

        RebootTablet(runtime, volumeTabletId, service1.GetSender(), nodeIdx1);

        TServiceClient service2(runtime, nodeIdx2);

        // Shouldn't be able to mount the same volume with read-write
        // access immediately after the volume was rebooted
        service2.SendMountVolumeRequest(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
        {
            auto response = service2.RecvMountVolumeResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        runtime.UpdateCurrentTime(runtime.GetCurrentTime() + unmountClientsTimeout);

        // Should now be able to mount the volume as the first service
        // should have timed out
        service2.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
    }

    Y_UNIT_TEST(ShouldAllowMultipleReadOnlyMountIfOnlyOneClientHasLocalMount)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");
        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        for (ui32 nodeIdx: {nodeIdx1, nodeIdx2}) {
            TServiceClient service2(env.GetRuntime(), nodeIdx);
            auto response = service2.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_REMOTE);
            auto sessionId = response->Record.GetSessionId();
            service2.UnmountVolume(DefaultDiskId, sessionId);
        }
    }

    Y_UNIT_TEST(ShouldBumpClientsLastActivityTimeOnMountRequest)
    {
        static constexpr TDuration mountVolumeTimeout = TDuration::Seconds(3);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetInactiveClientsTimeout(mountVolumeTimeout.MilliSeconds());

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, std::move(storageServiceConfig));

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume();

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        runtime.UpdateCurrentTime(runtime.GetCurrentTime() + mountVolumeTimeout);
        service.MountVolume();
        runtime.UpdateCurrentTime(runtime.GetCurrentTime() + mountVolumeTimeout * 0.05);

        // Wait for timeout check
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvInactiveClientsTimeout);
            runtime.DispatchEvents(options);
        }

        // Should receive the same session id as before because volume
        // was not unmounted via timeout due to present ping
        TString newSessionId;
        {
            auto response = service.MountVolume();
            newSessionId = response->Record.GetSessionId();
        }

        UNIT_ASSERT(sessionId == newSessionId);
    }

    Y_UNIT_TEST(ShouldMountVolumeWhenFirstMounterTimesOutOnVolumeSide)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(9);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        ui32 nodeIdx2 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        ui64 volumeTabletId = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        if (!volumeTabletId) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvSSProxy::EvDescribeVolumeResponse);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT(volumeTabletId);

        RebootTablet(runtime, volumeTabletId, service1.GetSender(), nodeIdx1);

        TServiceClient service2(runtime, nodeIdx2);

        // Shouldn't be able to mount the same volume with read-write
        // access immediately after the volume was rebooted
        service2.SendMountVolumeRequest(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
        {
            auto response = service2.RecvMountVolumeResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        runtime.UpdateCurrentTime(runtime.GetCurrentTime() + unmountClientsTimeout);

        // Should now be able to mount the volume as the first service
        // should have timed out
        service2.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
    }

    Y_UNIT_TEST(ShouldCauseRemountWhenVolumeIsStolen)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        TServiceClient service2(runtime, nodeIdx2);

        TActorId volumeActorId;
        TActorId startVolumeActorId;
        ui64 volumeTabletId = 0;
        bool startVolumeActorStopped = false;

        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        volumeActorId = msg->VolumeActor;
                        volumeTabletId = msg->TabletId;
                        startVolumeActorId = event->Sender;
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped:
                        startVolumeActorStopped = true;
                        break;
                }

                return false;
            });

        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        TString sessionId;
        {
            auto response = service1.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                0, // mountFlags
                0, // mountSeqNumber
                NProto::TEncryptionDesc(),
                0,  // fillSeqNumber
                0   // fillGeneration
            );
            sessionId = response->Record.GetSessionId();
        }

        service2.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0, // mountFlags
            1, // mountSeqNumber
            NProto::TEncryptionDesc(),
            0,  // fillSeqNumber
            0   // fillGeneration
        );

        // Wait until start volume actor is stopped
        if (!startVolumeActorStopped) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvStartVolumeActorStopped);
            runtime.DispatchEvents(options);
            UNIT_ASSERT(startVolumeActorStopped);
        }

        // Next write request should cause invalid session response
        {
            service1.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                sessionId,
                char(1));
            auto response = service1.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_INVALID_SESSION,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        // Should work again after remount
        {
            auto response = service1.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                0, // mountFlags
                2, // mountSeqNumber
                NProto::TEncryptionDesc(),
                0,  // fillSeqNumber
                0   // fillGeneration
            );
            sessionId = response->Record.GetSessionId();
        }

        service1.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId);
        service1.UnmountVolume(DefaultDiskId, sessionId);
    }

    Y_UNIT_TEST(ShouldDisallowMountByAnotherClient)
    {
        static constexpr TDuration mountVolumeTimeout = TDuration::Seconds(3);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetInactiveClientsTimeout(
            mountVolumeTimeout.MilliSeconds());

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, std::move(storageServiceConfig));

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");
        service1.MountVolume(DefaultDiskId, "foo", "bar");

        TServiceClient service2(runtime, nodeIdx);
        service2.SendMountVolumeRequest(DefaultDiskId, "foo", "bar");
        auto response = service2.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_BS_MOUNT_CONFLICT);
    }

    Y_UNIT_TEST(ShouldProcessMountAndUnmountRequestsSequentially)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TVector<TServiceClient> services;
        size_t numServices = 16;
        services.reserve(numServices);

        enum class EEventType
        {
            ADD_CLIENT_REQUEST,
            ADD_CLIENT_RESPONSE,
            REMOVE_CLIENT_REQUEST,
            REMOVE_CLIENT_RESPONSE
        };

        TVector<EEventType> events;
        THashSet<TActorId> addClientRequestSenders;
        THashSet<TActorId> removeClientRequestSenders;

        // Collect events (considering deduplication of forwarding through pipe)
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvAddClientRequest: {
                        events.emplace_back(EEventType::ADD_CLIENT_REQUEST);
                        addClientRequestSenders.emplace(event->Sender);
                        break;
                    }
                    case TEvVolume::EvAddClientResponse: {
                        if (addClientRequestSenders.contains(event->Recipient)) {
                            events.emplace_back(EEventType::ADD_CLIENT_RESPONSE);
                        }
                        break;
                    }
                    case TEvVolume::EvRemoveClientRequest: {
                        events.emplace_back(EEventType::REMOVE_CLIENT_REQUEST);
                        removeClientRequestSenders.emplace(event->Sender);
                        break;
                    }
                    case TEvVolume::EvRemoveClientResponse: {
                        if (removeClientRequestSenders.contains(event->Recipient)) {
                            events.emplace_back(EEventType::REMOVE_CLIENT_RESPONSE);
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // Send mount requests from half of services, then start sending
        // mount requests from the other half and also sending unmount requests
        // from the first half of services
        TString sessionId;
        for (size_t i = 0; i < numServices; ++i) {
            TServiceClient service(runtime, nodeIdx);
            if (i == 0) {
                service.CreateVolume();
                service.AssignVolume(DefaultDiskId, "foo", "bar");
                service.WaitForVolume();
            }

            service.SendMountVolumeRequest(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_REMOTE);

            services.emplace_back(service);

            if (i < numServices / 2) {
                continue;
            }

            // Need to receive at least one mount response for session id
            // before sending unmount requests
            if (!sessionId) {
                auto response = services[0].RecvMountVolumeResponse();
                UNIT_ASSERT_C(
                    SUCCEEDED(response->GetStatus()),
                    response->GetErrorReason());
                sessionId = response->Record.GetSessionId();
            }

            auto& prevService = services[i-numServices/2];
            prevService.SendUnmountVolumeRequest(DefaultDiskId, sessionId);
        }

        // Send unmount requests from the second half of services
        for (size_t i = numServices / 2; i < numServices; ++i) {
            auto& service = services[i];
            service.SendUnmountVolumeRequest(DefaultDiskId, sessionId);
        }

        // Receive mount and unmount responses
        for (size_t i = 0; i < numServices; ++i) {
            if (i != 0) {
                auto response = services[i].RecvMountVolumeResponse();
                UNIT_ASSERT_C(
                    SUCCEEDED(response->GetStatus()),
                    response->GetErrorReason());
            }

            auto response = services[i].RecvUnmountVolumeResponse();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldFailVolumeMountIfDescribeVolumeFails)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto error = MakeError(E_ARGUMENT, "Error");
        ui32 counter = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        if (++counter != 2) {
                            break;
                        }
                        auto response = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == error.GetCode());
        UNIT_ASSERT(response->GetErrorReason() == error.GetMessage());
    }

    Y_UNIT_TEST(ShouldFailMountVolumeIfUnableToSetupVolumeClient)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto error = MakeError(E_ARGUMENT, "Error");

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        auto response = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldFailPendingMountVolumeIfUnableToSetupVolumeClient)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto error = MakeError(E_REJECTED, "Error");

        TActorId target;
        TActorId source;
        ui64 cookie = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        target = ev->Sender;
                        source = ev->Recipient;
                        cookie = ev->Cookie;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        service.SendMountVolumeRequest();
        service.SendMountVolumeRequest();
        service.SendMountVolumeRequest();

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvServicePrivate::EvInternalMountVolumeRequest,
            3);
        runtime.DispatchEvents(options);

        using TResponseType = TEvSSProxy::TEvDescribeVolumeResponse;
        auto response =
            std::make_unique<TResponseType>(error);

        runtime.Send(
            new IEventHandle(
                target,
                source,
                response.release(),
                0, // flags
                cookie),
            nodeIdx);

        auto response1 = service.RecvMountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(response1->GetStatus(), error.GetCode());

        auto response2 = service.RecvMountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(response2->GetStatus(), error.GetCode());

        auto response3 = service.RecvMountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(response3->GetStatus(), error.GetCode());
    }

    void FailVolumeMountIfDescribeVolumeReturnsWrongInfoCommon(
        std::function<void(NKikimrSchemeOp::TPathDescription&)> mutator)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        runtime.SetObserverFunc( [=] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        auto& pathDescription = const_cast<NKikimrSchemeOp::TPathDescription&>(msg->PathDescription);
                        mutator(pathDescription);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
        UNIT_ASSERT(response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldFailVolumeMountIfDescribeVolumeReturnsUnparsableMountToken)
    {
        FailVolumeMountIfDescribeVolumeReturnsWrongInfoCommon(
            [] (NKikimrSchemeOp::TPathDescription& pathDescription) {
                auto& volumeDescription = *pathDescription.MutableBlockStoreVolumeDescription();
                volumeDescription.SetMountToken("some random string");
            });
    }

    Y_UNIT_TEST(ShouldFailVolumeMountIfWrongMountTokenIsUsed)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        service.AssignVolume(DefaultDiskId, TString(), "some mount token");

        service.SendMountVolumeRequest(DefaultDiskId, TString(), "other mount token");
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
        UNIT_ASSERT(response->GetErrorReason().Contains("Mount token"));
    }

    Y_UNIT_TEST(ShouldAllowMountRequestsFromCtrlPlaneWithoutMountToken)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(
            runtime,
            nodeIdx,
            NProto::SOURCE_SECURE_CONTROL_CHANNEL);
        service.CreateVolume();

        service.AssignVolume(DefaultDiskId, TString(), "some mount token");

        service.MountVolume(DefaultDiskId);
    }

    Y_UNIT_TEST(ShouldRejectMountRequestsFromCtrlPlaneWithMountToken)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(
            runtime,
            nodeIdx,
            NProto::SOURCE_SECURE_CONTROL_CHANNEL);
        service.CreateVolume();

        service.AssignVolume(DefaultDiskId, TString(), "some mount token");

        service.SendMountVolumeRequest(DefaultDiskId, TString(), "some mount token");
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
        UNIT_ASSERT(response->GetErrorReason().Contains("Mount token"));
    }

    Y_UNIT_TEST(ShouldAllowUnmountOfNonMountedVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service11(env.GetRuntime(), nodeIdx);
        service11.CreateVolume();
        service11.WaitForVolume();

        {
            service11.SendUnmountVolumeRequest();
            auto response = service11.RecvUnmountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        TString sessionId11;
        {
            auto response = service11.MountVolume();
            sessionId11 = response->Record.GetSessionId();

            service11.ReadBlocks(DefaultDiskId, 0, sessionId11);
        }

        TServiceClient service12(env.GetRuntime(), nodeIdx);

        TString sessionId12;
        {
            auto response = service12.MountVolume(
                    DefaultDiskId,
                    TString(),
                    TString(),
                    NProto::IPC_GRPC,
                    NProto::VOLUME_ACCESS_READ_ONLY,
                    NProto::VOLUME_MOUNT_REMOTE);
            sessionId12 = response->Record.GetSessionId();

            service12.ReadBlocks(DefaultDiskId, 0, sessionId12);
        }

        service11.UnmountVolume();
        service11.WaitForVolume();

        {
            service11.SendUnmountVolumeRequest();
            auto response = service11.RecvUnmountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        service12.ReadBlocks(DefaultDiskId, 0, sessionId12);
    }

    Y_UNIT_TEST(ShouldFailUnmountIfRemoveClientFails)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        auto sessionId = service.MountVolume()->Record.GetSessionId();

        auto error = MakeError(E_ARGUMENT, "Error");

        runtime.SetObserverFunc( [nodeIdx, error, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvRemoveClientResponse: {
                        auto response = std::make_unique<TEvVolume::TEvRemoveClientResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Recipient,
                                event->Sender,
                                response.release(),
                                0, // flags,
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    };
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendUnmountVolumeRequest(DefaultDiskId, sessionId);
        auto response = service.RecvUnmountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            error.GetCode(),
            response->GetStatus(),
            response->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            error.GetMessage(),
            response->GetErrorReason()
        );
    }

    Y_UNIT_TEST(ShouldFailUnmountIfStopVolumeFails)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        auto sessionId = service.MountVolume()->Record.GetSessionId();

        auto error = MakeError(E_ARGUMENT, "Error");

        runtime.SetObserverFunc( [nodeIdx, error, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvStopVolumeResponse: {
                        auto response = std::make_unique<TEvServicePrivate::TEvStopVolumeResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Recipient,
                                event->Sender,
                                response.release(),
                                0, // flags,
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendUnmountVolumeRequest(DefaultDiskId, sessionId);
        auto response = service.RecvUnmountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            error.GetCode(),
            response->GetStatus(),
            response->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            error.GetMessage(),
            response->GetErrorReason()
        );
    }

    Y_UNIT_TEST(ShouldHandleMountAfterUnmountInFlight)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.MountVolume();

        TAutoPtr<IEventHandle> unmountProcessedEvent;
        ui32 mountVolumeRequestsCount = 0;
        bool onceUnmountProcessedEventSent = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvServicePrivate::EvInternalMountVolumeRequest: {
                        UNIT_ASSERT_VALUES_EQUAL(
                            1,
                            ++mountVolumeRequestsCount);
                        break;
                    }
                    case TEvServicePrivate::EvUnmountRequestProcessed: {
                        if (!unmountProcessedEvent &&
                            !onceUnmountProcessedEventSent)
                        {
                            unmountProcessedEvent = ev;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                    default: {
                        break;
                    }
                }
                if (!onceUnmountProcessedEventSent &&
                    unmountProcessedEvent &&
                    mountVolumeRequestsCount)
                {
                    onceUnmountProcessedEventSent = true;
                    runtime.Send(unmountProcessedEvent.Release(), nodeIdx);
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        service.SendUnmountVolumeRequest();
        service.SendMountVolumeRequest();

        TDispatchOptions options;
        runtime.DispatchEvents(options, TDuration::Seconds(1));

        {
            auto response = service.RecvUnmountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        {
            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }
    }

    Y_UNIT_TEST(ShouldReturnAlreadyMountedOnRemountIfNoMountOptionsChanged)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.MountVolume();

        {
            auto response = service.MountVolume();
            // XXX this behaviour doesn't seem to be correct (qkrorlqr@)
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,   // not 'already' because of a pipe reset event
                        // that happens after the first AddClient for local
                        // mounts
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        {
            auto response = service.MountVolume();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }
    }

    Y_UNIT_TEST(ShouldProcessRemountFromLocalToRemote)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.MountVolume();

        ui32 waitReadyCnt = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvWaitReadyRequest: {
                        ++waitReadyCnt;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason()
        );  // Must not be S_ALREADY

        UNIT_ASSERT_VALUES_UNEQUAL(0, waitReadyCnt);
    }

    Y_UNIT_TEST(ShouldProcessRemountFromReadWriteToReadOnly)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.MountVolume();

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,   // Must not be S_ALREADY
            response->GetStatus(),
            response->GetErrorReason()
        );
    }

    Y_UNIT_TEST(ShouldProcessRemountFromRemoteToLocalWithoutOtherClients)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        auto readWriteAccessOptions = {
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_ACCESS_REPAIR,
        };

        for (auto readWriteAccessOption: readWriteAccessOptions) {
            service.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                readWriteAccessOption,
                NProto::VOLUME_MOUNT_REMOTE);

            service.WaitForVolume();

            auto response = service.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                readWriteAccessOption,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());  // Must not be S_ALREADY
        }
    }

    Y_UNIT_TEST(ShouldRejectRemountFromRemoteToLocalWithAnotherLocalMounter)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");
        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        TServiceClient service2(runtime, nodeIdx);
        service2.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        service1.SendMountVolumeRequest(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);
        auto response = service1.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_BS_MOUNT_CONFLICT);
    }

    Y_UNIT_TEST(ShouldProcessRemountFromReadOnlyToReadWriteWithoutOtherClients)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        auto localMountOptions = {
            NProto::VOLUME_MOUNT_LOCAL,
            NProto::VOLUME_MOUNT_REMOTE
        };

        for (auto localMountOption: localMountOptions) {
            {
                auto mountResponse = service.MountVolume(
                    DefaultDiskId,
                    "foo",
                    "bar",
                    NProto::IPC_GRPC,
                    NProto::VOLUME_ACCESS_READ_ONLY,
                    localMountOption);

                if (localMountOption == NProto::VOLUME_MOUNT_REMOTE) {
                    service.WaitForVolume();
                }

                service.SendWriteBlocksRequest(
                    DefaultDiskId,
                    TBlockRange64::WithLength(0, 1024),
                    mountResponse->Record.GetSessionId());

                auto writeResponse = service.RecvWriteBlocksResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    writeResponse->GetStatus(),
                    writeResponse->GetErrorReason()
                );
            }

            TString sessionId;
            {
                auto mountResponse = service.MountVolume(
                    DefaultDiskId,
                    "foo",
                    "bar",
                    NProto::IPC_GRPC,
                    NProto::VOLUME_ACCESS_READ_WRITE,
                    localMountOption
                );

                service.SendWriteBlocksRequest(
                    DefaultDiskId,
                    TBlockRange64::WithLength(0, 1024),
                    mountResponse->Record.GetSessionId());

                auto writeResponse = service.RecvWriteBlocksResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    writeResponse->GetStatus(),
                    writeResponse->GetErrorReason()
                );

                sessionId = mountResponse->Record.GetSessionId();
            }

            service.UnmountVolume(DefaultDiskId, sessionId);
        }
    }

    Y_UNIT_TEST(ShouldRejectRemountFromRemoteToLocalWithAnotherWriter)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        TServiceClient service2(runtime, nodeIdx);
        service2.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        service1.SendMountVolumeRequest(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
        auto response = service1.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_BS_MOUNT_CONFLICT);
    }

    Y_UNIT_TEST(ShouldRejectRemountFromReadOnlyToReadWriteWithAnotherWriter)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        TServiceClient service2(runtime, nodeIdx);
        service2.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        service1.SendMountVolumeRequest(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
        auto response = service1.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_BS_MOUNT_CONFLICT);
    }

    Y_UNIT_TEST(ShouldStopVolumeTabletAfterLocalStartBecauseOfTimedoutAddClient)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        auto& runtime = env.GetRuntime();

        ui32 numCalls = 0;
        ui32 stopSeen = 0;
        ui32 startSeen = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvAddClientRequest: {
                        if (!numCalls++) {
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                    case TEvServicePrivate::EvStopVolumeRequest: {
                        ++stopSeen;
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeRequest: {
                        ++startSeen;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        auto response1 = service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        TString sessionId1 = response1->Record.GetSessionId();
        UNIT_ASSERT(startSeen == 1);
        UNIT_ASSERT(stopSeen == 1);

        service1.WaitForVolume(DefaultDiskId);
        service1.ReadBlocks(DefaultDiskId, 0, sessionId1);
    }

    Y_UNIT_TEST(ShouldReleaseVolumeTabletIfRemountFromLocalToRemote)
    {
        TTestEnv env;
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.MountVolume();

        bool stopResponseSeen = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvStopVolumeResponse: {
                        stopResponseSeen = true;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        TString sessionId = response->Record.GetSessionId();
        UNIT_ASSERT(response->GetStatus() == S_OK);
        UNIT_ASSERT(stopResponseSeen == true);
    }

    Y_UNIT_TEST(ShouldHandleLocalMigrationScenario)
    {
        TTestEnv env;
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service_source(runtime, nodeIdx);
        TServiceClient service_target(runtime, nodeIdx);
        service_source.CreateVolume();
        service_source.MountVolume();

        auto response = service_source.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        TString sessionId = response->Record.GetSessionId();
        UNIT_ASSERT(response->GetStatus() == S_OK);

        service_source.UnmountVolume(DefaultDiskId, sessionId);

        response = service_target.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == DefaultBlocksCount);

        response = service_target.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == DefaultBlocksCount);
    }

    Y_UNIT_TEST(ShouldHandleInterNodeMigrationScenario)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        ui32 nodeIdx2 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service_source(runtime, nodeIdx1);
        TServiceClient service_target(runtime, nodeIdx2);
        service_source.CreateVolume();
        service_source.MountVolume();

        auto response = service_source.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        TString sessionId = response->Record.GetSessionId();
        UNIT_ASSERT(response->GetStatus() == S_OK);
        UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == DefaultBlocksCount);

        response = service_target.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);
        UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == DefaultBlocksCount);

        service_source.UnmountVolume(DefaultDiskId, sessionId);

        response= service_target.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == DefaultBlocksCount);
    }

    Y_UNIT_TEST(ShouldnotRevomeVolumeIfLastClientUnmountedWithError)
    {
        TTestEnv env;
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.SetClientId("0");
        service.MountVolume();

        // Collect events (considering deduplication of forwarding through pipe)
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvRemoveClientRequest: {
                        auto error = MakeError(E_REJECTED, "");
                        auto response = std::make_unique<TEvVolume::TEvRemoveClientResponse>(error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        auto response = service.MountVolume(DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlocksCount, response->Record.GetVolume().GetBlocksCount());

        service.SendUnmountVolumeRequest(
            DefaultDiskId,
            response->Record.GetSessionId());

        auto unmountResponse = service.RecvUnmountVolumeResponse();
        UNIT_ASSERT(FAILED(unmountResponse->GetStatus()));

        service.SetClientId("1");

        service.SendMountVolumeRequest(DefaultDiskId);

        response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            "Volume \"path_to_test_volume\" already has connection with read-write access: 0",
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldResetMountSeqNumberOnUnmount)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        ui32 nodeIdx2 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        TServiceClient service2(runtime, nodeIdx2);
        service1.CreateVolume();
        service1.MountVolume();

        auto response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            true,
            0
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response1->GetStatus(),
            response1->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlocksCount,
            response1->Record.GetVolume().GetBlocksCount()
        );

        service1.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            response1->Record.GetSessionId());

        auto response2 = service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            true,
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlocksCount,
            response2->Record.GetVolume().GetBlocksCount()
        );

        service2.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            response2->Record.GetSessionId());

        {
            service1.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                response1->Record.GetSessionId(),
                char(1));
            auto response = service1.RecvWriteBlocksResponse();
            UNIT_ASSERT_C(
                FAILED(response->GetStatus()),
                response->GetErrorReason()
            );
        }

        {
            service1.SendMountVolumeRequest(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);
            {
                auto response = service1.RecvMountVolumeResponse();
                UNIT_ASSERT_C(
                    FAILED(response->GetStatus()),
                    response->GetErrorReason()
                );
            }
        }

        service2.UnmountVolume(DefaultDiskId, response2->Record.GetSessionId());

        response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            true,
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlocksCount,
            response1->Record.GetVolume().GetBlocksCount()
        );

        service1.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            response1->Record.GetSessionId()
        );
    }

    Y_UNIT_TEST(ShouldHandleMountRequestWithMountSeqNumber)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        ui32 nodeIdx2 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        TServiceClient service2(runtime, nodeIdx2);
        service1.CreateVolume();
        service1.MountVolume();

        auto response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            true,
            0);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response1->GetStatus(),
            response1->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlocksCount,
            response1->Record.GetVolume().GetBlocksCount()
        );

        service1.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            response1->Record.GetSessionId()
        );

        auto response2 = service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            true,
            1
        );
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlocksCount,
            response2->Record.GetVolume().GetBlocksCount()
        );

        service2.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            response2->Record.GetSessionId()
        );

        {
            service1.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                response1->Record.GetSessionId(),
                char(1)
            );
            auto response = service1.RecvWriteBlocksResponse();
            UNIT_ASSERT_C(
                FAILED(response->GetStatus()),
                response->GetErrorReason()
            );
        }

        {
            service1.SendMountVolumeRequest(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL
            );

            {
                auto response = service1.RecvMountVolumeResponse();
                UNIT_ASSERT_C(
                    FAILED(response->GetStatus()),
                    response->GetErrorReason()
                );
            }
        }
    }

    Y_UNIT_TEST(ShouldReportMountSeqNumberInStatVolumeResponse)
    {
        TTestEnv env;
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.MountVolume();

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            true,
            1
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlocksCount,
            response->Record.GetVolume().GetBlocksCount()
        );

        auto statResponse = service.StatVolume(DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL_C(
            1,
            statResponse->Record.GetMountSeqNumber(),
            statResponse->GetErrorReason()
        );
    }

    void ShouldProperlyReactToAcquireDiskError(NProto::EVolumeMountMode mountMode)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        config.SetAcquireNonReplicatedDevices(true);
        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            1_GB / DefaultBlockSize,
            DefaultBlockSize,
            TString(), // folderId
            TString(), // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        int acquireResps = 0;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvAcquireDiskResponse: {
                        ++acquireResps;
                        auto* msg = event->Get<TEvDiskRegistry::TEvAcquireDiskResponse>();
                        msg->Record.Clear();
                        msg->Record.MutableError()->SetCode(E_FAIL);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        {
            service.SendMountVolumeRequest(
                DefaultDiskId,
                "", // instanceId
                "", // token
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                mountMode
            );

            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(1, acquireResps);
            UNIT_ASSERT_VALUES_EQUAL(E_FAIL, response->GetStatus());
        }

        env.GetRuntime().SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        {
            service.SendMountVolumeRequest(
                DefaultDiskId,
                "", // instanceId
                "", // token
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                mountMode
            );

            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            const auto& volume = response->Record.GetVolume();
            UNIT_ASSERT_VALUES_EQUAL(8, volume.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "transport0",
                volume.GetDevices(0).GetTransportId()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                128_MB / DefaultBlockSize,
                volume.GetDevices(0).GetBlockCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                100500,
                volume.GetDevices(0).GetPhysicalOffset()
            );
        }
    }

    Y_UNIT_TEST(ShouldProperlyReactToAcquireDiskErrorLocal)
    {
        ShouldProperlyReactToAcquireDiskError(NProto::VOLUME_MOUNT_LOCAL);
    }

    Y_UNIT_TEST(ShouldProperlyReactToAcquireDiskErrorRemote)
    {
        ShouldProperlyReactToAcquireDiskError(NProto::VOLUME_MOUNT_REMOTE);
    }

    void DoTestShouldChangeVolumeBindingForMountedVolume(TTestEnv& env, ui32 nodeIdx)
    {
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        service.WaitForVolume(DefaultDiskId);
        service.ReadBlocks(DefaultDiskId, 0, sessionId);

        {
            auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                DefaultDiskId,
                EChangeBindingOp::RELEASE_TO_HIVE,
                NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
        }

        service.WaitForVolume(DefaultDiskId);
        service.ReadBlocks(DefaultDiskId, 0, sessionId);

        service.UnmountVolume(DefaultDiskId, sessionId);
    }

    Y_UNIT_TEST(ShouldChangeVolumeBindingForMountedVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        DoTestShouldChangeVolumeBindingForMountedVolume(env, nodeIdx);
    }

    void DoTestShouldRejectChangeVolumeBindingForUnknownVolume(TTestEnv& env, ui32 nodeIdx)
    {
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        service.ReadBlocks(DefaultDiskId, 0, sessionId);

        {
            auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                "Unknown",
                EChangeBindingOp::RELEASE_TO_HIVE,
                NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
            UNIT_ASSERT_C(
                FAILED(response->GetStatus()),
                "ChangeVolumeBindingRequest request unexpectedly succeeded");
        }
        service.UnmountVolume(DefaultDiskId, sessionId);
    }

    Y_UNIT_TEST(ShouldRejectChangeVolumeBindingForUnknownVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        DoTestShouldRejectChangeVolumeBindingForUnknownVolume(env, nodeIdx);
    }

    void DoTestShouldBringRemotelyMountedVolumeBack(TTestEnv& env, ui32 nodeIdx)
    {
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        service.WaitForVolume(DefaultDiskId);
        service.ReadBlocks(DefaultDiskId, 0, sessionId);

        {
            auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                DefaultDiskId,
                EChangeBindingOp::RELEASE_TO_HIVE,
                NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
        }

        service.WaitForVolume(DefaultDiskId);
        service.ReadBlocks(DefaultDiskId, 0, sessionId);

        {
            auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                DefaultDiskId,
                EChangeBindingOp::ACQUIRE_FROM_HIVE,
                NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
        }

        service.UnmountVolume(DefaultDiskId, sessionId);
    }

    Y_UNIT_TEST(ShouldBringRemotelyMountedVolumeBack)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        DoTestShouldBringRemotelyMountedVolumeBack(env, nodeIdx);
    }

    void DoTestShouldSucceedSettingRemoteBindingForRemotelyMountedVolume(
        TTestEnv& env,
        ui32 nodeIdx)
    {
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        service.WaitForVolume(DefaultDiskId);
        service.ReadBlocks(DefaultDiskId, 0, sessionId);

        {
            auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                DefaultDiskId,
                EChangeBindingOp::RELEASE_TO_HIVE,
                NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
        }

        service.WaitForVolume(DefaultDiskId);
        service.ReadBlocks(DefaultDiskId, 0, sessionId);

        {
            auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                DefaultDiskId,
                EChangeBindingOp::ACQUIRE_FROM_HIVE,
                NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
        }

        service.UnmountVolume(DefaultDiskId, sessionId);
    }

    void DoTestShouldAllowRemoteMountIfBindingSetToLocal(TTestEnv& env, ui32 nodeIdx)
    {
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");
        auto response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        {
            auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                DefaultDiskId,
                EChangeBindingOp::ACQUIRE_FROM_HIVE,
                NProto::EPreemptionSource::SOURCE_BALANCER);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
        }

        response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        auto sessionId = response->Record.GetSessionId();
        service.UnmountVolume(DefaultDiskId, sessionId);

        response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
    }

    Y_UNIT_TEST(ShouldAllowRemoteMountIfBindingSetToLocal)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx = SetupTestEnv(env);

        DoTestShouldAllowRemoteMountIfBindingSetToLocal(env, nodeIdx);
    }

    void DoTestShouldAllowLocalMountIfBindingSetToRemote(TTestEnv& env, ui32 nodeIdx)
    {
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");
        auto response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        {
            auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                DefaultDiskId,
                EChangeBindingOp::RELEASE_TO_HIVE,
                NProto::EPreemptionSource::SOURCE_BALANCER);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
        }

        response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto sessionId = response->Record.GetSessionId();
        service.UnmountVolume(DefaultDiskId, sessionId);

        response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
    }

    Y_UNIT_TEST(ShouldAllowLocalMountIfBindingSetToRemote)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx = SetupTestEnv(env);

        DoTestShouldAllowLocalMountIfBindingSetToRemote(env, nodeIdx);
    }

    Y_UNIT_TEST(ShouldUseVolumeClientForRequestsIfVolumeWasMounted)
    {
        TTestEnv env;
        auto unmountClientsTimeout = TDuration::Seconds(9);
        ui32 nodeIdx = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        ui64 volumeTabletId = 0;
        TActorId volumeActorId;
        auto& runtime = env.GetRuntime();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        volumeActorId = msg->VolumeActor;
                        volumeTabletId = msg->TabletId;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        TString sessionId;
        service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        if (!volumeTabletId) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvSSProxy::EvDescribeVolumeResponse);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT(volumeTabletId);
        UNIT_ASSERT(volumeActorId);

        runtime.Send(new IEventHandle(volumeActorId, service.GetSender(), new TEvents::TEvPoisonPill()), nodeIdx);
        TDispatchOptions rebootOptions;
        rebootOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
        runtime.DispatchEvents(rebootOptions);

        service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        service.GetChangedBlocks(DefaultDiskId, 0, 1);
    }

    Y_UNIT_TEST(ShouldUseVolumeClientForRequestsIfVolumeWasMountedRemotely)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(9);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        ui64 volumeTabletId = 0;
        auto& runtime = env.GetRuntime();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(runtime, nodeIdx1);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        TString sessionId;
        service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        if (!volumeTabletId) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvSSProxy::EvDescribeVolumeResponse);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT(volumeTabletId);

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx1);

        service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        service.WaitForVolume(DefaultDiskId);
        service.GetChangedBlocks(DefaultDiskId, 0, 1);
    }

    Y_UNIT_TEST(ShouldFailVolumeMountIfWrongEncryptionKeyHashIsUsed)
    {
        auto keyHash = "01234567890123456789012345678901";

        NProto::TEncryptionSpec encryptionSpec;
        encryptionSpec.SetMode(NProto::ENCRYPTION_AES_XTS);
        encryptionSpec.SetKeyHash(keyHash);

        NProto::TEncryptionDesc encryptionDesc;
        encryptionDesc.SetMode(encryptionSpec.GetMode());
        encryptionDesc.SetKeyHash(encryptionSpec.GetKeyHash());

        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetDefaultTabletVersion(2);
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            service.SendCreateVolumeRequest(
                DefaultDiskId,
                DefaultBlocksCount,
                DefaultBlockSize,
                "",  // folderId
                "",  // cloudId
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                NProto::TVolumePerformanceProfile(),
                "",  // placementGroupId
                0,  // placementPartitionIndex
                0,
                encryptionSpec);
            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto encryption = encryptionDesc;
            encryption.SetKeyHash(TString("wrong") + encryption.GetKeyHash());
            service.SendMountVolumeRequest(
                DefaultDiskId,
                "",
                "",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                false,
                0,
                encryption);
            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason()
            );
            UNIT_ASSERT_C(
                response->GetErrorReason().Contains("encryption key hashes"),
                response->GetErrorReason()
            );
        }

        {
            service.SendMountVolumeRequest(
                DefaultDiskId,
                "",
                "",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                false,
                0,
                encryptionDesc);
            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }
    }

    Y_UNIT_TEST(ShouldRemoveVolumeFromServiceEvenIfUnmountFailed)
    {
        static constexpr TDuration mountVolumeTimeout = TDuration::Seconds(1);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetInactiveClientsTimeout(mountVolumeTimeout.MilliSeconds());

        TTestEnv env;
        ui32 nodeIdx1 = SetupTestEnv(env, std::move(storageServiceConfig));

        auto& runtime = env.GetRuntime();
        bool dieSeen = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvRemoveClientRequest: {
                        auto msg = std::make_unique<TEvVolume::TEvRemoveClientResponse>(
                            MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                msg.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx1);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    case TEvServicePrivate::EvSessionActorDied: {
                        dieSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        service1.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "foo",
            "bar"
        );

        service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL
        );

        service1.DestroyVolume(DefaultDiskId);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvService::EvUnregisterVolume);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                runtime.DispatchEvents(options));
        }

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvService::EvUnmountVolumeRequest);
            options.FinalEvents.emplace_back(
                TEvServicePrivate::EvSessionActorDied);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                runtime.DispatchEvents(options));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            dieSeen);
    }

    Y_UNIT_TEST(ShouldAllowToIncreaseSeqNumWithNonLocalReadOnlyMounts)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        TServiceClient service2(env.GetRuntime(), nodeIdx2);

        service1.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "foo",
            "bar"
        );

        auto response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );
        auto session1 = response1->Record.GetSessionId();

        service1.WriteBlocks(DefaultDiskId, TBlockRange64::MakeOneBlock(0), session1, 1);

        auto response2 = service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            false,
            1);
        auto session2 = response2->Record.GetSessionId();

        service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            false,
            1);

        service2.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::MakeOneBlock(0),
            session2,
            1);
    }

    Y_UNIT_TEST(ShouldNotUnmountInactiveClientIfThereArePendingMountUnmountRequests)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetInitialAddClientTimeout(
            TDuration::Seconds(5).MilliSeconds()
        );

        ui32 nodeIdx1 = SetupTestEnv(env, std::move(storageServiceConfig));

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        TServiceClient service2(env.GetRuntime(), nodeIdx1);

        ui64 volumeTabletId = 0;
        auto& runtime = env.GetRuntime();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service1.CreateVolume();

        auto sessionId1 = service1.MountVolume()->Record.GetSessionId();
        auto sessionId2 = service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE)->Record.GetSessionId();

        RebootTablet(runtime, volumeTabletId, service1.GetSender(), nodeIdx1);

        runtime.AdvanceCurrentTime(TDuration::Seconds(4));

        service1.SendMountVolumeRequest();

        TAutoPtr<IEventHandle> savedEvent;
        bool unmountSeen = false;
        bool responseCatched = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvAddClientResponse: {
                        if (!savedEvent && !responseCatched) {
                            savedEvent = event;
                            responseCatched = true;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                    case TEvService::EvUnmountVolumeRequest: {
                        unmountSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service2.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        runtime.AdvanceCurrentTime(TDuration::Seconds(6));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvAddClientRequest);
        runtime.DispatchEvents(options);

        UNIT_ASSERT(savedEvent);
        runtime.Send(savedEvent.Release(), nodeIdx1);

        service1.RecvMountVolumeResponse();
        service2.RecvMountVolumeResponse();

        UNIT_ASSERT_VALUES_EQUAL(false, unmountSeen);

        service1.UnmountVolume(DefaultDiskId, sessionId1);
        service2.UnmountVolume(DefaultDiskId, sessionId2);
    }

    Y_UNIT_TEST(ShouldNotRunVolumesLocallyIfRemoteMountOnly)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetRemoteMountOnly(true);

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, std::move(storageServiceConfig));

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();
        service.AssignVolume();

        ui32 localStartSeen = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvStartVolumeResponse: {
                        ++localStartSeen;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume();

        UNIT_ASSERT_VALUES_EQUAL(0, localStartSeen);
    }

    Y_UNIT_TEST(ShouldExtendAddClientTimeoutForVolumesPreviouslyMountedLocal)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();
        service.AssignVolume();

        ui64 volumeTabletId = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume();

        bool startVolumeSeen = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvAddClientRequest: {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    case TEvServicePrivate::EvStartVolumeRequest: {
                        startVolumeSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendMountVolumeRequest();

        TDispatchOptions options;
        runtime.DispatchEvents(options, TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(0, startVolumeSeen);
    }

    Y_UNIT_TEST(ShouldNotLockVolumeOnRemount)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        ui64 lockTabletRequestCount = 0;

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvLockTabletRequest: {
                        ++lockTabletRequestCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, lockTabletRequestCount);

        service.MountVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, lockTabletRequestCount);
    }

    Y_UNIT_TEST(ShouldLockVolumeOnRemountAfterLockLost)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetDoNotStopVolumeTabletOnLockLost(true);

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, storageServiceConfig);

        auto& runtime = env.GetRuntime();

        TActorId startVolumeActorId;
        ui64 volumeTabletId = 0;
        ui64 lockTabletRequestCount = 0;

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvLockTabletRequest: {
                        startVolumeActorId = event->Sender;
                        ++lockTabletRequestCount;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, lockTabletRequestCount);

        service.SendRequest(
            startVolumeActorId,
            std::make_unique<TEvHiveProxy::TEvTabletLockLost>(volumeTabletId));

        // Mount volume fails with error because Hive does not lost locks in
        // reality.
        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            "Concurrent lock requests detected", response->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(2, lockTabletRequestCount);
    }

    Y_UNIT_TEST(ShouldStopVolumeIfItWasDemotedByStateStorage)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetDoNotStopVolumeTabletOnLockLost(true);

        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env, storageServiceConfig);
        ui32 nodeIdx2 = SetupTestEnv(env, storageServiceConfig);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        TServiceClient service2(runtime, nodeIdx2);

        TVector<TActorId> volumeActorIds;
        ui64 volumeDemotedByStateStorageCount = 0;

        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        volumeActorIds.push_back(msg->VolumeActor);
                        break;
                    }
                    case TEvTablet::EvTabletDead: {
                        auto* msg = event->Get<TEvTablet::TEvTabletDead>();
                        if (msg->Reason == TEvTablet::TEvTabletDead::ReasonDemotedByStateStorage
                            // Ignore events that were sent to the volume.
                            && Find(volumeActorIds.begin(), volumeActorIds.end(), event->Recipient) ==
                            volumeActorIds.end())
                        {
                            ++volumeDemotedByStateStorageCount;
                        }
                        break;
                    }
                }

                return false;
            });

        service1.CreateVolume();
        service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0, // mountFlags
            0, // mountSeqNumber
            NProto::TEncryptionDesc(),
            0,  // fillSeqNumber
            0   // fillGeneration
        );

        service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0, // mountFlags
            1, // mountSeqNumber
            NProto::TEncryptionDesc(),
            0,  // fillSeqNumber
            0   // fillGeneration
        );

        // If tablet on node with outdated mount does not stop then it will
        // compete with other mount.
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(volumeDemotedByStateStorageCount, 1);
    }

    Y_UNIT_TEST(ShouldWaitForAddClientAfterTabletUnlocking)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetInitialAddClientTimeout(
            TDuration::Seconds(30).MilliSeconds());

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, storageServiceConfig);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume();
        bool mountRequestProcessed = false;
        bool unlockRequestProcessed = false;

        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvUnlockTabletRequest: {
                        if (!unlockRequestProcessed) {
                            unlockRequestProcessed = true;

                            runtime.Schedule(
                                event,
                                TDuration::Seconds(10),
                                nodeIdx);
                            return true;
                        }
                        break;
                    }
                    case TEvServicePrivate::EvMountRequestProcessed: {
                        if (!mountRequestProcessed) {
                            auto* msg = event->Get<
                                TEvServicePrivate::TEvMountRequestProcessed>();
                            const_cast<NProto::TError&>(msg->Error) = MakeKikimrError(
                                NKikimrProto::ERROR);
                            mountRequestProcessed = true;
                        }
                        break;
                    }
                }

                return false;
            });

        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
        service.SendMountVolumeRequest();
        response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldShutdownVolumeAfterLockLostOnVolumeDeletionDuringMounting)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetDoNotStopVolumeTabletOnLockLost(true);

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, storageServiceConfig);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        service.SendMountVolumeRequest();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvWaitReadyRequest: {
                        // Let volume tablet start.
                        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1000));
                        service.SendDestroyVolumeRequest();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response1 = service.RecvDestroyVolumeResponse();
        UNIT_ASSERT(SUCCEEDED(response1->GetStatus()));

        auto response2 = service.RecvMountVolumeResponse();
        UNIT_ASSERT(SUCCEEDED(response2->GetStatus()));
    }

    Y_UNIT_TEST(ShouldNotSendVolumeMountedForPingMounts)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("vol1");
        service.AssignVolume("vol1");

        bool addClientSeen = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvAddClientRequest: {
                        addClientSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // test that we don't exceed limit during mount

        service.MountVolume("vol1");
        service.MountVolume("vol1");

        addClientSeen = false;
        service.MountVolume("vol1");
        UNIT_ASSERT_VALUES_EQUAL(false, addClientSeen);
    }

    Y_UNIT_TEST(ShouldNotExceedThresholdForNumberOfLocallyMountedVolumes)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetMaxLocalVolumes(2);

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, std::move(storageServiceConfig));

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("vol1");
        service.AssignVolume("vol1");

        service.CreateVolume("vol2");
        service.AssignVolume("vol2");

        service.CreateVolume("vol3");
        service.AssignVolume("vol3");

        service.CreateVolume("vol4-remote");
        service.AssignVolume("vol4-remote");

        bool startVolumeSeen = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvStartVolumeRequest: {
                        startVolumeSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // test that we don't exceed limit during mount

        auto sessionId1 = service.MountVolume("vol1")->Record.GetSessionId();
        UNIT_ASSERT_VALUES_EQUAL(true, startVolumeSeen);

        UNIT_ASSERT_VALUES_EQUAL(
            sessionId1,
            service.MountVolume("vol1")->Record.GetSessionId());

        startVolumeSeen = false;
        service.MountVolume(
            "vol4-remote",
            "",
            "",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
        UNIT_ASSERT_VALUES_EQUAL(false, startVolumeSeen);

        startVolumeSeen = false;
        auto sessionId2 = service.MountVolume("vol2")->Record.GetSessionId();
        UNIT_ASSERT_VALUES_EQUAL(true, startVolumeSeen);

        startVolumeSeen = false;
        auto sessionId3 = service.MountVolume("vol3")->Record.GetSessionId();
        UNIT_ASSERT_VALUES_EQUAL(false, startVolumeSeen);

        // test that subsequent mounts does not change situation
        startVolumeSeen = false;
        UNIT_ASSERT_VALUES_EQUAL(
            sessionId3,
            service.MountVolume("vol3")->Record.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(false, startVolumeSeen);

        // test that service tracks current number of locally mounted volumes

        service.UnmountVolume("vol2", sessionId2);

        startVolumeSeen = false;
        UNIT_ASSERT_VALUES_EQUAL(
            sessionId3,
            service.MountVolume("vol3")->Record.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(true, startVolumeSeen);

        startVolumeSeen = false;
        sessionId2 = service.MountVolume("vol2")->Record.GetSessionId();
        UNIT_ASSERT_VALUES_EQUAL(false, startVolumeSeen);
    }

    Y_UNIT_TEST(ShouldAllowSeveralVolumePipesFromSameClient)
    {
        TTestEnv env(1, 3);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);
        ui32 nodeIdx3 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        TServiceClient service2(env.GetRuntime(), nodeIdx2);
        TServiceClient service3(env.GetRuntime(), nodeIdx3);

        service2.SetClientId(service1.GetClientId());
        service3.SetClientId(service1.GetClientId());

        service1.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "foo",
            "bar"
        );

        auto response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );
        auto session1 = response1->Record.GetSessionId();

        service1.WriteBlocks(DefaultDiskId, TBlockRange64::MakeOneBlock(0), session1, 1);

        auto response2 = service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            false,
            1);
        auto session2 = response2->Record.GetSessionId();

        service2.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::MakeOneBlock(0),
            session2,
            1);

        {
            service1.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::MakeOneBlock(0),
                session1,
                1);

            auto response = service1.RecvWriteBlocksResponse();
            UNIT_ASSERT_C(
                FAILED(response->GetStatus()),
                "WriteBlocks request unexpectedly succeeded");
        }

        auto response3 = service3.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE
        );
        auto session3 = response3->Record.GetSessionId();

        service3.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::MakeOneBlock(0),
            session3,
            1);

        {
            service2.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::MakeOneBlock(0),
                session2,
                1);

            auto response = service2.RecvWriteBlocksResponse();
            UNIT_ASSERT_C(
                FAILED(response->GetStatus()),
                "WriteBlocks request unexpectedly succeeded");
        }
    }

    Y_UNIT_TEST(ShouldSuccessfullyUnmountIfVolumeDestroyed)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        TServiceClient service2(env.GetRuntime(), nodeIdx2);

        service1.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "foo",
            "bar"
        );

        auto response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        auto sessionId = response1->Record.GetSessionId();

        bool tabletDeadSeen = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTablet::EvTabletDead: {
                        tabletDeadSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service2.DestroyVolume();

        if (!tabletDeadSeen) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvTabletDead);
            env.GetRuntime().DispatchEvents(options);
        }

        auto response2 = service1.UnmountVolume(
            DefaultDiskId,
            sessionId);
        UNIT_ASSERT_C(
            SUCCEEDED(response2->GetStatus()),
            response2->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldRetryUnmountForServiceInitiatedUnmountsWithRetriableError)
    {
        static constexpr TDuration mountVolumeTimeout = TDuration::Seconds(3);

        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetInactiveClientsTimeout(mountVolumeTimeout.MilliSeconds());

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, std::move(storageServiceConfig));
        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();
        auto response = service.MountVolume();
        auto sessionId = response->Record.GetSessionId();

        bool reject = true;
        NProto::TError lastError;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvRemoveClientRequest: {
                        if (!reject) {
                            break;
                        }
                        reject = false;
                        auto error = MakeError(E_REJECTED, "Something went wrong:(");
                        auto response = std::make_unique<TEvVolume::TEvRemoveClientResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags,
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    };
                    case TEvServicePrivate::EvUnmountRequestProcessed: {
                        const auto* msg = event->Get<TEvServicePrivate::TEvUnmountRequestProcessed>();
                        lastError = msg->Error;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        runtime.UpdateCurrentTime(runtime.GetCurrentTime() + mountVolumeTimeout);
        // wait for rejected unmount
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvVolume::EvRemoveClientResponse);
            options.FinalEvents.emplace_back(TEvService::EvUnmountVolumeResponse);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, lastError.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "Something went wrong:(",
            lastError.GetMessage());

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvService::EvUnmountVolumeResponse);
            options.FinalEvents.emplace_back(TEvServicePrivate::EvSessionActorDied);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(S_OK, lastError.GetCode());
    }

    Y_UNIT_TEST(ShouldReplyWithRejectedWhenAddClientTimesOutAndConfigRequiresIt)
    {
        NProto::TStorageServiceConfig storageConfig;
        NProto::TFeaturesConfig featuresConfig;

        storageConfig.SetInitialAddClientTimeout(TDuration::Seconds(2).MilliSeconds());
        storageConfig.SetRejectMountOnAddClientTimeout(true);

        TTestEnv env;

        ui32 nodeIdx = SetupTestEnv(env, storageConfig, {});
        auto& runtime = env.GetRuntime();

        bool rejectAddClient = true;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvAddClientRequest: {
                        if (rejectAddClient) {
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());

        {
            rejectAddClient = false;
            auto response = service.MountVolume();
            auto sessionId = response->Record.GetSessionId();
            service.WaitForVolume(DefaultDiskId);
            service.ReadBlocks(DefaultDiskId, 0, sessionId);
        }
    }

    Y_UNIT_TEST(ShouldStopLocalTabletIfLocalMounterUnableToDescribeVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        ui64 volumeTabletId = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        bool check = false;
        bool unlockSeen = false;
        bool failDescribe = true;
        bool sessionDeathSeen = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        if (failDescribe) {
                            auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                            const_cast<NProto::TError&>(msg->Error) =
                                MakeError(E_REJECTED, "SS is dead");
                            }
                        break;
                    }
                    case TEvHiveProxy::EvUnlockTabletRequest: {
                        if (check) {
                            auto* msg = event->Get<TEvHiveProxy::TEvUnlockTabletRequest>();
                            if (msg->TabletId == volumeTabletId) {
                                unlockSeen = true;
                            }
                        }
                        break;
                    }
                    case TEvServicePrivate::EvSessionActorDied: {
                        sessionDeathSeen = true;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendMountVolumeRequest(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        check = true;
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(true, unlockSeen);
        UNIT_ASSERT_VALUES_EQUAL(true, sessionDeathSeen);

        failDescribe = false;
        auto mountResponse = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        service.ReadBlocks(DefaultDiskId, 0, mountResponse->Record.GetSessionId());
    }

    Y_UNIT_TEST(ShouldRestartVolumeClientIfUnmountComesFromMonitoring)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(env.GetRuntime(), nodeIdx);
        TServiceClient service2(env.GetRuntime(), nodeIdx);
        TServiceClient service3(env.GetRuntime(), nodeIdx);

        service1.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "foo",
            "bar"
        );

        TActorId volumeClient1;
        TActorId volumeClient2;
        bool volumeClientPillSeen = false;
        ui32 addCnt = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvAddClientRequest: {
                        if (!volumeClient1 || volumeClientPillSeen) {
                            ++addCnt;
                            if (addCnt == 1) {
                                if (!volumeClient1) {
                                    volumeClient1 = event->Recipient;
                                } else {
                                    volumeClient2 = event->Recipient;
                                }
                            }
                        }
                        break;
                    }
                    case TEvents::TSystem::PoisonPill: {
                        if (volumeClient1 && volumeClient1 == event->Recipient) {
                            volumeClientPillSeen = true;
                            addCnt = 0;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        auto sessionId = response1->Record.GetSessionId();

        service1.SendUnmountVolumeRequest(
            DefaultDiskId,
            sessionId,
            NProto::SOURCE_SERVICE_MONITORING);

        service2.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE
        );

        service3.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE
        );

        service1.RecvUnmountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(true, volumeClientPillSeen);

        service2.RecvMountVolumeResponse();
        UNIT_ASSERT_VALUES_UNEQUAL(volumeClient1, volumeClient2);

        service3.RecvMountVolumeResponse();
    }

    Y_UNIT_TEST(ShouldMoveTabletToControlIfLocalMounterCannotRunTabletLocallyDuringMigration)
    {
        TTestEnv env(1, 2);
        NProto::TStorageServiceConfig config;
        config.SetMaxLocalVolumes(1);
        ui32 nodeIdx1 = SetupTestEnv(env, config, {});
        ui32 nodeIdx2 = SetupTestEnv(env, config, {});

        auto& runtime = env.GetRuntime();

        TServiceClient serviceSource(runtime, nodeIdx1);
        TServiceClient serviceTarget(runtime, nodeIdx2);
        serviceSource.CreateVolume();

        serviceTarget.CreateVolume("FakeVolume");
        serviceTarget.MountVolume(
            "FakeVolume",
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto response = serviceSource.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        TString sessionId = response->Record.GetSessionId();
        UNIT_ASSERT(response->GetStatus() == S_OK);
        UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == DefaultBlocksCount);

        response = serviceTarget.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0,
            1);
        UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == DefaultBlocksCount);

        ui32 unlockCnt = 0;
        ui32 lockCnt = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvUnlockTabletRequest: {
                        ++unlockCnt;
                        break;
                    }
                    case TEvHiveProxy::EvLockTabletRequest: {
                        ++lockCnt;
                        break;
                    }

                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        response = serviceTarget.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0,
            1);

        UNIT_ASSERT_VALUES_EQUAL(1, unlockCnt);
        UNIT_ASSERT_VALUES_EQUAL(1, lockCnt);

        response = serviceTarget.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0,
            1);

        UNIT_ASSERT_VALUES_EQUAL(1, unlockCnt);
        UNIT_ASSERT_VALUES_EQUAL(1, lockCnt);
    }

    Y_UNIT_TEST(ShouldReleaseTabletIfLocalMounterFailsAtRemount)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        ui32 unlockCnt = 0;
        ui32 lockCnt = 0;
        bool failAddClient = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvUnlockTabletRequest: {
                        ++unlockCnt;
                        break;
                    }
                    case TEvHiveProxy::EvLockTabletRequest: {
                        ++lockCnt;
                        break;
                    }
                    case TEvVolume::EvAddClientRequest: {
                        if (failAddClient) {
                            auto response = std::make_unique<TEvVolume::TEvAddClientResponse>(
                                MakeError(E_BS_MOUNT_CONFLICT, "error"));
                            runtime.Send(
                                new IEventHandle(
                                    event->Sender,
                                    event->Recipient,
                                    response.release(),
                                    0, // flags
                                    event->Cookie),
                                nodeIdx);
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }

                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        UNIT_ASSERT_VALUES_EQUAL(0, unlockCnt);
        UNIT_ASSERT_VALUES_EQUAL(1, lockCnt);

        failAddClient = true;

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_BS_MOUNT_CONFLICT);
        UNIT_ASSERT_VALUES_EQUAL(1, unlockCnt);
        UNIT_ASSERT_VALUES_EQUAL(1, lockCnt);

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_BS_MOUNT_CONFLICT);
        UNIT_ASSERT_VALUES_EQUAL(1, unlockCnt);
        UNIT_ASSERT_VALUES_EQUAL(1, lockCnt);
    }

    Y_UNIT_TEST(ShouldMoveTabletToControlIfLocalMounterCannotRunTabletLocally)
    {
        TTestEnv env(1, 2);
        NProto::TStorageServiceConfig config;
        config.SetMaxLocalVolumes(1);
        config.SetDisableLocalService(true);
        ui32 nodeIdx1 = SetupTestEnv(env, config, {});

        config.SetDisableLocalService(false);
        SetupTestEnv(env, config, {});

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx1);
        service.CreateVolume();

        service.CreateVolume("FakeVolume");
        service.MountVolume(
            "FakeVolume",
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        ui32 unlockCnt = 0;
        ui32 lockCnt = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvUnlockTabletRequest: {
                        ++unlockCnt;
                        break;
                    }
                    case TEvHiveProxy::EvLockTabletRequest: {
                        ++lockCnt;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);
        TString sessionId = response->Record.GetSessionId();
        UNIT_ASSERT(response->GetStatus() == S_OK);
        UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == DefaultBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(1, unlockCnt);
        UNIT_ASSERT_VALUES_EQUAL(1, lockCnt);

        response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0,
            1);

        UNIT_ASSERT_VALUES_EQUAL(1, unlockCnt);
        UNIT_ASSERT_VALUES_EQUAL(1, lockCnt);
    }

    Y_UNIT_TEST(ShouldSkipCheckingMaxLocalVolumesForDiskRegistryVolumes)
    {
        TTestEnv env(1, 2);
        NProto::TStorageServiceConfig config;
        config.SetMaxLocalVolumes(1);
        config.SetAllocationUnitNonReplicatedSSD(1);
        ui32 nodeIdx1 = SetupTestEnv(env, config, {});

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx1);
        service.CreateVolume();
        service.CreateVolume(
            "DrVolume",
            1_GB / DefaultBlockSize,
            DefaultBlockSize,
            TString(), // folderId
            TString(), // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);

        auto response1 = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        ui32 lockCnt = 0;
        ui32 unlockCnt = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                     case TEvHiveProxy::EvUnlockTabletRequest: {
                        ++unlockCnt;
                        break;
                    }
                    case TEvHiveProxy::EvLockTabletRequest: {
                        ++lockCnt;
                        break;
                    }

                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response2 = service.MountVolume(
            "DrVolume",
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0,
            1);

        UNIT_ASSERT_VALUES_EQUAL(1, lockCnt);
        UNIT_ASSERT_VALUES_EQUAL(0, unlockCnt);


        service.UnmountVolume("DrVolume", response2->Record.GetSessionId());
        service.UnmountVolume(DefaultDiskId, response2->Record.GetSessionId());

        service.CreateVolume("GenericVolume");
        service.MountVolume(
            "GenericVolume",
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0,
            1);

        UNIT_ASSERT_VALUES_EQUAL(2, lockCnt);
        UNIT_ASSERT_VALUES_EQUAL(2, unlockCnt);
    }

    Y_UNIT_TEST(ShouldSuccessfullyUnmountIfDrVolumeDestroyed)
    {
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        config.SetAcquireNonReplicatedDevices(true);

        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            1_GB / DefaultBlockSize,
            DefaultBlockSize,
            TString(), // folderId
            TString(), // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        auto response1 = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        auto sessionId = response1->Record.GetSessionId();

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvReleaseDiskRequest: {
                        using TResponse =
                            TEvDiskRegistry::TEvReleaseDiskResponse;
                        auto response =
                            std::make_unique<TResponse>(
                                MakeError(E_NOT_FOUND, "Disk not found"));
                        env.GetRuntime().Send(
                            new IEventHandle(
                                ev->Sender,
                                ev->Recipient,
                                response.release(),
                                0, // flags
                                ev->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        service.DestroyVolume();

        service.UnmountVolume(
            DefaultDiskId,
            sessionId);
    }

    void CheckChangeVolumeBindingRequestDelay(
        TDuration configDelay,
        TDuration delayLowerBound,
        TDuration delayUpperBound,
        NProto::EPreemptionSource preemptionSource)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBalancerActionDelayInterval(
            configDelay.MilliSeconds());
        ui32 nodeIdx1 = SetupTestEnv(env, config, {});

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx1);
        service.CreateVolume();

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        auto response = service.MountVolume();

        auto sessionId = response->Record.GetSessionId();

        TVector<TMonotonic> tss;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                     case TEvService::EvChangeVolumeBindingRequest: {
                        auto value = runtime.GetCurrentMonotonicTime();
                        tss.push_back(value);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        {
            using TResponse = TEvService::TEvChangeVolumeBindingResponse;
            auto request =
                std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                    DefaultDiskId,
                    EChangeBindingOp::RELEASE_TO_HIVE,
                    preemptionSource);
            service.SendRequest(
                MakeStorageServiceId(),
                std::move(request));

            auto response = service.RecvResponse<TResponse>();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT(tss.size() >= 2);
            auto end = tss.back();
            tss.pop_back();
            auto start = tss.back();
            UNIT_ASSERT_LE((end - start), delayUpperBound);
            UNIT_ASSERT_GE((end - start), delayLowerBound);
        }

        {
            using TResponse = TEvService::TEvChangeVolumeBindingResponse;
            auto request =
                std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                    DefaultDiskId,
                    EChangeBindingOp::ACQUIRE_FROM_HIVE,
                    preemptionSource);
            service.SendRequest(
                MakeStorageServiceId(),
                std::move(request));

            auto response = service.RecvResponse<TResponse>();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT(tss.size() >= 2);
            auto end = tss.back();
            tss.pop_back();
            auto start = tss.back();
            UNIT_ASSERT_LE((end - start), delayUpperBound);
            UNIT_ASSERT_GE((end - start), delayLowerBound);
        }
    }

    Y_UNIT_TEST(ShouldPostponeChangeVolumeBindingFromBalancer)
    {
        CheckChangeVolumeBindingRequestDelay(
            TDuration::Seconds(1),
            TDuration::Seconds(1),
            TDuration::Seconds(60),
            NProto::EPreemptionSource::SOURCE_BALANCER);
    }

    Y_UNIT_TEST(ShouldNotPostponeChangeVolumeBindingFromMonitoring)
    {
        CheckChangeVolumeBindingRequestDelay(
            TDuration::Seconds(1),
            TDuration::Seconds(0),
            TDuration::Seconds(60),
            NProto::EPreemptionSource::SOURCE_MANUAL);
    }

    Y_UNIT_TEST(ShouldSetSilenceRetriableErrorsInCallContextDuringVolumeMove)
    {
        TTestEnv env;
        ui32 nodeIdx1 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx1);
        service.CreateVolume();

        auto response = service.MountVolume();
        auto sessionId = response->Record.GetSessionId();

        TAutoPtr<IEventHandle> delayedMsg;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                     case TEvServicePrivate::EvMountRequestProcessed: {
                        delayedMsg = ev;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        {
            auto request =
                std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                    DefaultDiskId,
                    EChangeBindingOp::RELEASE_TO_HIVE,
                    NProto::EPreemptionSource::SOURCE_BALANCER);
            service.SendRequest(
                MakeStorageServiceId(),
                std::move(request));
        }

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvVolume::EvAddClientRequest);
            runtime.DispatchEvents(options);
        }

        {
            auto writeRequest = service.CreateWriteBlocksRequest(
                DefaultDiskId, TBlockRange64::MakeOneBlock(0), sessionId);
            auto callContext =  writeRequest->CallContext;
            service.SendRequest(
                MakeStorageServiceId(),
                std::move(writeRequest));
            auto writeResponse = service.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(writeResponse->GetStatus(), E_REJECTED);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                callContext->GetSilenceRetriableErrors());
        }

        TDispatchOptions options;
        runtime.DispatchEvents(options, TDuration::Seconds(2));
        runtime.Send(delayedMsg.Release(), nodeIdx1);

        {
            using TResponse = TEvService::TEvChangeVolumeBindingResponse;
            auto response = service.RecvResponse<TResponse>();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        {
            auto writeRequest = service.CreateWriteBlocksRequest(
                DefaultDiskId, TBlockRange64::MakeOneBlock(0), sessionId);
            auto callContext =  writeRequest->CallContext;
            service.SendRequest(
                MakeStorageServiceId(),
                std::move(writeRequest));
            auto writeResponse = service.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(writeResponse->GetStatus()));
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                callContext->GetSilenceRetriableErrors());
        }
    }

    Y_UNIT_TEST(ShouldNotAllowParallelBindingChanges)
    {
        TTestEnv env;
        ui32 nodeIdx1 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx1);
        service.CreateVolume();

        auto response = service.MountVolume();
        auto sessionId = response->Record.GetSessionId();

        TAutoPtr<IEventHandle> delayedMsg;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                     case TEvServicePrivate::EvMountRequestProcessed: {
                        delayedMsg = ev;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        {
            auto request =
                std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                    DefaultDiskId,
                    EChangeBindingOp::RELEASE_TO_HIVE,
                    NProto::EPreemptionSource::SOURCE_BALANCER);
            service.SendRequest(
                MakeStorageServiceId(),
                std::move(request));
        }

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvVolume::EvAddClientRequest);
            runtime.DispatchEvents(options);
        }

        {
            auto request =
                std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                    DefaultDiskId,
                    EChangeBindingOp::RELEASE_TO_HIVE,
                    NProto::EPreemptionSource::SOURCE_BALANCER);
            service.SendRequest(
                MakeStorageServiceId(),
                std::move(request));

            using TResponse = TEvService::TEvChangeVolumeBindingResponse;
            auto response = service.RecvResponse<TResponse>();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_TRY_AGAIN);
            UNIT_ASSERT_VALUES_EQUAL(
                response->GetError().GetMessage(),
                "Rebinding is in progress");
        }

        TDispatchOptions options;
        runtime.DispatchEvents(options, TDuration::Seconds(2));
        runtime.Send(delayedMsg.Release(), nodeIdx1);

        {
            using TResponse = TEvService::TEvChangeVolumeBindingResponse;
            auto response = service.RecvResponse<TResponse>();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }
    }

    Y_UNIT_TEST(ShouldReturnSilentErrorIfVolumeRemovedBeforeMountForReplicated)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume();

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        service.DestroyVolume();

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        auto response1 = service.RecvMountVolumeResponse();
        const auto errorCode = response1->GetStatus();
        auto status =
            static_cast<ui32>(STATUS_FROM_CODE(errorCode));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NKikimrScheme::StatusPathDoesNotExist),
            status);

        const auto& error = response1->Record.GetError();
        UNIT_ASSERT(HasProtoFlag(error.GetFlags(), NProto::EF_SILENT));
    }

    Y_UNIT_TEST(ShouldReturnSilentErrorIfVolumeRemovedBeforeMountForNonReplicated)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        config.SetAcquireNonReplicatedDevices(true);

        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            1_GB / DefaultBlockSize,
            DefaultBlockSize,
            TString(), // folderId
            TString(), // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvAcquireDiskRequest: {
                        using TResponse =
                            TEvDiskRegistry::TEvAcquireDiskResponse;
                        auto response =
                            std::make_unique<TResponse>(
                                MakeError(E_NOT_FOUND, "Disk not found"));
                        env.GetRuntime().Send(
                            new IEventHandle(
                                ev->Sender,
                                ev->Recipient,
                                response.release(),
                                0, // flags
                                ev->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            });

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        auto response1 = service.RecvMountVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response1->GetStatus());

        const auto& error = response1->Record.GetError();
        UNIT_ASSERT(HasProtoFlag(error.GetFlags(), NProto::EF_SILENT));
    }

    Y_UNIT_TEST(ShouldProperlyTerminateMountUnmountRequestsIfVolumeIsRemoved)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        config.SetAcquireNonReplicatedDevices(true);

        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui32 numDescribes = 0;
        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        ++numDescribes;
                        break;
                    }
                    case TEvServicePrivate::EvSessionActorDied: {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        service.SendUnmountVolumeRequest(
            DefaultDiskId,
            ""
        );

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        {
            auto response = service.RecvMountVolumeResponse();
            const auto errorCode = response->GetStatus();
            auto status =
                static_cast<ui32>(STATUS_FROM_CODE(errorCode));

            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NKikimrScheme::StatusPathDoesNotExist),
                status);
            const auto& error = response->Record.GetError();
            UNIT_ASSERT(HasProtoFlag(error.GetFlags(), NProto::EF_SILENT));
        }

        {
            auto response = service.RecvUnmountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_ALREADY,
                response->GetStatus());
        }

        {
            auto response = service.RecvMountVolumeResponse();
            const auto errorCode = response->GetStatus();
            auto status =
                static_cast<ui32>(STATUS_FROM_CODE(errorCode));

            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NKikimrScheme::StatusPathDoesNotExist),
                status);
            const auto& error = response->Record.GetError();
            UNIT_ASSERT(HasProtoFlag(error.GetFlags(), NProto::EF_SILENT));
            UNIT_ASSERT_VALUES_EQUAL(1, numDescribes);
        }
    }

    Y_UNIT_TEST(ShouldMountEncryptedVolumeAndUpdateKeyHash)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto projectId = "project";
        auto folderId = "folder";
        auto cloudId = "cloud";

        auto req = service.CreateCreateVolumeRequest(
            DefaultDiskId,
            DefaultBlocksCount,
            DefaultBlockSize,
            folderId,
            cloudId);
        req->Record.MutableEncryptionSpec()->SetMode(NProto::ENCRYPTION_AES_XTS);
        req->Record.SetProjectId(projectId);
        service.SendRequest(MakeStorageServiceId(), std::move(req));
        auto resp = service.RecvResponse<TEvService::TEvCreateVolumeResponse>();
        UNIT_ASSERT_C(SUCCEEDED(resp->GetStatus()), resp->GetErrorReason());

        service.AssignVolume();

        NProto::TEncryptionDesc encryptionDesc;
        encryptionDesc.SetMode(NProto::ENCRYPTION_AES_XTS);
        encryptionDesc.SetKeyHash("encryptionkeyhash");

        auto validateVolume = [&](const NProto::TMountVolumeResponse& response) {
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            const auto& volume = response.GetVolume();
            UNIT_ASSERT_VALUES_EQUAL(projectId, volume.GetProjectId());
            UNIT_ASSERT_VALUES_EQUAL(folderId, volume.GetFolderId());
            UNIT_ASSERT_VALUES_EQUAL(cloudId, volume.GetCloudId());
        };

        // first mount without keyhash
        {
            auto response = service.MountVolume();
            validateVolume(response->Record);
            auto sessionId = response->Record.GetSessionId();
            service.UnmountVolume(DefaultDiskId, sessionId);
        }

        // set keyhash using mount
        {
            auto response = service.MountVolume(
                DefaultDiskId,
                "",
                "",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                0,
                0,
                encryptionDesc);
            validateVolume(response->Record);
            auto sessionId = response->Record.GetSessionId();
            service.UnmountVolume(DefaultDiskId, sessionId);
        }

        for (size_t i = 0; i < 2; ++i) {
            // mount without encryption
            {
                auto response = service.MountVolume();
                auto sessionId = response->Record.GetSessionId();
                service.UnmountVolume(DefaultDiskId, sessionId);
            }

            // failed to mount without keyhash
            {
                auto encryption = encryptionDesc;
                encryption.ClearKeyHash();
                service.SendMountVolumeRequest(
                    DefaultDiskId,
                    "",
                    "",
                    NProto::IPC_GRPC,
                    NProto::VOLUME_ACCESS_READ_WRITE,
                    NProto::VOLUME_MOUNT_LOCAL,
                    0,
                    0,
                    encryption);
                auto response = service.RecvMountVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    response->GetStatus(),
                    response->GetErrorReason());
            }

            // failed to mount with wrong keyhash
            {
                auto encryption = encryptionDesc;
                encryption.SetKeyHash(TString("wrong") + encryption.GetKeyHash());
                service.SendMountVolumeRequest(
                    DefaultDiskId,
                    "",
                    "",
                    NProto::IPC_GRPC,
                    NProto::VOLUME_ACCESS_READ_WRITE,
                    NProto::VOLUME_MOUNT_LOCAL,
                    0,
                    0,
                    encryption);
                auto response = service.RecvMountVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    response->GetStatus(),
                    response->GetErrorReason());
            }

            // mount with correct keyhash
            {
                auto response = service.MountVolume(
                    DefaultDiskId,
                    "",
                    "",
                    NProto::IPC_GRPC,
                    NProto::VOLUME_ACCESS_READ_WRITE,
                    NProto::VOLUME_MOUNT_LOCAL,
                    0,
                    0,
                    encryptionDesc);
                auto sessionId = response->Record.GetSessionId();
                service.UnmountVolume(DefaultDiskId, sessionId);
            }

            // check that keyhash is not reset
            service.AlterVolume(DefaultDiskId, "project2", "folder2", "cloud2");
        }
    }

    Y_UNIT_TEST(ShouldRemountEncryptedVolumesByBalancer)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto projectId = "project";
        auto folderId = "folder";
        auto cloudId = "cloud";

        auto req = service.CreateCreateVolumeRequest(
            DefaultDiskId,
            DefaultBlocksCount,
            DefaultBlockSize,
            folderId,
            cloudId);
        req->Record.MutableEncryptionSpec()->SetMode(NProto::ENCRYPTION_AES_XTS);
        req->Record.SetProjectId(projectId);
        service.SendRequest(MakeStorageServiceId(), std::move(req));
        auto resp = service.RecvResponse<TEvService::TEvCreateVolumeResponse>();
        UNIT_ASSERT_C(SUCCEEDED(resp->GetStatus()), resp->GetErrorReason());

        service.AssignVolume();

        NProto::TEncryptionDesc encryptionDesc;
        encryptionDesc.SetMode(NProto::ENCRYPTION_AES_XTS);
        encryptionDesc.SetKeyHash("encryptionkeyhash");

        auto validateVolume = [&](const NProto::TMountVolumeResponse& response) {
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            const auto& volume = response.GetVolume();
            UNIT_ASSERT_VALUES_EQUAL(projectId, volume.GetProjectId());
            UNIT_ASSERT_VALUES_EQUAL(folderId, volume.GetFolderId());
            UNIT_ASSERT_VALUES_EQUAL(cloudId, volume.GetCloudId());
        };

        TString sessionId;

        // set keyhash using mount
        {
            auto response = service.MountVolume(
                DefaultDiskId,
                "",
                "",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                0,
                0,
                encryptionDesc);
            validateVolume(response->Record);
            sessionId = response->Record.GetSessionId();
        }

        TVector<NProto::EPreemptionSource> sources {
            NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT,
            NProto::EPreemptionSource::SOURCE_BALANCER,
            NProto::EPreemptionSource::SOURCE_MANUAL};

        for (auto s: sources) {
            {
                auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                    DefaultDiskId,
                    EChangeBindingOp::RELEASE_TO_HIVE,
                    s);
                service.SendRequest(MakeStorageServiceId(), std::move(request));
                auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
            }

            service.WaitForVolume(DefaultDiskId);
            service.ReadBlocks(DefaultDiskId, 0, sessionId);

            {
                auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
                    DefaultDiskId,
                    EChangeBindingOp::ACQUIRE_FROM_HIVE,
                    s);
                service.SendRequest(MakeStorageServiceId(), std::move(request));
                auto response = service.RecvResponse<TEvService::TEvChangeVolumeBindingResponse>();
            }

            service.WaitForVolume(DefaultDiskId);
            service.ReadBlocks(DefaultDiskId, 0, sessionId);
        }
    }

    Y_UNIT_TEST(ShouldHandleMountRequestWithFillSeqNumber)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        ui32 nodeIdx2 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        TServiceClient service2(runtime, nodeIdx2);
        {
            auto request = service1.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(1);
            service1.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service1.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, response->GetStatus(), response->GetStatus());
        }

        ui32 mountFlags = 0;
        SetProtoFlag(mountFlags, NProto::MF_THROTTLING_DISABLED);

        ui64 mountSeqNumber = 0;

        auto response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            0,  // fillSeqNumber
            1   // fillGeneration
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response1->GetStatus(),
            response1->GetErrorReason()
        );

        auto response2 = service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            1,  // fillSeqNumber
            1   // fillGeneration
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response2->GetStatus(),
            response2->GetErrorReason()
        );

        service1.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            0,  // fillSeqNumber
            1   // fillGeneration
        );

        {
            auto response = service1.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_PRECONDITION_FAILED,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        response1 = service1.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            0,  // fillSeqNumber
            1   // fillGeneration
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response1->GetStatus(),
            response1->GetErrorReason()
        );

        service2.UnmountVolume(DefaultDiskId, response2->Record.GetSessionId());

        // Checking that client with old fill seqno will not be allowed
        // even after another client is removed.
        service1.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            0,  // fillSeqNumber
            1   // fillGeneration
        );

        {
            auto response = service1.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_PRECONDITION_FAILED,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        // Checking that client can mount with same fill seq no after unmount.
        response2 = service2.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            1,  // fillSeqNumber
            1   // fillGeneration
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response2->GetStatus(),
            response2->GetErrorReason()
        );
    }

    Y_UNIT_TEST(ShouldPreserveFillSeqNumberAfterVolumeConfigUpdate)
    {
        TTestEnv env(1, 1);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(1);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, response->GetStatus(), response->GetStatus());
        }

        ui32 mountFlags = 0;
        SetProtoFlag(mountFlags, NProto::MF_THROTTLING_DISABLED);

        ui64 mountSeqNumber = 0;

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            1,  // fillSeqNumber
            1   // fillGeneration
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason()
        );

        // Resize volume request updates volume config
        service.ResizeVolume(DefaultDiskId, DefaultBlocksCount * 2);

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            0,  // fillSeqNumber
            1   // fillGeneration
        );

        {
            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_PRECONDITION_FAILED,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }
    }

    Y_UNIT_TEST(ShouldCheckIsFillFinishedOnMount)
    {
        TTestEnv env(1, 1);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        ui64 fillGeneration = 1;
        {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(fillGeneration);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, response->GetStatus(), response->GetStatus());
        }

        ui32 mountFlags = 0;
        SetProtoFlag(mountFlags, NProto::MF_THROTTLING_DISABLED);

        ui64 mountSeqNumber = 0;

        auto response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            1, // fillSeqNumber
            fillGeneration
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason()
        );

        {
            NPrivateProto::TFinishFillDiskRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(1);
            request.SetFillGeneration(fillGeneration);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto response = service.ExecuteAction("FinishFillDisk", buf);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        // Should not be able to mount when IS_FILL flag is set and fill is finished
        // even if fillSeqNumber is big enough
        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            2, // fillSeqNumber
            fillGeneration
        );

        {
            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_PRECONDITION_FAILED,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        // Should be able to mount when fill is finished
        // In particular, should not check fillSeqNumber here
        response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            0, // fillSeqNumber
            0 // fillGeneration
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason()
        );

        response = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            mountFlags,
            mountSeqNumber++,
            NProto::TEncryptionDesc(),
            0, // fillSeqNumber
            0 // fillGeneration
        );
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason()
        );
    }

    Y_UNIT_TEST(ShouldHandleMountRequestWithFillGeneration)
    {
        TTestEnv env(1, 2);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx1 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);
        ui32 nodeIdx2 = SetupTestEnvWithMultipleMount(
            env,
            unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        TServiceClient service2(runtime, nodeIdx2);

        {
            auto request = service1.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(713);
            service1.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service1.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, response->GetStatus(), response->GetStatus());
        }

        ui32 mountFlags = 0;
        SetProtoFlag(mountFlags, NProto::MF_THROTTLING_DISABLED);

        ui64 mountSeqNumber = 0;

        auto checkMountVolume = [&](
            TServiceClient& service,
            ui64 fillGeneration,
            ui32 expectedStatus)
        {
            service.SendMountVolumeRequest(
                DefaultDiskId,
                TString(),
                TString(),
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE,
                mountFlags,
                mountSeqNumber++,
                NProto::TEncryptionDesc(),
                0,
                fillGeneration
            );

            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedStatus,
                response->GetStatus(),
                response->GetErrorReason()
            );
        };

        checkMountVolume(service1, 713, S_OK);
        checkMountVolume(service2, 712, E_PRECONDITION_FAILED);
        checkMountVolume(service2, 714, E_PRECONDITION_FAILED);
        checkMountVolume(service2, 713, S_OK);
        checkMountVolume(service1, 0, S_OK);
    }

    Y_UNIT_TEST(ShouldFailQueuedRequestsIfVolumeDestroyed)
    {
        TTestEnv env(1, 1);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            TString(),
            TString()
        );

        auto response1 = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        auto sessionId = response1->Record.GetSessionId();

        bool tabletDeadSeen = false;
        env.GetRuntime().SetObserverFunc(
            [&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTablet::EvTabletDead: {
                        tabletDeadSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.DestroyVolume();

        if (!tabletDeadSeen) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvTabletDead);
            env.GetRuntime().DispatchEvents(options);
        }
        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            TString(),
            TString()
        );

        service.SendUnmountVolumeRequest(DefaultDiskId, sessionId);

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        service.SendUnmountVolumeRequest(DefaultDiskId, sessionId);

        {
            auto unmountResponse = service.RecvUnmountVolumeResponse();
            UNIT_ASSERT_C(
                SUCCEEDED(unmountResponse->GetStatus()),
                unmountResponse->GetErrorReason());
        }

        {
            auto mountResponse = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, mountResponse->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk tablet is changed. Retrying",
                mountResponse->GetErrorReason());
        }

        {
            auto unmountResponse = service.RecvUnmountVolumeResponse();
            UNIT_ASSERT_C(
                SUCCEEDED(unmountResponse->GetStatus()),
                unmountResponse->GetErrorReason());
        }

        service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );
    }

    Y_UNIT_TEST(ShouldKillMountActorIfTabletIsChangedDuringTabletStart)
    {
        TTestEnv env(1, 1);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            TString(),
            TString()
        );

        ui64 volumeTabletId = 0;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response1 = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        auto sessionId = response1->Record.GetSessionId();

        if (!volumeTabletId) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvSSProxy::EvDescribeVolumeResponse);
            env.GetRuntime().DispatchEvents(options);
        }

        UNIT_ASSERT(volumeTabletId);

        RebootTablet(env.GetRuntime(), volumeTabletId, service.GetSender(), nodeIdx);

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        ui32 sessionActorDeathCnt = 0;
        env.GetRuntime().SetObserverFunc(
            [&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvLockTabletResponse: {
                        auto* msg = event->Get<TEvHiveProxy::TEvLockTabletResponse>();
                        const_cast<NProto::TError&>(msg->Error) = MakeKikimrError(
                            NKikimrProto::ERROR,
                            "Could not connect");
                        break;
                    }
                    case TEvServicePrivate::EvSessionActorDied: {
                        ++sessionActorDeathCnt;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        {
            auto mountResponse = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                MAKE_KIKIMR_ERROR(NKikimrProto::ERROR),
                mountResponse->GetStatus() );
        }

        if (!sessionActorDeathCnt) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvSessionActorDied);
            env.GetRuntime().DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, sessionActorDeathCnt);
    }

    Y_UNIT_TEST(ShouldKillMountActorIfMountDetectsTabletIdChange)
    {
        TTestEnv env(1, 1);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            TString(),
            TString()
        );

        auto response1 = service.MountVolume(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        service.SendMountVolumeRequest(
            DefaultDiskId,
            TString(),
            TString(),
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL
        );

        ui32 sessionActorDeathCnt = 0;
        env.GetRuntime().SetObserverFunc(
            [&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        auto& pathDescription =
                            const_cast<NKikimrSchemeOp::TPathDescription&>(msg->PathDescription);
                        pathDescription.
                        MutableBlockStoreVolumeDescription()->
                        SetVolumeTabletId(111);
                        break;
                    }
                    case TEvServicePrivate::EvSessionActorDied: {
                        ++sessionActorDeathCnt;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        {
            auto mountResponse = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, mountResponse->GetStatus() );
        }

        if (!sessionActorDeathCnt) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvSessionActorDied);
            env.GetRuntime().DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, sessionActorDeathCnt);
    }

    Y_UNIT_TEST(ShouldRestartVolumeTabletOnLocalMountIfNeeded)
    {
        TTestEnv env(1, 1);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx =
            SetupTestEnvWithMultipleMount(env, unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx);
        TServiceClient service2(runtime, nodeIdx);

        ui64 fillGeneration = 1;

        {
            auto request = service1.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(fillGeneration);
            service1.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service1.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetStatus());
        }

        auto mountVolume =
            [&](TServiceClient& service,
                const ui64 mountSeqNumber,
                const ui64 fillSeqNumber,
                const ui64 fillGeneration,
                const NProto::EVolumeAccessMode accessMode =
                    NProto::VOLUME_ACCESS_READ_WRITE) -> std::pair<ui64, ui64>
        {
            auto statResponseBefore = service1.StatVolume(DefaultDiskId);

            auto response = service.MountVolume(
                DefaultDiskId,
                TString(),
                TString(),
                NProto::IPC_GRPC,
                accessMode,
                NProto::VOLUME_MOUNT_LOCAL,
                NProto::MF_THROTTLING_DISABLED,
                mountSeqNumber,
                NProto::TEncryptionDesc(),
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());

            auto statResponseAfter = service1.StatVolume(DefaultDiskId);

            return {
                statResponseBefore->Record.GetVolumeGeneration(),
                statResponseAfter->Record.GetVolumeGeneration()};
        };

        ui64 mountSeqNumber = 0;
        ui64 fillSeqNumber = 0;

        {
            auto [generationBefore, generationAfter] = mountVolume(
                service1,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_LE_C(
                generationBefore + 1,
                generationAfter,
                "volume should restart");
        }

        ++fillSeqNumber;
        {
            auto [generationBefore, generationAfter] = mountVolume(
                service1,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_LE_C(
                generationBefore + 1,
                generationAfter,
                "volume should restart");
        }

        {
            auto [generationBefore, generationAfter] = mountVolume(
                service1,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_VALUES_EQUAL_C(
                generationBefore,
                generationAfter,
                "volume should not restart");
        }

        ++fillSeqNumber;
        {
            auto [generationBefore, generationAfter] = mountVolume(
                service1,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_LE_C(
                generationBefore + 1,
                generationAfter,
                "volume should restart");
        }

        service1.UnmountVolume(DefaultDiskId);

        {
            auto [generationBefore, generationAfter] = mountVolume(
                service2,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_LE_C(
                generationBefore + 1,
                generationAfter,
                "volume should restart");
        }

        {
            NPrivateProto::TFinishFillDiskRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(1);
            request.SetFillGeneration(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto response = service1.ExecuteAction("FinishFillDisk", buf);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        fillGeneration = 0;
        fillSeqNumber = 0;
        {
            auto [generationBefore, generationAfter] = mountVolume(
                service2,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_LE_C(
                generationBefore + 1,
                generationAfter,
                "volume should restart");
        }

        ++mountSeqNumber;
        {
            auto [generationBefore, generationAfter] = mountVolume(
                service2,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_LE_C(
                generationBefore + 1,
                generationAfter,
                "volume should restart");
        }

        {
            auto [generationBefore, generationAfter] = mountVolume(
                service2,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_VALUES_EQUAL_C(
                generationBefore,
                generationAfter,
                "volume should not restart");
        }

        ++mountSeqNumber;
        {
            auto [generationBefore, generationAfter] = mountVolume(
                service2,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration);
            UNIT_ASSERT_LE_C(
                generationBefore + 1,
                generationAfter,
                "volume should restart");
        }

        ++mountSeqNumber;
        {
            auto [generationBefore, generationAfter] = mountVolume(
                service1,
                mountSeqNumber,
                fillSeqNumber,
                fillGeneration,
                NProto::VOLUME_ACCESS_READ_ONLY);
            UNIT_ASSERT_VALUES_EQUAL_C(
                generationBefore,
                generationAfter,
                "volume should not restart");
        }
    }

    Y_UNIT_TEST(ShouldHandleMountRequestsSingleClient)
    {
        TTestEnv env(1, 1);
        auto unmountClientsTimeout = TDuration::Seconds(10);
        ui32 nodeIdx =
            SetupTestEnvWithMultipleMount(env, unmountClientsTimeout);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        ui64 fillGeneration = 10;

        {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(fillGeneration);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetStatus());
        }

        auto mountVolume = [&](const ui64 mountSeqNumber,
                               const ui64 fillSeqNumber,
                               const ui64 fillGeneration,
                               EWellKnownResultCodes expectedResult)
        {
            auto statResponseBefore = service.StatVolume(DefaultDiskId);

            service.SendMountVolumeRequest(
                DefaultDiskId,
                TString(),
                TString(),
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                NProto::MF_THROTTLING_DISABLED,
                mountSeqNumber,
                NProto::TEncryptionDesc(),
                fillSeqNumber,
                fillGeneration);

            auto response = service.RecvMountVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedResult,
                response->GetStatus(),
                response->GetErrorReason());

            auto statResponseAfter = service.StatVolume(DefaultDiskId);
        };

        ui64 mountSeqNumber = 10;
        ui64 fillSeqNumber = 10;

        mountVolume(mountSeqNumber, fillSeqNumber, fillGeneration, S_OK);
        mountVolume(mountSeqNumber, fillSeqNumber, fillGeneration, S_OK);
        mountVolume(mountSeqNumber, fillSeqNumber, fillGeneration, S_ALREADY);
        mountVolume(
            mountSeqNumber,
            fillSeqNumber - 1,
            fillGeneration,
            E_PRECONDITION_FAILED);
        mountVolume(mountSeqNumber, fillSeqNumber, fillGeneration, S_OK);
        mountVolume(mountSeqNumber, fillSeqNumber, fillGeneration, S_OK);
        mountVolume(mountSeqNumber, fillSeqNumber, fillGeneration, S_ALREADY);
        mountVolume(
            mountSeqNumber,
            fillSeqNumber,
            fillGeneration - 1,
            E_PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(ShouldGetEmptyPrincipalDiskIdDuringMountPrincipal)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("Disk-1");

        auto response = service.MountVolume("Disk-1");
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            FormatError(response->GetError()));

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            response->Record.GetVolume().GetPrincipalDiskId());
    }

    Y_UNIT_TEST(ShouldGetPrincipalDiskIdDuringMountDiskThatIsNotReadyYet)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("Disk-1");

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId("Disk-1");
            *request.AddTagsToAdd() = "source-disk-id=Disk-1-copy";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        auto response = service.MountVolume("Disk-1");
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            FormatError(response->GetError()));

        UNIT_ASSERT_VALUES_EQUAL(
            "Disk-1-copy",
            response->Record.GetVolume().GetPrincipalDiskId());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
