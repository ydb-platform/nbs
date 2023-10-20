#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/volume.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <ydb/core/base/statestorage.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceStartStopVolumeTest)
{
    Y_UNIT_TEST(ShouldRebootDeadVolumes)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TActorId volumeActorId;
        bool startVolumeActorStopped = false;

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        volumeActorId = msg->VolumeActor;
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped:
                        startVolumeActorStopped = true;
                        break;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.MountVolume();

        UNIT_ASSERT(volumeActorId);
        UNIT_ASSERT(!startVolumeActorStopped);

        // Kill the volume actor
        service.SendRequest(volumeActorId, std::make_unique<TEvents::TEvPoisonPill>());

        // Wait until tablet goes down
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvVolumeTabletStatus);
            runtime.DispatchEvents(options);
            UNIT_ASSERT(!volumeActorId);
        }

        // Wait until is up again
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvVolumeTabletStatus);
            runtime.DispatchEvents(options);
            UNIT_ASSERT(volumeActorId);
        }

        // Requests should be working now
        service.StatVolume();

        UNIT_ASSERT(volumeActorId);
        UNIT_ASSERT(!startVolumeActorStopped);
    }

    Y_UNIT_TEST(ShouldUnmountStolenVolumes)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TActorId volumeActorId;
        TActorId startVolumeActorId;
        ui64 volumeTabletId = 0;
        bool startVolumeActorStopped = false;

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
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
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume(DefaultDiskId);
        service.AssignVolume(DefaultDiskId, "foo", "bar");
        service.MountVolume(DefaultDiskId, "foo", "bar");

        UNIT_ASSERT(volumeActorId);
        UNIT_ASSERT(!startVolumeActorStopped);

        // Change assignment
        service.AssignVolume(DefaultDiskId, "foo", "baz");

        // Simulate lock loss
        service.SendRequest(
            startVolumeActorId,
            std::make_unique<TEvHiveProxy::TEvTabletLockLost>(volumeTabletId));

        // Wait until start volume actor is stopped
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvStartVolumeActorStopped);
            runtime.DispatchEvents(options);
            UNIT_ASSERT(startVolumeActorStopped);
        }
    }

    Y_UNIT_TEST(ShouldUnmountVolumesOnLostLockWithoutToken)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        ui64 volumeTabletId = 0;
        bool startVolumeActorStopped = false;

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        volumeTabletId = msg->TabletId;
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped:
                        startVolumeActorStopped = true;
                        break;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId);
        service.MountVolume(DefaultDiskId);

        UNIT_ASSERT(volumeTabletId);
        UNIT_ASSERT(!startVolumeActorStopped);

        // Lock tablet to a different owner in hive
        {
            TActorId edge = runtime.AllocateEdgeActor();
            runtime.SendToPipe(env.GetHive(), edge, new TEvHive::TEvLockTabletExecution(volumeTabletId));
            auto reply = runtime.GrabEdgeEvent<TEvHive::TEvLockTabletExecutionResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NKikimrProto::OK);
        }

        // Wait until volume is unmounted
        if (!startVolumeActorStopped) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvServicePrivate::EvStartVolumeActorStopped);
            runtime.DispatchEvents(options);
            UNIT_ASSERT(startVolumeActorStopped);
        }
    }

    Y_UNIT_TEST(ShouldStopVolumeActorWhenSingleReadWriteClientUnmounts)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TActorId mountActorId;
        bool volumeActorStopped = false;

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus:
                        mountActorId = event->Sender;
                        break;
                    case TEvents::TSystem::PoisonPill:
                        if (mountActorId && event->Sender == mountActorId) {
                            volumeActorStopped = true;
                        }
                        break;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service1(runtime, nodeIdx);
        service1.CreateVolume();
        service1.AssignVolume();

        TString sessionId;
        {
            auto response = service1.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        UNIT_ASSERT(mountActorId);
        UNIT_ASSERT(!volumeActorStopped);

        service1.UnmountVolume(DefaultDiskId, sessionId);
        UNIT_ASSERT(volumeActorStopped);
    }

    Y_UNIT_TEST(ShouldStopVolumeActorWhenLocalMounterClientUnmounts)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TActorId mountActorId;
        bool volumeActorStopped = false;

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus:
                        mountActorId = event->Sender;
                        break;
                    case TEvents::TSystem::PoisonPill:
                        if (mountActorId && event->Sender == mountActorId) {
                            volumeActorStopped = true;
                        }
                        break;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service1(runtime, nodeIdx);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        TString sessionId;
        {
            auto response = service1.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_LOCAL);
            sessionId = response->Record.GetSessionId();
        }
        UNIT_ASSERT(mountActorId);

        TServiceClient service2(runtime, nodeIdx);
        service2.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        TServiceClient service3(runtime, nodeIdx);
        service3.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE);

        service3.UnmountVolume(DefaultDiskId, sessionId);
        UNIT_ASSERT(!volumeActorStopped);

        service1.UnmountVolume(DefaultDiskId, sessionId);
        UNIT_ASSERT(volumeActorStopped);
    }

    Y_UNIT_TEST(ShouldStartStopVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        bool volumeStarted = false;
        bool volumeStopped = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        if (msg->VolumeActor) {
                            volumeStarted = true;
                        }
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped:
                        volumeStopped = true;
                        break;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto sessionId = service.MountVolume()->Record.GetSessionId();
        UNIT_ASSERT(volumeStarted);
        UNIT_ASSERT(!volumeStopped);

        service.UnmountVolume(DefaultDiskId, sessionId);
        UNIT_ASSERT(volumeStopped);
    }

    Y_UNIT_TEST(ShouldReturnErrorIfUnableToObtainLockWhenStartingVolumeTablet)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto error = MakeError(E_FAIL, "Lock failed");
        bool attemptedToLockVolumeTablet = false;
        bool volumeStarted = false;
        bool volumeStopped = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        if (msg->VolumeActor) {
                            volumeStarted = true;
                        }
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped:
                        volumeStopped = true;
                        break;
                    case TEvHiveProxy::EvLockTabletRequest: {
                        attemptedToLockVolumeTablet = true;
                        auto response = std::make_unique<TEvHiveProxy::TEvLockTabletResponse>(
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
        UNIT_ASSERT(!volumeStarted);
        UNIT_ASSERT(volumeStopped);
    }

    Y_UNIT_TEST(ShouldHandleExternalBootFailureDuringVolumeTabletStart)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        bool onceFailedExternalBoot = false;
        bool rebootAttempted = false;
        bool volumeStarted = false;
        bool volumeStopped = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        if (msg->VolumeActor) {
                            volumeStarted = true;
                        }
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped:
                        volumeStopped = true;
                        break;
                    case TEvHiveProxy::EvBootExternalRequest: {
                        if (!onceFailedExternalBoot) {
                            onceFailedExternalBoot = true;
                            auto response = std::make_unique<TEvHiveProxy::TEvBootExternalResponse>(
                                MakeError(E_ARGUMENT, "Boot external failed"));
                            runtime.Send(
                                new IEventHandle(
                                    event->Sender,
                                    event->Recipient,
                                    response.release(),
                                    0, // flags
                                    event->Cookie),
                                nodeIdx);
                            return TTestActorRuntime::EEventAction::DROP;
                        } else {
                            rebootAttempted = true;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume();
        UNIT_ASSERT(onceFailedExternalBoot);
        UNIT_ASSERT(rebootAttempted);
        UNIT_ASSERT(volumeStarted);
        UNIT_ASSERT(!volumeStopped);
    }

    Y_UNIT_TEST(ShouldFinishLockBeforeUnlockingOnShutdown)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        bool volumeStarted = false;
        bool volumeStopped = false;

        enum class EEvent
        {
            LOCK_REQUEST,
            LOCK_RESPONSE,
            UNLOCK_REQUEST,
            UNLOCK_RESPONSE,
            STOPPED
        };

        TVector<EEvent> capturedEvents;

        TActorId startVolumeActorId;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        if (msg->VolumeActor) {
                            volumeStarted = true;
                        }
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped: {
                        volumeStopped = true;
                        capturedEvents.emplace_back(EEvent::STOPPED);
                        break;
                    }
                    case TEvHiveProxy::EvLockTabletRequest: {
                        startVolumeActorId = event->Sender;
                        auto poisonPill = std::make_unique<TEvents::TEvPoisonPill>();
                        runtime.Send(
                            new IEventHandle(
                                startVolumeActorId,
                                service.GetSender(),
                                poisonPill.release()),
                            nodeIdx);
                        capturedEvents.emplace_back(EEvent::LOCK_REQUEST);
                        // Let poison pill reach the actor
                        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
                        break;
                    }
                    case TEvHiveProxy::EvLockTabletResponse: {
                        if (startVolumeActorId && event->Recipient == startVolumeActorId) {
                            capturedEvents.emplace_back(EEvent::LOCK_RESPONSE);
                        }
                        break;
                    }
                    case TEvHiveProxy::EvUnlockTabletRequest: {
                        if (startVolumeActorId && event->Sender == startVolumeActorId) {
                            capturedEvents.emplace_back(EEvent::UNLOCK_REQUEST);
                        }
                        break;
                    }
                    case TEvHiveProxy::EvUnlockTabletResponse: {
                        if (startVolumeActorId && event->Recipient == startVolumeActorId) {
                            capturedEvents.emplace_back(EEvent::UNLOCK_RESPONSE);
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_INVALID_STATE);
        UNIT_ASSERT(response->GetErrorReason());

        UNIT_ASSERT(!volumeStarted);
        UNIT_ASSERT(volumeStopped);

        UNIT_ASSERT(capturedEvents.size() == 5);
        UNIT_ASSERT(capturedEvents[0] == EEvent::LOCK_REQUEST);
        UNIT_ASSERT(capturedEvents[1] == EEvent::LOCK_RESPONSE);
        UNIT_ASSERT(capturedEvents[2] == EEvent::UNLOCK_REQUEST);
        UNIT_ASSERT(capturedEvents[3] == EEvent::UNLOCK_RESPONSE);
        UNIT_ASSERT(capturedEvents[4] == EEvent::STOPPED);
    }

    Y_UNIT_TEST(ShouldFinishTabletStartBeforeStoppingItOnShutdown)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        bool volumeStarted = false;
        bool volumeStopped = false;

        enum class EEvent
        {
            LOCK_REQUEST,
            LOCK_RESPONSE,
            BOOT_EXTERNAL_REQUEST,
            BOOT_EXTERNAL_RESPONSE,
            UNLOCK_REQUEST,
            UNLOCK_RESPONSE,
            START_REQUEST,
            START_RESPONSE,
            STOP_REQUEST,
            STOP_RESPONSE,
            STOPPED
        };

        TVector<EEvent> capturedEvents;

        TActorId startVolumeActorId;
        TActorId volumeActorId;
        ui64 volumeTabletId = 0;
        bool bootExternalReceived = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        if (msg->VolumeActor) {
                            volumeStarted = true;
                        }
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped: {
                        volumeStopped = true;
                        capturedEvents.emplace_back(EEvent::STOPPED);
                        break;
                    }
                    case TEvHiveProxy::EvLockTabletRequest: {
                        const auto* msg = event->Get<TEvHiveProxy::TEvLockTabletRequest>();
                        volumeTabletId = msg->TabletId;
                        startVolumeActorId = event->Sender;
                        capturedEvents.emplace_back(EEvent::LOCK_REQUEST);
                        break;
                    }
                    case TEvHiveProxy::EvLockTabletResponse: {
                        if (startVolumeActorId == event->Recipient) {
                            capturedEvents.emplace_back(EEvent::LOCK_RESPONSE);
                        }
                        break;
                    }
                    case TEvHiveProxy::EvBootExternalRequest: {
                        if (startVolumeActorId == event->Sender) {
                            capturedEvents.emplace_back(EEvent::BOOT_EXTERNAL_REQUEST);
                        }
                        break;
                    }
                    case TEvHiveProxy::EvBootExternalResponse: {
                        if (startVolumeActorId == event->Recipient ||
                            startVolumeActorId == event->GetRecipientRewrite())
                        {
                            capturedEvents.emplace_back(EEvent::BOOT_EXTERNAL_RESPONSE);
                            bootExternalReceived = true;
                        }
                        break;
                    }
                    case NNodeWhiteboard::TEvWhiteboard::EvTabletStateUpdate: {
                        const auto* msg = event->Get<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>();
                        if (bootExternalReceived &&
                            volumeTabletId &&
                            msg->Record.GetTabletId() == volumeTabletId &&
                            !volumeActorId)
                        {
                            volumeActorId = event->Sender;
                            capturedEvents.emplace_back(EEvent::START_REQUEST);
                            auto poisonPill = std::make_unique<TEvents::TEvPoisonPill>();
                            runtime.Send(
                                new IEventHandle(
                                    startVolumeActorId,
                                    service.GetSender(),
                                    poisonPill.release()),
                                nodeIdx);
                            // Let poison pill reach the actor
                            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
                        }
                        break;
                    }
                    case TEvTablet::EvRestored: {
                        if (startVolumeActorId == event->Recipient ||
                            startVolumeActorId == event->GetRecipientRewrite())
                        {
                            capturedEvents.emplace_back(EEvent::START_RESPONSE);
                        }
                        break;
                    }
                    case TEvents::TSystem::PoisonPill: {
                        if (startVolumeActorId == event->Sender) {
                            capturedEvents.emplace_back(EEvent::STOP_REQUEST);
                        }
                        break;
                    }
                    case TEvTablet::EvTabletDead: {
                        if (startVolumeActorId == event->Recipient ||
                            startVolumeActorId == event->GetRecipientRewrite())
                        {
                            capturedEvents.emplace_back(EEvent::STOP_RESPONSE);
                        }
                        break;
                    }
                    case TEvHiveProxy::EvUnlockTabletRequest: {
                        if (startVolumeActorId == event->Sender) {
                            capturedEvents.emplace_back(EEvent::UNLOCK_REQUEST);
                        }
                        break;
                    }
                    case TEvHiveProxy::EvUnlockTabletResponse: {
                        if (startVolumeActorId == event->Recipient ||
                            startVolumeActorId == event->GetRecipientRewrite())
                        {
                            capturedEvents.emplace_back(EEvent::UNLOCK_RESPONSE);
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == E_INVALID_STATE);
        UNIT_ASSERT(response->GetErrorReason());

        UNIT_ASSERT(!volumeStarted);
        UNIT_ASSERT(volumeStopped);

        UNIT_ASSERT(capturedEvents.size() == 11);
        UNIT_ASSERT(capturedEvents[0] == EEvent::LOCK_REQUEST);
        UNIT_ASSERT(capturedEvents[1] == EEvent::LOCK_RESPONSE);
        UNIT_ASSERT(capturedEvents[2] == EEvent::BOOT_EXTERNAL_REQUEST);
        UNIT_ASSERT(capturedEvents[3] == EEvent::BOOT_EXTERNAL_RESPONSE);
        UNIT_ASSERT(capturedEvents[4] == EEvent::START_REQUEST);
        UNIT_ASSERT(capturedEvents[5] == EEvent::START_RESPONSE);
        UNIT_ASSERT(capturedEvents[6] == EEvent::STOP_REQUEST);
        UNIT_ASSERT(capturedEvents[7] == EEvent::STOP_RESPONSE);
        UNIT_ASSERT(capturedEvents[8] == EEvent::UNLOCK_REQUEST);
        UNIT_ASSERT(capturedEvents[9] == EEvent::UNLOCK_RESPONSE);
        UNIT_ASSERT(capturedEvents[10] == EEvent::STOPPED);
    }

    Y_UNIT_TEST(ShouldNotHangIfLockIsLostDuringTabletBoot)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TActorId startVolumeActorId;

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvLockTabletRequest: {
                        startVolumeActorId = event->Sender;
                        break;
                    }
                    case TEvStateStorage::EvInfo: {
                        auto* msg = event->Get<TEvStateStorage::TEvInfo>();

                        {
                            auto request = std::make_unique<TEvHiveProxy::TEvTabletLockLost>(0);
                            runtime.Send(
                                new IEventHandle(
                                    startVolumeActorId,
                                    event->Sender,
                                    request.release(),
                                    0, // flags
                                    event->Cookie),
                                nodeIdx);
                        }

                        // trigger TabletDead message
                        {
                            auto request = std::make_unique<TEvStateStorage::TEvReplicaLeaderDemoted>();
                            request->Record.SetTabletID(msg->TabletID);
                            runtime.Send(
                                new IEventHandle(
                                    event->Recipient,
                                    event->Sender,
                                    request.release(),
                                    0, // flags
                                    event->Cookie),
                                nodeIdx);
                        }
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendMountVolumeRequest();
        auto response = service.RecvMountVolumeResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldStartVolumeIfBootSuggestIsOutdated)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        TActorId volumeActorId;
        bool volumeStopped = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        volumeActorId = msg->VolumeActor;
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeActorStopped:
                        volumeStopped = true;
                        break;
                    case TEvHiveProxy::EvBootExternalResponse: {
                        auto* msg = event->Get<TEvHiveProxy::TEvBootExternalResponse>();
                        auto* suggestedGeneration =
                            const_cast<ui32*>(&msg->SuggestedGeneration);
                        *suggestedGeneration = 1;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume();
        UNIT_ASSERT(volumeActorId);
        UNIT_ASSERT(!volumeStopped);

        service.SendRequest(volumeActorId, std::make_unique<TEvents::TEvPoisonPill>());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TEvVolume::EvWaitReadyResponse);
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT(volumeActorId);
        UNIT_ASSERT(!volumeStopped);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
