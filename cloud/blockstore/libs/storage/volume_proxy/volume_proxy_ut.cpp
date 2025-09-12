#include "volume_proxy.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/testlib/service_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <unordered_set>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());
}

ui32 SetupTestEnv(TTestEnv& env)
{
    env.CreateSubDomain("nbs");

    auto storageConfig = CreateTestStorageConfig({});

    return env.CreateBlockStoreNode(
        "nbs",
        storageConfig,
        CreateTestDiagnosticsConfig());
}

ui32 SetupTestEnv(
    TTestEnv& env,
    NProto::TStorageServiceConfig storageServiceConfig)
{
    env.CreateSubDomain("nbs");

    auto storageConfig =
        CreateTestStorageConfig(std::move(storageServiceConfig));

    return env.CreateBlockStoreNode(
        "nbs",
        storageConfig,
        CreateTestDiagnosticsConfig());
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeProxyTest)
{
    Y_UNIT_TEST(ShouldDescribeVolumeBeforeCreatingPipeToIt)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        bool detectedWaitReadyRequestToVolumeProxy = false;
        bool detectedWaitReadyResponseFromVolumeProxy = false;
        bool detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard = false;
        bool detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvWaitReadyRequest: {
                        if (!detectedWaitReadyRequestToVolumeProxy) {
                            detectedWaitReadyRequestToVolumeProxy = true;
                            UNIT_ASSERT(!detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard);
                            UNIT_ASSERT(!detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy);
                            UNIT_ASSERT(!detectedWaitReadyResponseFromVolumeProxy);
                        }
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        if (!detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard) {
                            detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard = true;
                            UNIT_ASSERT(detectedWaitReadyRequestToVolumeProxy);
                            UNIT_ASSERT(!detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy);
                            UNIT_ASSERT(!detectedWaitReadyResponseFromVolumeProxy);
                        }
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        if (!detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy) {
                            detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy = true;
                            UNIT_ASSERT(detectedWaitReadyRequestToVolumeProxy);
                            UNIT_ASSERT(detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard);
                            UNIT_ASSERT(!detectedWaitReadyResponseFromVolumeProxy);
                        }
                        break;
                    }
                    case TEvVolume::EvWaitReadyResponse: {
                        if (!detectedWaitReadyResponseFromVolumeProxy) {
                            detectedWaitReadyResponseFromVolumeProxy = true;
                            UNIT_ASSERT(detectedWaitReadyRequestToVolumeProxy);
                            UNIT_ASSERT(detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard);
                            UNIT_ASSERT(detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy);
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        service.CreateVolume();

        service.WaitForVolume();
        UNIT_ASSERT(detectedWaitReadyRequestToVolumeProxy);
        UNIT_ASSERT(detectedWaitReadyResponseFromVolumeProxy);
        UNIT_ASSERT(detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard);
        UNIT_ASSERT(detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy);
    }

    Y_UNIT_TEST(ShouldDescribeVolumeBeforeForwardingIfPipeFailed)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        TActorId volumeActorId;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvWaitReadyResponse: {
                        if (!volumeActorId) {
                            volumeActorId = event->Sender;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.WaitForVolume();
        UNIT_ASSERT(volumeActorId);

        bool droppedStatVolumeRequest = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvStatVolumeRequest: {
                        if (!droppedStatVolumeRequest
                                && event->GetRecipientRewrite() == volumeActorId)
                        {
                            droppedStatVolumeRequest = true;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendStatVolumeRequest();
        service.DestroyVolume();

        {
            // Upon broken pipe volume proxy should send back the response
            // with E_REJECTED error code
            auto response = service.RecvStatVolumeResponse();
            UNIT_ASSERT(droppedStatVolumeRequest);
            UNIT_ASSERT(response->GetStatus() == E_REJECTED);
        }

        // As the pipe is broken, volume proxy should send DescribeVolume
        // request to SchemeShard before attempting to forward the request
        // to volume
        bool detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard = false;
        bool detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard = true;
                        UNIT_ASSERT(!detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy);
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        UNIT_ASSERT(detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard);
                        detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        droppedStatVolumeRequest = false;
        service.SendStatVolumeRequest();
        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
        UNIT_ASSERT(response->GetStatus() != E_REJECTED);
        UNIT_ASSERT(FACILITY_FROM_CODE(response->GetStatus()) == FACILITY_SCHEMESHARD);
        UNIT_ASSERT(detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard);
        UNIT_ASSERT(detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy);
    }

    Y_UNIT_TEST(ShouldHandlePipeConnectionRestoration)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

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

        auto sessionId = service.MountVolume()->Record.GetSessionId();
        UNIT_ASSERT(volumeTabletId);

        service.UnmountVolume(sessionId);
        service.WaitForVolume();

        bool detectedConnect = false;
        bool detectedDisconnect = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvClientConnected: {
                        const auto* msg = event->Get<TEvTabletPipe::TEvClientConnected>();
                        if (msg->TabletId == volumeTabletId) {
                            detectedConnect = true;
                            UNIT_ASSERT(detectedDisconnect);
                        }
                        break;
                    }
                    case TEvTabletPipe::EvClientDestroyed: {
                        const auto* msg = event->Get<TEvTabletPipe::TEvClientDestroyed>();
                        if (msg->TabletId == volumeTabletId) {
                            detectedDisconnect = true;
                            UNIT_ASSERT(!detectedConnect);
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        UNIT_ASSERT(detectedDisconnect);

        service.WaitForVolume();
        UNIT_ASSERT(detectedConnect);

        bool detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard = false;
        bool detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard = true;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.StatVolume();
        UNIT_ASSERT(!detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard);
        UNIT_ASSERT(!detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy);
    }

    Y_UNIT_TEST(ShouldReleasePipeClientAfterInactivityTimeout)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        ui32 ppCount = 0;
        TMutex lock;
        THashSet<TActorId> pipeClients;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvSend: {
                        if (event->Type == TEvService::EvStatVolumeRequest) {
                            with_lock(lock) {
                                pipeClients.insert(event->GetRecipientRewrite());
                            }
                        }
                        break;
                    }
                    case TEvents::TSystem::Poison: {
                        with_lock(lock) {
                            if (pipeClients.count(event->Recipient)) {
                                ++ppCount;
                            }
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.WaitForVolume();

        service.StatVolume();

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));

        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT(ppCount != 0);
    }

    Y_UNIT_TEST(ShouldMapBaseDisk)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();
        service.WaitForVolume();

        ui64 volumeTabletId;
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
        service.DescribeVolume();

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                DefaultDiskId,
                volumeTabletId));
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                DefaultDiskId,
                volumeTabletId));

        service.StatVolume();

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                DefaultDiskId));
        service.StatVolume();

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                DefaultDiskId));

        bool describe = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        describe = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.StatVolume();
        UNIT_ASSERT(describe);
    }

    Y_UNIT_TEST(ShouldDetectRemoteTabletDeath)
    {
        TTestEnv env(1, 2);

        auto nodeIdx1 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service1(runtime, nodeIdx1);

        ui64 volumeTabletId = 0;
        std::unordered_set<TActorId> connections;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeVolumeResponse: {
                    if (!volumeTabletId) {
                        using TEvent = TEvSSProxy::TEvDescribeVolumeResponse;
                        auto* msg = event->template Get<TEvent>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                    }
                    break;
                }
                case TEvTabletPipe::EvClientConnected: {
                    auto* msg =
                        event->template Get<TEvTabletPipe::TEvClientConnected>();
                    if (volumeTabletId && msg->TabletId == volumeTabletId) {
                        connections.emplace(event->Recipient);
                    }
                    break;
                }
                case TEvTabletPipe::EvClientDestroyed: {
                    auto* msg =
                        event->template Get<TEvTabletPipe::TEvClientDestroyed>();
                    if (volumeTabletId && msg->TabletId == volumeTabletId) {
                        connections.erase(event->Recipient);
                    }
                    break;
                }
            }
            return false;
        });

        service1.CreateVolume();
        service1.WaitForVolume();

        auto nodeIdx2 = SetupTestEnv(env);
        TServiceClient service2(runtime, nodeIdx2);

        service2.StatVolume();

        UNIT_ASSERT_LE(2, connections.size());

        RebootTablet(runtime, volumeTabletId, service1.GetSender(), nodeIdx1);
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return connections.empty();
            }});
    }

    Y_UNIT_TEST(ShouldRunDescribeForCachedTablets)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();
        service.WaitForVolume();

        ui64 volumeTabletId;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->template Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return false;
            }
        );
        service.DescribeVolume();

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                DefaultDiskId,
                volumeTabletId));

        service.DestroyVolume();

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvClientDestroyed);
        runtime.DispatchEvents(options);

        {
            service.SendStatVolumeRequest();
            auto response = service.RecvStatVolumeResponse();
            auto code = response->GetStatus();
            UNIT_ASSERT_VALUES_EQUAL(
                FACILITY_SCHEMESHARD,
                FACILITY_FROM_CODE(code));
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimrScheme::StatusPathDoesNotExist,
                static_cast<NKikimrScheme::EStatus>(STATUS_FROM_CODE(code)));
        }
    }

    Y_UNIT_TEST(ShouldAnswerIfDisconnectedCachedVolumeIsOnlineAgain)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();
        service.WaitForVolume();

        ui64 volumeTabletId;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->template Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return false;
            }
        );
        service.DescribeVolume();

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                DefaultDiskId,
                volumeTabletId));

        service.StatVolume();

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);

        bool failConnects = true;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvClientConnected: {
                        auto* msg = event->template Get<TEvTabletPipe::TEvClientConnected>();
                        if (msg->TabletId == volumeTabletId) {
                            if (failConnects) {
                              auto& code =
                                const_cast<NKikimrProto::EReplyStatus&>(msg->Status);
                              code = NKikimrProto::ERROR;
                            }
                        }
                        break;
                    }
                }
                return false;
            }
        );

        {
            service.SendStatVolumeRequest();
            auto response = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        failConnects = false;

        service.StatVolume();
        service.StatVolume();
    }

    Y_UNIT_TEST(ShouldMapBaseDiskIfSchemeShardIsNotAvailable)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();
        service.WaitForVolume();

        ui64 volumeTabletId;
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
        service.DescribeVolume();

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                DefaultDiskId,
                volumeTabletId));
        service.StatVolume();

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        bool describe = false;
        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const_cast<NProto::TError&>(msg->Error) =
                                MakeError(E_REJECTED, "SS is dead");
                        describe = true;
                        break;
                    }
                }

                return false;
            });

        service.StatVolume();
        UNIT_ASSERT(describe);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
