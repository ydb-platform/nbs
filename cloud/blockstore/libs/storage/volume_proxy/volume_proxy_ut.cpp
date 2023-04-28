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

#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

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

        runtime.SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
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
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
        runtime.SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvWaitReadyResponse: {
                        if (!volumeActorId) {
                            volumeActorId = event->Sender;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

        service.WaitForVolume();
        UNIT_ASSERT(volumeActorId);

        bool droppedStatVolumeRequest = false;
        runtime.SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvStatVolumeRequest: {
                        if (event->GetRecipientRewrite() == volumeActorId) {
                            droppedStatVolumeRequest = true;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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

        runtime.SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
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
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

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
        runtime.SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription.GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

        auto sessionId = service.MountVolume()->Record.GetSessionId();
        UNIT_ASSERT(volumeTabletId);

        service.UnmountVolume(sessionId);
        service.WaitForVolume();

        bool detectedConnect = false;
        bool detectedDisconnect = false;

        runtime.SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvClientConnected: {
                        auto* msg = reinterpret_cast<TEvTabletPipe::TEvClientConnected::TPtr*>(&event);
                        if ((*msg)->Get()->TabletId == volumeTabletId) {
                            detectedConnect = true;
                            UNIT_ASSERT(detectedDisconnect);
                        }
                        break;
                    }
                    case TEvTabletPipe::EvClientDestroyed: {
                        auto* msg = reinterpret_cast<TEvTabletPipe::TEvClientDestroyed::TPtr*>(&event);
                        if ((*msg)->Get()->TabletId == volumeTabletId) {
                            detectedDisconnect = true;
                            UNIT_ASSERT(!detectedConnect);
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        UNIT_ASSERT(detectedDisconnect);

        service.WaitForVolume();
        UNIT_ASSERT(detectedConnect);

        bool detectedDescribeVolumeRequestFromVolumeProxyToSchemeShard = false;
        bool detectedDescribeVolumeResponseFromSchemeShardToVolumeProxy = false;

        runtime.SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
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
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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

        runtime.SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
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
                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

        service.WaitForVolume();

        service.StatVolume();

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));

        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT(ppCount != 0);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
