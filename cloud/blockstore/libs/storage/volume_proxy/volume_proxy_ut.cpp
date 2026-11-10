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

#include <util/generic/size_literals.h>

#include <unordered_set>
#include <vector>

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
    Y_UNIT_TEST(ShouldRouteExternalVolumesByDiskAndTabletId)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        auto createVolume = [&](const TString& diskId)
        {
            service.CreateVolume(
                diskId,
                100_GB / DefaultBlockSize,
                DefaultBlockSize,
                "",
                "",
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
            service.WaitForVolume(diskId);
        };

        createVolume("tablet-a");
        createVolume("tablet-b");

        TVector<ui64> tabletIds;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() ==
                TEvSSProxy::EvDescribeVolumeResponse)
            {
                const auto& description =
                    event->Get<TEvSSProxy::TEvDescribeVolumeResponse>()
                        ->PathDescription.GetBlockStoreVolumeDescription();
                tabletIds.push_back(description.GetVolumeTabletId());
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });
        service.DescribeVolume("tablet-a");
        service.DescribeVolume("tablet-b");
        UNIT_ASSERT_VALUES_EQUAL(2, tabletIds.size());
        UNIT_ASSERT(tabletIds[0] != tabletIds[1]);

        TVector<TActorId> recipients;
        ui32 describeRequests = 0;
        runtime.SetEventFilter([&](auto&, auto& event) {
            if (event->GetTypeRewrite() ==
                    TEvSSProxy::EvDescribeVolumeRequest)
            {
                ++describeRequests;
            }
            if (event->GetTypeRewrite() ==
                    TEvVolume::EvReallocateDiskRequest &&
                event->GetRecipientRewrite() != MakeVolumeProxyServiceId())
            {
                recipients.push_back(event->GetRecipientRewrite());
            }
            return false;
        });

        auto reallocate = [&](const TString& diskId, ui64 tabletId)
        {
            auto request =
                std::make_unique<TEvVolume::TEvReallocateDiskRequest>();
            request->Record.SetDiskId(diskId);
            request->Record.SetExternalVolumeTabletId(tabletId);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            service.RecvResponse<TEvVolume::TEvReallocateDiskResponse>();
            UNIT_ASSERT_VALUES_EQUAL(0, describeRequests);
            UNIT_ASSERT(!recipients.empty());
            auto recipient = recipients.back();
            recipients.clear();
            return recipient;
        };

        const auto first = reallocate("external-a", tabletIds[0]);
        const auto shared = reallocate("external-b", tabletIds[0]);
        const auto second = reallocate("external-a", tabletIds[1]);
        const auto repeated = reallocate("external-a", tabletIds[0]);

        UNIT_ASSERT(first);
        UNIT_ASSERT_VALUES_EQUAL(first, shared);
        UNIT_ASSERT_VALUES_EQUAL(first, repeated);
        UNIT_ASSERT(second != first);
    }

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

    Y_UNIT_TEST(ShouldRejectBaseDiskWithoutTabletId)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TString baseDiskId = "base-disk";
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                baseDiskId,
                0));

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest(baseDiskId));
        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, response->GetStatus());

        UNIT_ASSERT(SUCCEEDED(service.StatVolume()->GetStatus()));
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

    Y_UNIT_TEST(ShouldRemoveOldRealDiskIdMappingsAfterRedescribe)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(
            TDuration::Seconds(1).MilliSeconds());
        const ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(1100));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvClientDestroyed);
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));

        const TString oldRealDiskId = "old-real-disk";
        const TString newRealDiskId = "new-real-disk";
        ui32 describeCount = 0;
        ui64 tabletId = 0;
        TActorId clientId;
        IEventHandlePtr heldDescribeResponse;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvSSProxy::EvDescribeVolumeResponse) {
                    const auto* msg = event->template Get<
                        TEvSSProxy::TEvDescribeVolumeResponse>();
                    tabletId =
                        msg->PathDescription.GetBlockStoreVolumeDescription()
                            .GetVolumeTabletId();
                    ++describeCount;
                    if (describeCount <= 2) {
                        heldDescribeResponse.reset(event.Release());
                        return true;
                    }
                } else if (
                    event->GetTypeRewrite() == TEvTabletPipe::EvClientConnected)
                {
                    const auto* msg =
                        event
                            ->template Get<TEvTabletPipe::TEvClientConnected>();
                    if (msg->TabletId == tabletId) {
                        clientId = msg->ClientId;
                    }
                }
                return false;
            });

        auto resolveWithRealDiskId = [&](const TString& realDiskId)
        {
            service.SendStatVolumeRequest();
            UNIT_ASSERT(runtime.DispatchEvents(
                TDispatchOptions{
                    .CustomFinalCondition =
                        [&]
                    {
                        return bool(heldDescribeResponse);
                    }},
                TDuration::Seconds(5)));

            const auto* msg =
                heldDescribeResponse
                    ->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
            auto description = msg->PathDescription;
            description.MutableBlockStoreVolumeDescription()->SetName(
                realDiskId);
            runtime.Send(
                new IEventHandle(
                    heldDescribeResponse->Recipient,
                    heldDescribeResponse->Sender,
                    new TEvSSProxy::TEvDescribeVolumeResponse(
                        msg->Path,
                        std::move(description)),
                    0,
                    heldDescribeResponse->Cookie),
                nodeIdx);
            heldDescribeResponse.reset();

            UNIT_ASSERT(
                SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        };

        resolveWithRealDiskId(oldRealDiskId);
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);
        UNIT_ASSERT(tabletId);
        UNIT_ASSERT(clientId);

        runtime.Send(
            new IEventHandle(
                MakeVolumeProxyServiceId(),
                service.GetSender(),
                new TEvTabletPipe::TEvClientDestroyed(tabletId, clientId, {})),
            nodeIdx);

        resolveWithRealDiskId(newRealDiskId);
        UNIT_ASSERT_VALUES_EQUAL(2, describeCount);

        auto request = service.CreateStatVolumeRequest(oldRealDiskId);
        request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(3, describeCount);
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldKeepResolvingConnectionOnSharedPipeEvents)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(
            TDuration::Seconds(1).MilliSeconds());
        const ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(1100));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvClientDestroyed);
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));

        auto sendStatRequest = [&](bool exactDiskIdMatch)
        {
            auto request = service.CreateStatVolumeRequest();
            request->Record.MutableHeaders()->SetExactDiskIdMatch(
                exactDiskIdMatch);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
        };

        TVector<IEventHandlePtr> initialDescribeResponses;
        IEventHandlePtr pendingDescribeResponse;
        ui64 tabletId = 0;
        ui64 firstConnectionId = 0;
        ui32 firstDescribeCount = 0;
        TActorId clientId;
        bool holdInitialResponses = true;
        bool holdFirstRedescribe = false;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeRequest &&
                    event->Cookie == firstConnectionId)
                {
                    ++firstDescribeCount;
                }
                if (event->GetTypeRewrite() ==
                    TEvSSProxy::EvDescribeVolumeResponse) {
                    if (holdInitialResponses) {
                        const auto* msg = event->template Get<
                            TEvSSProxy::TEvDescribeVolumeResponse>();
                        tabletId = msg->PathDescription
                                       .GetBlockStoreVolumeDescription()
                                       .GetVolumeTabletId();
                        initialDescribeResponses.emplace_back(event.Release());
                        return true;
                    }
                    if (holdFirstRedescribe &&
                        event->Cookie == firstConnectionId) {
                        UNIT_ASSERT(!pendingDescribeResponse);
                        pendingDescribeResponse.reset(event.Release());
                        return true;
                    }
                }
                if (event->GetTypeRewrite() == TEvTabletPipe::EvClientConnected)
                {
                    const auto* msg =
                        event
                            ->template Get<TEvTabletPipe::TEvClientConnected>();
                    if (msg->TabletId == tabletId &&
                        msg->Status == NKikimrProto::OK)
                    {
                        clientId = msg->ClientId;
                    }
                }
                return false;
            });

        sendStatRequest(false);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition =
                    [&]
                {
                    return initialDescribeResponses.size() == 1;
                }},
            TDuration::Seconds(5)));
        sendStatRequest(true);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition =
                    [&]
                {
                    return initialDescribeResponses.size() == 2;
                }},
            TDuration::Seconds(5)));
        UNIT_ASSERT(tabletId);
        firstConnectionId = initialDescribeResponses[0]->Cookie;
        UNIT_ASSERT(firstConnectionId != initialDescribeResponses[1]->Cookie);

        holdInitialResponses = false;
        for (auto& response: initialDescribeResponses) {
            runtime.Send(response.release(), nodeIdx);
            UNIT_ASSERT(
                SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        }
        UNIT_ASSERT(clientId);

        auto assertConnectionNotStarted = [&]
        {
            service.SendRequest(
                MakeVolumeProxyServiceId(),
                std::make_unique<TEvVolumeProxy::TEvKeepAliveRequest>(
                    DefaultDiskId));
            auto response =
                runtime.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveResponse>(
                    service.GetSender(),
                    TDuration::Seconds(5));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(
                E_INVALID_STATE,
                response->Get()->GetStatus());
        };

        auto sendPipeDestroyed = [&]
        {
            runtime.Send(
                new IEventHandle(
                    MakeVolumeProxyServiceId(),
                    service.GetSender(),
                    new TEvTabletPipe::TEvClientDestroyed(
                        tabletId,
                        clientId,
                        {})),
                nodeIdx);
            assertConnectionNotStarted();
        };

        sendPipeDestroyed();

        holdFirstRedescribe = true;
        sendStatRequest(false);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition =
                    [&]
                {
                    return bool(pendingDescribeResponse);
                }},
            TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(1, firstDescribeCount);

        sendStatRequest(true);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        UNIT_ASSERT(clientId);

        sendPipeDestroyed();
        UNIT_ASSERT_VALUES_EQUAL(1, firstDescribeCount);

        sendStatRequest(true);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(1, firstDescribeCount);

        runtime.Send(
            new IEventHandle(
                MakeVolumeProxyServiceId(),
                service.GetSender(),
                new TEvTabletPipe::TEvClientConnected(
                    tabletId,
                    NKikimrProto::ERROR,
                    clientId,
                    {},
                    true,
                    false,
                    0)),
            nodeIdx);
        assertConnectionNotStarted();
        UNIT_ASSERT_VALUES_EQUAL(1, firstDescribeCount);

        holdFirstRedescribe = false;
        runtime.Send(pendingDescribeResponse.release(), nodeIdx);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
    }

    Y_UNIT_TEST(ShouldKeepSharedPipeWhenFirstConnectionExpires)
    {
        const auto inactivityTimeout = TDuration::Seconds(1);
        const auto timeoutMargin = TDuration::MilliSeconds(100);
        const auto connectionStartDelay = inactivityTimeout / 2;

        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(
            inactivityTimeout.MilliSeconds());
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();

        // CreateVolume sends WaitReady through VolumeProxy. Expire the
        // connection created by that request so the two requests below create
        // separate connections.
        runtime.AdvanceCurrentTime(inactivityTimeout + timeoutMargin);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvClientDestroyed);
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));

        auto sendStatRequest = [&](bool exactDiskIdMatch, ui64 cookie)
        {
            auto request = service.CreateStatVolumeRequest();
            request->Record.MutableHeaders()->SetExactDiskIdMatch(
                exactDiskIdMatch);
            runtime.Send(
                new IEventHandle(
                    MakeVolumeProxyServiceId(),
                    service.GetSender(),
                    request.release(),
                    0,
                    cookie),
                nodeIdx);
        };

        std::vector<std::unique_ptr<IEventHandle>> describeResponses;
        ui64 volumeTabletId = 0;
        TActorId volumeActorId;
        bool holdDescribeResponses = true;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvStatVolumeRequest: {
                        if (event->GetRecipientRewrite() !=
                            MakeVolumeProxyServiceId()) {
                            volumeActorId = event->GetRecipientRewrite();
                        }
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        if (holdDescribeResponses) {
                            auto* msg = event->template Get<
                                TEvSSProxy::TEvDescribeVolumeResponse>();
                            const auto& volumeDescription =
                                msg->PathDescription
                                    .GetBlockStoreVolumeDescription();
                            volumeTabletId =
                                volumeDescription.GetVolumeTabletId();
                            describeResponses.emplace_back(event.Release());
                            return true;
                        }
                        break;
                    }
                }
                return false;
            });

        auto waitForDescribeResponses = [&](size_t count)
        {
            UNIT_ASSERT(runtime.DispatchEvents(
                TDispatchOptions{
                    .CustomFinalCondition = [&]
                    {
                        return describeResponses.size() == count;
                    }},
                TDuration::Seconds(5)));
        };

        sendStatRequest(false, 1);
        waitForDescribeResponses(1);

        sendStatRequest(true, 2);
        waitForDescribeResponses(2);
        UNIT_ASSERT(
            describeResponses[0]->Cookie != describeResponses[1]->Cookie);
        UNIT_ASSERT(volumeTabletId);

        holdDescribeResponses = false;
        const ui64 firstConnectionId = describeResponses[0]->Cookie;
        runtime.Send(describeResponses[0].release(), nodeIdx);
        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        UNIT_ASSERT(volumeActorId);

        runtime.AdvanceCurrentTime(connectionStartDelay);

        runtime.Send(describeResponses[1].release(), nodeIdx);
        response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());

        // Expire the first connection while the second one shares its pipe.
        runtime.AdvanceCurrentTime(
            inactivityTimeout - connectionStartDelay + timeoutMargin);
        options.FinalEvents.clear();
        options.FinalEvents.emplace_back(
            [firstConnectionId](IEventHandle& event)
            {
                if (event.GetTypeRewrite() != TEvents::TSystem::Wakeup) {
                    return false;
                }
                return event.Get<TEvents::TEvWakeup>()->Tag ==
                    firstConnectionId;
            });
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));

        bool requestDropped = false;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() == volumeActorId)
                {
                    requestDropped = true;
                    return true;
                }
                return false;
            });

        sendStatRequest(true, 3);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return requestDropped;
                }},
            TDuration::Seconds(5)));

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
    }

    void CheckSharedPipeWhenSecondConnectionExpires(bool checkFinalExpiry)
    {
        const auto inactivityTimeout = TDuration::Seconds(1);
        const auto timeoutMargin = TDuration::MilliSeconds(100);

        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(
            inactivityTimeout.MilliSeconds());
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        runtime.AdvanceCurrentTime(inactivityTimeout + timeoutMargin);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvClientDestroyed);
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));

        auto sendStatRequest = [&](bool exactDiskIdMatch)
        {
            auto request = service.CreateStatVolumeRequest();
            request->Record.MutableHeaders()->SetExactDiskIdMatch(
                exactDiskIdMatch);
            service.SendRequest(
                MakeVolumeProxyServiceId(),
                std::move(request));
        };

        TVector<IEventHandlePtr> describeResponses;
        ui64 volumeTabletId = 0;
        TActorId volumeActorId;
        bool holdDescribeResponses = true;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() !=
                        MakeVolumeProxyServiceId())
                {
                    volumeActorId = event->GetRecipientRewrite();
                }
                if (holdDescribeResponses &&
                    event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeResponse)
                {
                    const auto& volumeDescription = event->template Get<
                        TEvSSProxy::TEvDescribeVolumeResponse>()
                        ->PathDescription.GetBlockStoreVolumeDescription();
                    volumeTabletId = volumeDescription.GetVolumeTabletId();
                    describeResponses.emplace_back(event.Release());
                    return true;
                }
                return false;
            });

        sendStatRequest(false);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return describeResponses.size() == 1;
                }},
            TDuration::Seconds(5)));

        sendStatRequest(true);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return describeResponses.size() == 2;
                }},
            TDuration::Seconds(5)));
        UNIT_ASSERT(volumeTabletId);
        const ui64 firstConnectionId = describeResponses[0]->Cookie;
        const ui64 secondConnectionId = describeResponses[1]->Cookie;

        holdDescribeResponses = false;
        runtime.Send(describeResponses[0].release(), nodeIdx);
        UNIT_ASSERT_C(
            SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
            "first connection failed");

        runtime.AdvanceCurrentTime(inactivityTimeout / 2);
        runtime.Send(describeResponses[1].release(), nodeIdx);
        UNIT_ASSERT_C(
            SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
            "second connection failed");
        UNIT_ASSERT(volumeActorId);

        // Refresh the first connection so the second one expires first.
        sendStatRequest(false);
        UNIT_ASSERT_C(
            SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
            "first connection refresh failed");

        bool requestDropped = false;
        runtime.AdvanceCurrentTime(inactivityTimeout / 2 + timeoutMargin);
        if (checkFinalExpiry) {
            // Keep the first connection alive without an in-flight request.
            sendStatRequest(false);
            UNIT_ASSERT_C(
                SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
                "first connection refresh failed");
        } else {
            runtime.SetEventFilter(
                [&](auto&, auto& event)
                {
                    if (event->GetTypeRewrite() ==
                            TEvService::EvStatVolumeRequest &&
                        event->GetRecipientRewrite() == volumeActorId)
                    {
                        requestDropped = true;
                        return true;
                    }
                    return false;
                });

            sendStatRequest(false);
            UNIT_ASSERT(runtime.DispatchEvents(
                TDispatchOptions{
                    .CustomFinalCondition = [&]
                    {
                        return requestDropped;
                    }},
                TDuration::Seconds(5)));
        }

        runtime.AdvanceCurrentTime(inactivityTimeout / 2);
        options.FinalEvents.clear();
        options.FinalEvents.emplace_back(
            [secondConnectionId](IEventHandle& event)
            {
                return event.GetTypeRewrite() == TEvents::TSystem::Wakeup &&
                    event.Get<TEvents::TEvWakeup>()->Tag == secondConnectionId;
            });
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));

        if (checkFinalExpiry) {
            TActorId pipeClientId;
            runtime.SetEventFilter(
                [&](auto&, auto& event)
                {
                    if (event->GetTypeRewrite() == TEvTabletPipe::EvSend &&
                        event->Type == TEvService::EvStatVolumeRequest)
                    {
                        pipeClientId = event->GetRecipientRewrite();
                    }
                    return false;
                });

            sendStatRequest(false);
            UNIT_ASSERT_C(
                SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
                "surviving connection failed");
            UNIT_ASSERT(pipeClientId);
            const TActorId previousPipeClientId = pipeClientId;

            runtime.AdvanceCurrentTime(
                inactivityTimeout + timeoutMargin * 2);
            options.FinalEvents.clear();
            options.FinalEvents.emplace_back(
                [firstConnectionId](IEventHandle& event)
                {
                    return event.GetTypeRewrite() ==
                            TEvents::TSystem::Wakeup &&
                        event.Get<TEvents::TEvWakeup>()->Tag ==
                            firstConnectionId;
                });
            UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));

            sendStatRequest(false);
            UNIT_ASSERT_C(
                SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
                "new connection failed");
            UNIT_ASSERT(pipeClientId != previousPipeClientId);
        } else {
            RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
            auto response = runtime.GrabEdgeEvent<
                TEvService::TEvStatVolumeResponse>(
                service.GetSender(),
                TDuration::Seconds(5));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->Get()->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldKeepSharedPipeWhenSecondConnectionExpires)
    {
        CheckSharedPipeWhenSecondConnectionExpires(false);
    }

    Y_UNIT_TEST(ShouldShutdownPipeWhenLastConnectionExpires)
    {
        CheckSharedPipeWhenSecondConnectionExpires(true);
    }

    void CheckSharedPipeCleanupAfterDisconnect(bool completeFirstRequest)
    {
        const auto inactivityTimeout = TDuration::Seconds(1);
        const auto timeoutMargin = TDuration::MilliSeconds(100);

        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(
            inactivityTimeout.MilliSeconds());
        const ui32 nodeIdx = SetupTestEnv(env, config);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        TActorId volumeActorId;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() != MakeVolumeProxyServiceId())
                {
                    volumeActorId = event->GetRecipientRewrite();
                }
                return false;
            });
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        UNIT_ASSERT(volumeActorId);

        // Expire the setup connection before creating two shared connections.
        runtime.AdvanceCurrentTime(inactivityTimeout + timeoutMargin);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvClientDestroyed);
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(1)));

        auto sendStatRequest = [&](bool exactDiskIdMatch)
        {
            auto request = service.CreateStatVolumeRequest();
            request->Record.MutableHeaders()->SetExactDiskIdMatch(
                exactDiskIdMatch);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
        };
        auto waitFor = [&](auto condition)
        {
            UNIT_ASSERT(runtime.DispatchEvents(
                TDispatchOptions{.CustomFinalCondition = condition},
                TDuration::Seconds(1)));
        };

        TVector<IEventHandlePtr> describeResponses;
        bool holdDescribeResponses = true;
        bool dropRequests = !completeFirstRequest;
        ui32 droppedRequests = 0;
        ui64 tabletId = 0;
        TActorId pipeClientId;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (holdDescribeResponses &&
                    event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeResponse)
                {
                    const auto* msg = event->template Get<
                        TEvSSProxy::TEvDescribeVolumeResponse>();
                    tabletId = msg->PathDescription
                                   .GetBlockStoreVolumeDescription()
                                   .GetVolumeTabletId();
                    describeResponses.emplace_back(event.Release());
                    return true;
                }
                if (event->GetTypeRewrite() == TEvTabletPipe::EvSend &&
                    event->Type == TEvService::EvStatVolumeRequest)
                {
                    pipeClientId = event->GetRecipientRewrite();
                }
                if (dropRequests &&
                    event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() == volumeActorId)
                {
                    ++droppedRequests;
                    return true;
                }
                return false;
            });

        // Hold both lookups so ordinary and exact requests cannot reuse a
        // connection. Describe cookies and Wakeup tags are connection ids.
        sendStatRequest(false);
        waitFor([&] { return describeResponses.size() == 1; });
        sendStatRequest(true);
        waitFor([&] { return describeResponses.size() == 2; });
        const ui64 firstConnectionId = describeResponses[0]->Cookie;
        const ui64 secondConnectionId = describeResponses[1]->Cookie;
        UNIT_ASSERT(firstConnectionId != secondConnectionId);
        UNIT_ASSERT(tabletId);

        holdDescribeResponses = false;
        runtime.Send(describeResponses[0].release(), nodeIdx);
        if (completeFirstRequest) {
            UNIT_ASSERT(
                SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
            dropRequests = true;
            sendStatRequest(false);
        }
        waitFor([&] { return droppedRequests == 1; });
        const TActorId originalPipeClientId = pipeClientId;
        UNIT_ASSERT(originalPipeClientId);

        runtime.Send(describeResponses[1].release(), nodeIdx);
        waitFor([&] { return droppedRequests == 2; });
        UNIT_ASSERT_VALUES_EQUAL(originalPipeClientId, pipeClientId);

        if (completeFirstRequest) {
            // Consume the first connection's timer while its request is active.
            runtime.AdvanceCurrentTime(inactivityTimeout + timeoutMargin);
            options.FinalEvents.clear();
            options.FinalEvents.emplace_back(
                [firstConnectionId](IEventHandle& event)
                {
                    return event.GetTypeRewrite() == TEvents::TSystem::Wakeup &&
                        event.Get<TEvents::TEvWakeup>()->Tag == firstConnectionId;
                });
            UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(1)));
        }

        RebootTablet(runtime, tabletId, service.GetSender(), nodeIdx);
        for (ui32 i = 0; i != 2; ++i) {
            auto response =
                runtime.GrabEdgeEvent<TEvService::TEvStatVolumeResponse>(
                    service.GetSender(),
                    TDuration::Seconds(1));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->Get()->GetStatus());
        }

        // Only the second connection retries. The first stays idle after the
        // shared pipe reconnects and must not retain the replacement pipe.
        dropRequests = false;
        sendStatRequest(true);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        const TActorId replacementPipeClientId = pipeClientId;
        UNIT_ASSERT(replacementPipeClientId != originalPipeClientId);

        runtime.AdvanceCurrentTime(inactivityTimeout + timeoutMargin);
        options.FinalEvents.clear();
        THashSet<ui64> expiredConnections;
        options.FinalEvents.emplace_back(
            [&](IEventHandle& event)
            {
                if (event.GetTypeRewrite() == TEvents::TSystem::Wakeup) {
                    const ui64 connectionId =
                        event.Get<TEvents::TEvWakeup>()->Tag;
                    if (connectionId == firstConnectionId ||
                        connectionId == secondConnectionId)
                    {
                        expiredConnections.insert(connectionId);
                    }
                }
                return expiredConnections.size() == 2;
            });
        UNIT_ASSERT_C(
            runtime.DispatchEvents(options, TDuration::Seconds(1)),
            "Both connections must get a cleanup timer after disconnect");

        sendStatRequest(true);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        UNIT_ASSERT(pipeClientId != replacementPipeClientId);
    }

    Y_UNIT_TEST(ShouldShutdownSharedPipeAfterDisconnectWithConsumedTimer)
    {
        CheckSharedPipeCleanupAfterDisconnect(true);
    }

    Y_UNIT_TEST(ShouldShutdownSharedPipeAfterDisconnectOnFirstRequest)
    {
        CheckSharedPipeCleanupAfterDisconnect(false);
    }

    Y_UNIT_TEST(ShouldCancelRequestsForAllConnectionsSharingTablet)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(
            TDuration::Seconds(1).MilliSeconds());
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(1100));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvClientDestroyed);
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));

        auto sendStatRequest = [&](bool exactDiskIdMatch)
        {
            auto request = service.CreateStatVolumeRequest();
            request->Record.MutableHeaders()->SetExactDiskIdMatch(
                exactDiskIdMatch);
            service.SendRequest(
                MakeVolumeProxyServiceId(),
                std::move(request));
        };

        TVector<IEventHandlePtr> describeResponses;
        ui64 volumeTabletId = 0;
        TActorId volumeActorId;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() !=
                        MakeVolumeProxyServiceId())
                {
                    volumeActorId = event->GetRecipientRewrite();
                }
                if (event->GetTypeRewrite() ==
                    TEvSSProxy::EvDescribeVolumeResponse)
                {
                    const auto& volumeDescription = event->template Get<
                        TEvSSProxy::TEvDescribeVolumeResponse>()
                        ->PathDescription.GetBlockStoreVolumeDescription();
                    volumeTabletId = volumeDescription.GetVolumeTabletId();
                    describeResponses.emplace_back(event.Release());
                    return true;
                }
                return false;
            });

        sendStatRequest(false);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return describeResponses.size() == 1;
                }},
            TDuration::Seconds(5)));

        sendStatRequest(true);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return describeResponses.size() == 2;
                }},
            TDuration::Seconds(5)));
        UNIT_ASSERT(volumeTabletId);

        runtime.Send(describeResponses[0].release(), nodeIdx);
        UNIT_ASSERT_C(
            SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
            "first connection failed");
        runtime.Send(describeResponses[1].release(), nodeIdx);
        UNIT_ASSERT_C(
            SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
            "second connection failed");
        UNIT_ASSERT(volumeActorId);

        ui32 droppedRequests = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() == volumeActorId)
                {
                    ++droppedRequests;
                    return true;
                }
                return false;
            });

        sendStatRequest(false);
        sendStatRequest(true);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return droppedRequests == 2;
                }},
            TDuration::Seconds(5)));

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        for (int i = 0; i < 2; ++i) {
            auto response = runtime.GrabEdgeEvent<
                TEvService::TEvStatVolumeResponse>(
                service.GetSender(),
                TDuration::Seconds(5));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->Get()->GetStatus());
        }
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

    Y_UNIT_TEST(ShouldForwardRequestToPrimaryVolumeTablet)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1");
        service.WaitForVolume("Disk-1");

        auto statResponse = service.StatVolume("Disk-1");
        UNIT_ASSERT_VALUES_EQUAL(
            "Disk-1",
            statResponse->Record.GetVolume().GetDiskId());
    }

    Y_UNIT_TEST(ShouldForwardRequestToSecondaryVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        auto statResponse = service.StatVolume("Disk-1");
        UNIT_ASSERT_VALUES_EQUAL(
            "Disk-1-copy",
            statResponse->Record.GetVolume().GetDiskId());
    }

    Y_UNIT_TEST(
        ShouldActLikeSecondaryVolumeDoesNotExistWhenExactDiskIdMatchIsRequired)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        auto request = service.CreateStatVolumeRequest("Disk-1");
        request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
        auto statResponse = service.RecvStatVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist),
            statResponse->GetStatus(),
            FormatError(statResponse->GetError()));
    }

    Y_UNIT_TEST(ShouldFailRequestToSecondaryVolumeWhenItDoesNotExist)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1");
        service.WaitForVolume("Disk-1");

        {
            auto request = service.CreateStatVolumeRequest("Disk-1-copy");
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            auto statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist),
                statResponse->GetStatus(),
                FormatError(statResponse->GetError()));
        }

        {
            auto request = service.CreateStatVolumeRequest("Disk-1-copy");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            auto statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist),
                statResponse->GetStatus(),
                FormatError(statResponse->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldForwardRequestToPrimaryAndSecondaryVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1");
        service.WaitForVolume("Disk-1");

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        {
            // Forward to Disk-1
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1",
                statResponse->Record.GetVolume().GetDiskId());

            auto request = service.CreateStatVolumeRequest("Disk-1");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1",
                statResponse->Record.GetVolume().GetDiskId());
        }

        {
            // Forward to Disk-1-copy
            auto statResponse = service.StatVolume("Disk-1-copy");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());

            auto request = service.CreateStatVolumeRequest("Disk-1-copy");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }
    }

    Y_UNIT_TEST(ShouldDescribeOnceWhenPublicNameUsedFirst)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        ui64 describeCount = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        ++describeCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        {
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }
        {
            auto statResponse = service.StatVolume("Disk-1-copy");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }

        // The VolumeProxy should find both Disk-1 and Disk-1-copy when forward
        // first request to Disk-1.
        UNIT_ASSERT_VALUES_EQUAL(1ul, describeCount);
    }

    Y_UNIT_TEST(ShouldDescribeTwiceWhenSecondaryNameUsedFirst)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        ui64 describeCount = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        ++describeCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        {
            // The VolumeProxy should find only Disk-1-copy when forward
            // first request to Disk-1-copy.

            auto statResponse = service.StatVolume("Disk-1-copy");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1ul, describeCount);
        }
        {
            // The VolumeProxy should describe Disk-1 explicitly.

            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(2ul, describeCount);
        }
    }

    Y_UNIT_TEST(ShouldNotMakeDiskIdRemapping)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        {
            // Forward to Disk-1-copy
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }

        service.CreateVolume("Disk-1");
        service.WaitForVolume("Disk-1");

        {
            // Request requiring an exact DiskId match should be forward to the
            // newly created disk.
            auto request = service.CreateStatVolumeRequest("Disk-1");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            auto statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1",
                statResponse->Record.GetVolume().GetDiskId());
        }

        {
            // A request that does not require an exact name match should be
            // forward to the old disk, since the existing connection has not
            // been terminated.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        {
            // A request that does not require an exact name match should be
            // forward to the new disk, since the existing connection has been
            // terminated.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1",
                statResponse->Record.GetVolume().GetDiskId());
        }
    }

    Y_UNIT_TEST(ShouldNotDescribeVolumeForBaseDiskIfDataPlaneRequest)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume();
        service.WaitForVolume();

        ui64 volumeTabletId;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        auto* msg = event->template Get<
                            TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& volumeDescription =
                            msg->PathDescription
                                .GetBlockStoreVolumeDescription();
                        volumeTabletId = volumeDescription.GetVolumeTabletId();
                        break;
                    }
                }
                return false;
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

        runtime.ClearCounters();

        auto request = std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();
        request->Record.SetDiskId(DefaultDiskId);
        request->Record.SetStartIndex(0);
        request->Record.SetBlocksCount(1);

        service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
        service.StatVolume();

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            runtime.GetCounter(TEvSSProxy::EvDescribeVolumeResponse));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
