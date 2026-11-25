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

#include <cloud/storage/core/libs/api/tablet_proxy.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

#include <functional>
#include <unordered_set>
#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

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

// DispatchEvents times out if the condition already holds.
void WaitFor(TTestActorRuntime& runtime, const std::function<bool()>& condition)
{
    if (condition()) {
        return;
    }
    UNIT_ASSERT(runtime.DispatchEvents(
        TDispatchOptions{.CustomFinalCondition = condition},
        TDuration::Seconds(5)));
}


void WaitForPipeClosed(TTestActorRuntime& runtime)
{
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvTabletProxy::EvPipeClosed);
    UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(5)));
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
            auto response =
                service.RecvResponse<TEvVolume::TEvReallocateDiskResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
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
        WaitForPipeClosed(runtime);

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
        WaitForPipeClosed(runtime);

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                DefaultDiskId));
        service.StatVolume();

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        WaitForPipeClosed(runtime);

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
        bool pipeClosed = false;
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
                    case TEvTabletProxy::EvPipeClosed: {
                        pipeClosed = true;
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

        WaitFor(runtime, [&] { return pipeClosed; });

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
        WaitForPipeClosed(runtime);

        const TString oldRealDiskId = "old-real-disk";
        const TString newRealDiskId = "new-real-disk";
        ui32 describeCount = 0;
        ui64 tabletId = 0;
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

        runtime.Send(
            new IEventHandle(
                MakeVolumeProxyServiceId(),
                service.GetSender(),
                new TEvTabletProxy::TEvPipeClosed(tabletId)),
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

    Y_UNIT_TEST(ShouldKeepResolvingVolumeInfoOnPipeClosed)
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
        WaitForPipeClosed(runtime);

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
        ui32 exactDescribeCount = 0;
        bool holdInitialResponses = true;
        bool holdFirstRedescribe = false;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvSSProxy::EvDescribeVolumeRequest)
                {
                    if (holdFirstRedescribe && !firstConnectionId) {
                        firstConnectionId = event->Cookie;
                    }
                    if (event->Cookie == firstConnectionId) {
                        ++firstDescribeCount;
                    }
                    const auto* msg = event->template Get<
                        TEvSSProxy::TEvDescribeVolumeRequest>();
                    if (msg->ExactDiskIdMatch) {
                        ++exactDescribeCount;
                    }
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
        UNIT_ASSERT(
            initialDescribeResponses[0]->Cookie !=
            initialDescribeResponses[1]->Cookie);

        holdInitialResponses = false;
        for (auto& response: initialDescribeResponses) {
            runtime.Send(response.release(), nodeIdx);
            UNIT_ASSERT(
                SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        }

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

        auto sendPipeClosed = [&]
        {
            runtime.Send(
                new IEventHandle(
                    MakeVolumeProxyServiceId(),
                    service.GetSender(),
                    new TEvTabletProxy::TEvPipeClosed(tabletId)),
                nodeIdx);
            assertConnectionNotStarted();
        };

        sendPipeClosed();

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
        UNIT_ASSERT_VALUES_EQUAL(2, exactDescribeCount);

        sendPipeClosed();
        UNIT_ASSERT_VALUES_EQUAL(1, firstDescribeCount);

        sendStatRequest(true);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(1, firstDescribeCount);
        UNIT_ASSERT_VALUES_EQUAL(3, exactDescribeCount);

        holdFirstRedescribe = false;
        runtime.Send(pendingDescribeResponse.release(), nodeIdx);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));
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
        WaitForPipeClosed(runtime);

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

    Y_UNIT_TEST(ShouldHandleLateDisconnect)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui64 tabletId = 0;
        ui32 describeCount = 0;
        ui32 droppedRequests = 0;
        bool dropRequests = true;
        bool holdDisconnects = true;
        bool pipeClosed = false;
        TVector<IEventHandlePtr> heldDisconnects;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletProxy::EvForward: {
                        tabletId =
                            event->template Get<TEvTabletProxy::TEvForward>()
                                ->TabletId;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        if (event->Sender == volumeProxyId) {
                            ++describeCount;
                        }
                        break;
                    }
                    case TEvService::EvStatVolumeRequest: {
                        if (dropRequests &&
                            event->GetRecipientRewrite() != volumeProxyId)
                        {
                            ++droppedRequests;
                            return true;
                        }
                        break;
                    }
                    case TEvTabletProxy::EvDisconnected: {
                        if (holdDisconnects) {
                            heldDisconnects.emplace_back(event.Release());
                            return true;
                        }
                        break;
                    }
                    case TEvTabletProxy::EvPipeClosed: {
                        pipeClosed = true;
                        break;
                    }
                }
                return false;
            });

        // Two requests are in flight when the tablet restarts.
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
        WaitFor(runtime, [&] { return droppedRequests == 2; });

        RebootTablet(runtime, tabletId, service.GetSender(), nodeIdx);
        WaitFor(
            runtime,
            [&] { return heldDisconnects.size() == 2 && pipeClosed; });
        dropRequests = false;
        holdDisconnects = false;

        // The pipe closed notice came first: no volume info is left.
        runtime.Send(heldDisconnects[0].release(), nodeIdx);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service.RecvStatVolumeResponse()->GetStatus());

        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);

        // The second disconnect keeps the new volume info.
        runtime.Send(heldDisconnects[1].release(), nodeIdx);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service.RecvStatVolumeResponse()->GetStatus());

        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);
    }

    Y_UNIT_TEST(ShouldDeliverResponseAfterVolumeInfoIsErased)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui32 describeCount = 0;
        bool holdReply = true;
        IEventHandlePtr heldReply;
        bool dropRequest = false;
        ui64 tabletId = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        if (event->Sender == volumeProxyId) {
                            ++describeCount;
                        }
                        break;
                    }
                    case TEvTabletProxy::EvForward: {
                        tabletId =
                            event->template Get<TEvTabletProxy::TEvForward>()
                                ->TabletId;
                        break;
                    }
                    case TEvService::EvStatVolumeRequest: {
                        if (dropRequest &&
                            event->GetRecipientRewrite() != volumeProxyId)
                        {
                            dropRequest = false;
                            return true;
                        }
                        break;
                    }
                    case TEvService::EvStatVolumeResponse: {
                        if (holdReply &&
                            event->GetRecipientRewrite() == volumeProxyId)
                        {
                            holdReply = false;
                            heldReply.reset(event.Release());
                            return true;
                        }
                        break;
                    }
                }
                return false;
            });

        // The volume answered, the reply is on its way to the volume proxy.
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return bool(heldReply);
                }},
            TDuration::Seconds(5)));

        // Another request fails on a disconnect and drops the volume info.
        dropRequest = true;
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return !dropRequest;
                }},
            TDuration::Seconds(5)));
        RebootTablet(runtime, tabletId, service.GetSender(), nodeIdx);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service.RecvStatVolumeResponse()->GetStatus());

        runtime.Send(heldReply.release(), nodeIdx);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));

        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);
    }

    Y_UNIT_TEST(ShouldDeliverResponseAfterVolumeInfoExpires)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(
            TDuration::Seconds(10).MilliSeconds());
        const ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui32 describeCount = 0;
        IEventHandlePtr heldReply;
        bool replyHeld = false;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        if (event->Sender == volumeProxyId) {
                            ++describeCount;
                        }
                        break;
                    }
                    case TEvService::EvStatVolumeResponse: {
                        if (!replyHeld &&
                            event->GetRecipientRewrite() == volumeProxyId)
                        {
                            replyHeld = true;
                            heldReply.reset(event.Release());
                            return true;
                        }
                        break;
                    }
                }
                return false;
            });

        // The volume answered, the reply is on its way to the volume proxy.
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return bool(heldReply);
                }},
            TDuration::Seconds(5)));

        // A request in flight does not keep the volume info: it expires.
        runtime.AdvanceCurrentTime(TDuration::Seconds(11));
        TDispatchOptions wakeup;
        wakeup.FinalEvents.emplace_back(
            [&](IEventHandle& event)
            {
                return event.GetTypeRewrite() == TEvents::TSystem::Wakeup &&
                       event.GetRecipientRewrite() == volumeProxyId;
            });
        UNIT_ASSERT(runtime.DispatchEvents(wakeup, TDuration::Seconds(5)));

        // Nothing is known about the disk any more: a ping finds no tablet.
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolumeProxy::TEvKeepAliveRequest>(
                DefaultDiskId));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            service.RecvResponse<TEvVolumeProxy::TEvKeepAliveResponse>()
                ->GetStatus());

        // The reply is still delivered, the next request describes again.
        runtime.Send(heldReply.release(), nodeIdx);
        UNIT_ASSERT(SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()));

        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);
    }

    Y_UNIT_TEST(ShouldNotUseClearedBaseDiskMappingAfterFailure)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui64 otherTabletId = 0;
        bool dropRequest = false;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (!otherTabletId &&
                    event->GetTypeRewrite() == TEvTabletProxy::EvForward)
                {
                    otherTabletId =
                        event->template Get<TEvTabletProxy::TEvForward>()
                            ->TabletId;
                }
                if (dropRequest &&
                    event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() != volumeProxyId)
                {
                    dropRequest = false;
                    return true;
                }
                return false;
            });

        service.CreateVolume("other");
        UNIT_ASSERT(otherTabletId);

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                DefaultDiskId,
                otherTabletId));
        service.CreateVolume();
        UNIT_ASSERT_VALUES_EQUAL(
            "other",
            service.StatVolume()->Record.GetVolume().GetDiskId());

        // A request is in flight through the mapping when it is cleared.
        dropRequest = true;
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return !dropRequest;
                }},
            TDuration::Seconds(5)));

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                DefaultDiskId));

        RebootTablet(runtime, otherTabletId, service.GetSender(), nodeIdx);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service.RecvStatVolumeResponse()->GetStatus());

        UNIT_ASSERT_VALUES_EQUAL(
            DefaultDiskId,
            service.StatVolume()->Record.GetVolume().GetDiskId());
    }

    Y_UNIT_TEST(ShouldAnswerRequestsOnPoisonPill)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui32 forwardCount = 0;
        bool requestDropped = false;
        IEventHandlePtr describeResponse;
        IEventHandlePtr pingRequest;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletProxy::EvForward: {
                        ++forwardCount;
                        break;
                    }
                    case TEvService::EvStatVolumeRequest: {
                        if (event->GetRecipientRewrite() != volumeProxyId) {
                            requestDropped = true;
                            return true;
                        }
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        describeResponse.reset(event.Release());
                        return true;
                    }
                    case TEvTabletProxy::EvKeepAliveRequest: {
                        pingRequest.reset(event.Release());
                        return true;
                    }
                }
                return false;
            });

        auto send = [&](IEventBase* request, ui64 cookie)
        {
            runtime.Send(
                new IEventHandle(
                    MakeVolumeProxyServiceId(),
                    service.GetSender(),
                    request,
                    0,
                    cookie),
                nodeIdx);
        };

        // One request is in flight, another waits for the describe, a ping
        // waits for the tablet proxy.
        send(service.CreateStatVolumeRequest().release(), 1);
        send(service.CreateStatVolumeRequest("unknown").release(), 2);
        send(new TEvVolumeProxy::TEvKeepAliveRequest(DefaultDiskId), 3);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return requestDropped && describeResponse && pingRequest;
                }},
            TDuration::Seconds(5)));

        send(new TEvents::TEvPoisonPill(), 0);

        TVector<ui64> cookies;
        for (ui32 i = 0; i != 2; ++i) {
            auto response =
                runtime.GrabEdgeEvent<TEvService::TEvStatVolumeResponse>(
                    service.GetSender(),
                    TDuration::Seconds(5));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->Get()->GetStatus());
            cookies.push_back(response->Cookie);
        }
        Sort(cookies);
        UNIT_ASSERT_VALUES_EQUAL(1, cookies[0]);
        UNIT_ASSERT_VALUES_EQUAL(2, cookies[1]);

        auto pingResponse =
            runtime.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveResponse>(
                service.GetSender(),
                TDuration::Seconds(5));
        UNIT_ASSERT(pingResponse);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, pingResponse->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(3, pingResponse->Cookie);

        runtime.Send(describeResponse.release(), nodeIdx);

        runtime.Send(
            new IEventHandle(
                MakeVolumeProxyServiceId(),
                service.GetSender(),
                service.CreateStatVolumeRequest().release(),
                IEventHandle::FlagTrackDelivery,
                0),
            nodeIdx);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(
            service.GetSender(),
            TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(1, forwardCount);
    }

    Y_UNIT_TEST(ShouldKeepBaseDiskPipeAlive)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        ui64 volumeTabletId = 0;
        TVector<ui64> keepAlives;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        volumeTabletId =
                            event->template Get<
                                TEvSSProxy::TEvDescribeVolumeResponse>()
                                ->PathDescription
                                .GetBlockStoreVolumeDescription()
                                .GetVolumeTabletId();
                        break;
                    }
                    case TEvTabletProxy::EvKeepAliveRequest: {
                        keepAlives.push_back(
                            event
                                ->template Get<
                                    TEvTabletProxy::TEvKeepAliveRequest>()
                                ->TabletId);
                        break;
                    }
                }
                return false;
            });
        service.DescribeVolume();
        UNIT_ASSERT(volumeTabletId);

        auto keepAlive = [&](const TString& diskId)
        {
            service.SendRequest(
                MakeVolumeProxyServiceId(),
                std::make_unique<TEvVolumeProxy::TEvKeepAliveRequest>(diskId));
            return service.RecvResponse<TEvVolumeProxy::TEvKeepAliveResponse>()
                ->GetStatus();
        };

        // An unknown disk has no pipe to keep.
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, keepAlive("unknown"));
        UNIT_ASSERT_VALUES_EQUAL(0, keepAlives.size());

        // The volume info resolved for the create request serves the ping of
        // a partition that registers no mapping; the pipe was opened by that
        // request.
        UNIT_ASSERT_VALUES_EQUAL(S_OK, keepAlive(DefaultDiskId));
        UNIT_ASSERT_VALUES_EQUAL(1, keepAlives.size());
        UNIT_ASSERT_VALUES_EQUAL(volumeTabletId, keepAlives[0]);

        // A mapped base disk needs no volume info.
        const TString baseDiskId = "base";
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                baseDiskId,
                volumeTabletId));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, keepAlive(baseDiskId));
        UNIT_ASSERT_VALUES_EQUAL(2, keepAlives.size());
        UNIT_ASSERT_VALUES_EQUAL(volumeTabletId, keepAlives[1]);

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                baseDiskId));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, keepAlive(baseDiskId));
        UNIT_ASSERT_VALUES_EQUAL(2, keepAlives.size());

        // A ping does not open a pipe: the tablet proxy's refusal is relayed.
        const TString idleDiskId = "idle";
        const ui64 idleTabletId = volumeTabletId + 1000;
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                idleDiskId,
                idleTabletId));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, keepAlive(idleDiskId));
        UNIT_ASSERT_VALUES_EQUAL(3, keepAlives.size());
        UNIT_ASSERT_VALUES_EQUAL(idleTabletId, keepAlives[2]);
    }

    Y_UNIT_TEST(ShouldAnswerEachBaseDiskPing)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        ui64 volumeTabletId = 0;
        bool holdPings = true;
        TVector<IEventHandlePtr> heldPings;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        volumeTabletId =
                            event->template Get<
                                TEvSSProxy::TEvDescribeVolumeResponse>()
                                ->PathDescription
                                .GetBlockStoreVolumeDescription()
                                .GetVolumeTabletId();
                        break;
                    }
                    case TEvTabletProxy::EvKeepAliveRequest: {
                        if (holdPings) {
                            heldPings.emplace_back(event.Release());
                            return true;
                        }
                        break;
                    }
                }
                return false;
            });
        service.DescribeVolume();
        UNIT_ASSERT(volumeTabletId);

        // One base disk has a pipe, opened by the create; the other has none.
        const TString idleDiskId = "idle";
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                idleDiskId,
                volumeTabletId + 1000));

        const TActorId first = runtime.AllocateEdgeActor(nodeIdx);
        const TActorId second = runtime.AllocateEdgeActor(nodeIdx);
        auto ping = [&](const TActorId& sender, const TString& diskId, ui64 cookie)
        {
            runtime.Send(
                new IEventHandle(
                    MakeVolumeProxyServiceId(),
                    sender,
                    new TEvVolumeProxy::TEvKeepAliveRequest(diskId),
                    0,
                    cookie),
                nodeIdx);
        };
        ping(first, DefaultDiskId, 1);
        ping(second, idleDiskId, 2);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return heldPings.size() == 2;
                }},
            TDuration::Seconds(5)));

        // Both pings wait; the answers come in the reverse order.
        holdPings = false;
        runtime.Send(heldPings[1].release(), nodeIdx);
        runtime.Send(heldPings[0].release(), nodeIdx);

        auto grab = [&](const TActorId& sender)
        {
            auto response =
                runtime.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveResponse>(
                    sender,
                    TDuration::Seconds(5));
            UNIT_ASSERT(response);
            return std::make_pair(
                response->Cookie,
                response->Get()->GetStatus());
        };
        UNIT_ASSERT_VALUES_EQUAL(
            std::make_pair(ui64(1), ui32(S_OK)),
            grab(first));
        UNIT_ASSERT_VALUES_EQUAL(
            std::make_pair(ui64(2), ui32(E_INVALID_STATE)),
            grab(second));
    }

    Y_UNIT_TEST(ShouldDescribeAfterIdleTabletRestart)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui64 tabletId = 0;
        ui32 describeCount = 0;
        ui32 pipeClosedCount = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletProxy::EvForward: {
                        tabletId =
                            event->template Get<TEvTabletProxy::TEvForward>()
                                ->TabletId;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        if (event->Sender == volumeProxyId) {
                            ++describeCount;
                        }
                        break;
                    }
                    case TEvTabletProxy::EvPipeClosed: {
                        ++pipeClosedCount;
                        break;
                    }
                }
                return false;
            });

        service.StatVolume();
        UNIT_ASSERT(tabletId);
        UNIT_ASSERT_VALUES_EQUAL(0, describeCount);

        // Each idle pipe break costs one describe and no error.
        for (ui32 i = 1; i != 3; ++i) {
            RebootTablet(runtime, tabletId, service.GetSender(), nodeIdx);
            UNIT_ASSERT(runtime.DispatchEvents(
                TDispatchOptions{
                    .CustomFinalCondition = [&]
                    {
                        return pipeClosedCount == i;
                    }},
                TDuration::Seconds(5)));

            service.StatVolume();
            UNIT_ASSERT_VALUES_EQUAL(i, describeCount);
            service.StatVolume();
            UNIT_ASSERT_VALUES_EQUAL(i, describeCount);
        }
    }

    Y_UNIT_TEST(ShouldHandleLatePipeClosedNotice)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui64 tabletId = 0;
        ui32 describeCount = 0;
        ui32 pipeClosedCount = 0;
        bool holdPipeClosed = true;
        IEventHandlePtr heldPipeClosed;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletProxy::EvForward: {
                        tabletId =
                            event->template Get<TEvTabletProxy::TEvForward>()
                                ->TabletId;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        if (event->Sender == volumeProxyId) {
                            ++describeCount;
                        }
                        break;
                    }
                    case TEvTabletProxy::EvPipeClosed: {
                        if (holdPipeClosed) {
                            holdPipeClosed = false;
                            heldPipeClosed.reset(event.Release());
                            return true;
                        }
                        ++pipeClosedCount;
                        break;
                    }
                }
                return false;
            });

        service.StatVolume();
        UNIT_ASSERT(tabletId);

        // The pipe broke, the notice is on its way to the volume proxy.
        RebootTablet(runtime, tabletId, service.GetSender(), nodeIdx);
        WaitFor(runtime, [&] { return bool(heldPipeClosed); });

        // A request opens a new pipe using the old volume info.
        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(0, describeCount);

        // The late notice drops the volume info: one more describe.
        runtime.Send(heldPipeClosed.release(), nodeIdx);
        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);

        // The new pipe is watched: its break is noticed too.
        pipeClosedCount = 0;
        RebootTablet(runtime, tabletId, service.GetSender(), nodeIdx);
        WaitFor(runtime, [&] { return pipeClosedCount == 1; });
        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(2, describeCount);
    }

    Y_UNIT_TEST(ShouldResolveRecreatedVolume)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        ui64 tabletId = 0;
        ui32 pipeClosedCount = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletProxy::EvForward: {
                        tabletId =
                            event->template Get<TEvTabletProxy::TEvForward>()
                                ->TabletId;
                        break;
                    }
                    case TEvTabletProxy::EvPipeClosed: {
                        ++pipeClosedCount;
                        break;
                    }
                }
                return false;
            });

        service.StatVolume();
        const ui64 oldTabletId = tabletId;
        UNIT_ASSERT(oldTabletId);

        // The pipe to the destroyed tablet breaks.
        service.DestroyVolume();
        WaitFor(runtime, [&] { return pipeClosedCount != 0; });

        service.SendDestroyVolumeRequest();
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            service.RecvDestroyVolumeResponse()->GetStatus());

        service.CreateVolume();

        UNIT_ASSERT_VALUES_EQUAL(
            DefaultDiskId,
            service.StatVolume()->Record.GetVolume().GetDiskId());
        UNIT_ASSERT(tabletId != oldTabletId);
    }

    Y_UNIT_TEST(ShouldResolvePublicNameAfterCopyBackAndIdleRestart)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("disk-copy");
        service.WaitForVolume("disk-copy");

        ui64 tabletId = 0;
        ui32 pipeClosedCount = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletProxy::EvForward: {
                        tabletId =
                            event->template Get<TEvTabletProxy::TEvForward>()
                                ->TabletId;
                        break;
                    }
                    case TEvTabletProxy::EvPipeClosed: {
                        ++pipeClosedCount;
                        break;
                    }
                }
                return false;
            });

        // Only the copy exists: the public name leads to it.
        UNIT_ASSERT_VALUES_EQUAL(
            "disk-copy",
            service.StatVolume("disk")->Record.GetVolume().GetDiskId());
        const ui64 copyTabletId = tabletId;
        UNIT_ASSERT(copyTabletId);

        service.CreateVolume("disk");
        service.WaitForVolume("disk");

        // The copy restarts with nothing in flight.
        pipeClosedCount = 0;
        RebootTablet(runtime, copyTabletId, service.GetSender(), nodeIdx);
        WaitFor(runtime, [&] { return pipeClosedCount != 0; });

        UNIT_ASSERT_VALUES_EQUAL(
            "disk",
            service.StatVolume("disk")->Record.GetVolume().GetDiskId());
    }

    Y_UNIT_TEST(ShouldForgetPublicAliasWhenSecondaryVolumeIsRecreated)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("other");
        service.WaitForVolume("other");
        service.CreateVolume("disk-copy");
        service.WaitForVolume("disk-copy");

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        TVector<ui64> closedTabletIds;
        ui64 tabletId = 0;
        ui32 describeCount = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletProxy::EvForward: {
                        tabletId =
                            event->template Get<TEvTabletProxy::TEvForward>()
                                ->TabletId;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        if (event->Sender == volumeProxyId) {
                            ++describeCount;
                        }
                        break;
                    }
                    case TEvTabletProxy::EvPipeClosed: {
                        closedTabletIds.push_back(
                            event->template Get<TEvTabletProxy::TEvPipeClosed>()
                                ->TabletId);
                        break;
                    }
                }
                return false;
            });

        // Two volume infos of one tablet.
        service.StatVolume("other");
        UNIT_ASSERT_VALUES_EQUAL(
            "disk-copy",
            service.StatVolume("disk-copy")->Record.GetVolume().GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            "disk-copy",
            service.StatVolume("disk")->Record.GetVolume().GetDiskId());
        const ui64 oldTabletId = tabletId;
        UNIT_ASSERT(oldTabletId);

        service.DestroyVolume("disk-copy");
        WaitFor(runtime, [&] { return !closedTabletIds.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(1, closedTabletIds.size());
        UNIT_ASSERT_VALUES_EQUAL(oldTabletId, closedTabletIds[0]);

        service.CreateVolume("disk-copy");
        service.WaitForVolume("disk-copy");

        // Both volume infos are gone, the public name leads to the new copy.
        UNIT_ASSERT_VALUES_EQUAL(
            "disk-copy",
            service.StatVolume("disk")->Record.GetVolume().GetDiskId());
        UNIT_ASSERT(tabletId != oldTabletId);
        UNIT_ASSERT_VALUES_EQUAL(
            "disk-copy",
            service.StatVolume("disk-copy")->Record.GetVolume().GetDiskId());

        // The volume info of another tablet is not affected.
        const ui32 describes = describeCount;
        service.StatVolume("other");
        UNIT_ASSERT_VALUES_EQUAL(describes, describeCount);
    }

    Y_UNIT_TEST(ShouldForgetUnusedVolumeInfo)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(
            TDuration::Seconds(10).MilliSeconds());
        const ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui32 describeCount = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeRequest &&
                    event->Sender == volumeProxyId)
                {
                    ++describeCount;
                }
                return false;
            });

        auto keepAlive = [&]
        {
            service.SendRequest(
                MakeVolumeProxyServiceId(),
                std::make_unique<TEvVolumeProxy::TEvKeepAliveRequest>(
                    DefaultDiskId));
            return service.RecvResponse<TEvVolumeProxy::TEvKeepAliveResponse>()
                ->GetStatus();
        };

        TDispatchOptions wakeup;
        wakeup.FinalEvents.emplace_back(
            [&](IEventHandle& event)
            {
                return event.GetTypeRewrite() == TEvents::TSystem::Wakeup &&
                       event.GetRecipientRewrite() == volumeProxyId;
            });

        // A keep-alive counts as activity: the check after the timeout finds
        // the volume info used and keeps it.
        runtime.AdvanceCurrentTime(TDuration::Seconds(6));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, keepAlive());
        runtime.AdvanceCurrentTime(TDuration::Seconds(6));
        UNIT_ASSERT(runtime.DispatchEvents(wakeup, TDuration::Seconds(5)));
        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(0, describeCount);

        // Unused for a whole timeout: forgotten, the next request describes.
        runtime.AdvanceCurrentTime(TDuration::Seconds(11));
        UNIT_ASSERT(runtime.DispatchEvents(wakeup, TDuration::Seconds(5)));
        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);
    }

    Y_UNIT_TEST(ShouldKeepUnusedVolumeInfoWithZeroTimeout)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetVolumeProxyPipeInactivityTimeout(0);
        const ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui32 describeCount = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeRequest &&
                    event->Sender == volumeProxyId)
                {
                    ++describeCount;
                }
                return false;
            });

        // The volume info of the created volume is used without a describe.
        runtime.AdvanceCurrentTime(TDuration::Hours(1));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(0, describeCount);
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
        WaitForPipeClosed(runtime);

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

    Y_UNIT_TEST(ShouldDescribeTwiceWhenPublicNameUsedFirst)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        // adjust time to trigger pipe connection destroy
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        WaitForPipeClosed(runtime);

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

        // The VolumeProxy should describe each requested name on its own.
        UNIT_ASSERT_VALUES_EQUAL(2ul, describeCount);
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
        WaitForPipeClosed(runtime);

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
        WaitForPipeClosed(runtime);

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
        WaitForPipeClosed(runtime);

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

    Y_UNIT_TEST(ShouldKeepCopyRouteWhenExactLookupFails)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        {
            // Disk-1 resolves to its copy.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }

        {
            // There is no Disk-1, the exact request fails.
            auto request = service.CreateStatVolumeRequest("Disk-1");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            auto statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist),
                statResponse->GetStatus());
        }

        runtime.ClearCounters();

        {
            // The route to the copy is used as before, without a describe.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                runtime.GetCounter(TEvSSProxy::EvDescribeVolumeRequest));
        }
    }

    Y_UNIT_TEST(ShouldNotUseBaseDiskMappingForExactRequest)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

        ui64 copyTabletId = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvSSProxy::EvDescribeVolumeResponse)
                {
                    auto* msg = event->template Get<
                        TEvSSProxy::TEvDescribeVolumeResponse>();
                    copyTabletId =
                        msg->PathDescription.GetBlockStoreVolumeDescription()
                            .GetVolumeTabletId();
                }
                return false;
            });
        service.DescribeVolume("Disk-1-copy");
        UNIT_ASSERT(copyTabletId);
        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);

        // The base disk mapping of Disk-1 points to the tablet of the copy.
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                "Disk-1",
                copyTabletId));

        {
            // The exact request does not take the tablet from the mapping.
            auto request = service.CreateStatVolumeRequest("Disk-1");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            auto statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist),
                statResponse->GetStatus());
        }

        {
            // The request without the exact match goes to the mapped tablet.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }

        {
            // The exact request does not use the volume info of the base disk.
            auto request = service.CreateStatVolumeRequest("Disk-1");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            auto statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist),
                statResponse->GetStatus());
        }

        service.CreateVolume("Disk-1");
        service.WaitForVolume("Disk-1");

        {
            // The exact request goes to the created Disk-1.
            auto request = service.CreateStatVolumeRequest("Disk-1");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            auto statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1",
                statResponse->Record.GetVolume().GetDiskId());
        }

        {
            // The overlay requests still go to the mapped tablet.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }
    }

    Y_UNIT_TEST(ShouldUseBaseDiskMappingOverCachedVolumeInfo)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1-copy");
        service.CreateVolume("Disk-1");

        ui64 copyTabletId = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvSSProxy::EvDescribeVolumeResponse)
                {
                    auto* msg = event->template Get<
                        TEvSSProxy::TEvDescribeVolumeResponse>();
                    copyTabletId =
                        msg->PathDescription.GetBlockStoreVolumeDescription()
                            .GetVolumeTabletId();
                }
                return false;
            });
        service.DescribeVolume("Disk-1-copy");
        UNIT_ASSERT(copyTabletId);
        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);

        {
            // Disk-1 is resolved by its name before the mapping is set.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1",
                statResponse->Record.GetVolume().GetDiskId());
        }

        // The base disk mapping of Disk-1 points to the tablet of the copy.
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                "Disk-1",
                copyTabletId));

        {
            // The exact request goes to Disk-1.
            auto request = service.CreateStatVolumeRequest("Disk-1");
            request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            auto statResponse = service.RecvStatVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1",
                statResponse->Record.GetVolume().GetDiskId());
        }

        {
            // The overlay requests go to the mapped tablet.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                "Disk-1"));

        {
            // Without the mapping Disk-1 is resolved by its name again.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1",
                statResponse->Record.GetVolume().GetDiskId());
        }
    }

    Y_UNIT_TEST(ShouldNotUseVolumeInfoOfClearedBaseDiskMapping)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk-1");
        service.CreateVolume("Disk-A");
        service.CreateVolume("Disk-B");

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui64 describedTabletId = 0;
        bool holdDescribeResponse = false;
        IEventHandlePtr describeResponse;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() !=
                    TEvSSProxy::EvDescribeVolumeResponse)
                {
                    return false;
                }
                if (holdDescribeResponse &&
                    event->GetRecipientRewrite() == volumeProxyId)
                {
                    holdDescribeResponse = false;
                    describeResponse.reset(event.Release());
                    return true;
                }
                auto* msg =
                    event->template Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                describedTabletId =
                    msg->PathDescription.GetBlockStoreVolumeDescription()
                        .GetVolumeTabletId();
                return false;
            });

        service.DescribeVolume("Disk-A");
        const ui64 tabletIdA = describedTabletId;
        service.DescribeVolume("Disk-B");
        const ui64 tabletIdB = describedTabletId;
        UNIT_ASSERT(tabletIdA);
        UNIT_ASSERT(tabletIdB);
        UNIT_ASSERT_VALUES_UNEQUAL(tabletIdA, tabletIdB);

        auto mapBaseDisk = [&](ui64 tabletId)
        {
            service.SendRequest(
                MakeVolumeProxyServiceId(),
                std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
                    "Disk-1",
                    tabletId));
        };
        auto clearBaseDisk = [&]
        {
            service.SendRequest(
                MakeVolumeProxyServiceId(),
                std::make_unique<
                    TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>("Disk-1"));
        };

        // The base disk mapping of Disk-1 points to the tablet of Disk-A.
        mapBaseDisk(tabletIdA);
        UNIT_ASSERT_VALUES_EQUAL(
            "Disk-A",
            service.StatVolume("Disk-1")->Record.GetVolume().GetDiskId());

        // A new mapping points to the tablet of Disk-B, both tablets are alive.
        clearBaseDisk();
        mapBaseDisk(tabletIdB);
        UNIT_ASSERT_VALUES_EQUAL(
            "Disk-B",
            service.StatVolume("Disk-1")->Record.GetVolume().GetDiskId());

        // A request waits for the describe when the mapping is cleared: it is
        // rejected.
        clearBaseDisk();
        mapBaseDisk(tabletIdA);
        holdDescribeResponse = true;
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest("Disk-1"));
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return !!describeResponse;
                }},
            TDuration::Seconds(5)));
        clearBaseDisk();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service.RecvStatVolumeResponse()->GetStatus());

        // The late describe response does not bring the old tablet back.
        mapBaseDisk(tabletIdB);
        runtime.Send(describeResponse.release(), nodeIdx);
        UNIT_ASSERT_VALUES_EQUAL(
            "Disk-B",
            service.StatVolume("Disk-1")->Record.GetVolume().GetDiskId());
    }

    Y_UNIT_TEST(ShouldMatchExactDiskIdWithEscapedPath)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("Disk@1");
        service.WaitForVolume("Disk@1");

        // The path name of the volume is escaped, the disk id is not.
        auto request = service.CreateStatVolumeRequest("Disk@1");
        request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
        auto statResponse = service.RecvStatVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            "Disk@1",
            statResponse->Record.GetVolume().GetDiskId());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
