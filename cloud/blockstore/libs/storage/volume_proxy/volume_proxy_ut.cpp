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
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
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

// Drops the volume info the proxy holds for the disk: a request is held back
// on the way to the volume and fails on an injected disconnect notice.
void DropVolumeInfo(
    TTestActorRuntime& runtime,
    TServiceClient& service,
    ui32 nodeIdx,
    const TString& diskId = DefaultDiskId)
{
    ui64 tabletId = 0;
    ui64 cookie = 0;
    {
        auto prevFilter = runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (cookie ||
                    event->GetTypeRewrite() != TEvTabletProxy::EvForward)
                {
                    return false;
                }

                const auto* msg =
                    event->template Get<TEvTabletProxy::TEvForward>();
                if (msg->Request->GetTypeRewrite() ==
                    TEvService::EvStatVolumeRequest)
                {
                    tabletId = msg->TabletId;
                    cookie = msg->Request->Cookie;
                    return true;
                }
                return false;
            });
        Y_DEFER {
            runtime.SetEventFilter(std::move(prevFilter));
        };

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest(diskId));
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return cookie != 0;
                }},
            TDuration::Seconds(5)));
    }

    runtime.Send(
        new IEventHandle(
            MakeVolumeProxyServiceId(),
            service.GetSender(),
            new TEvTabletProxy::TEvDisconnected(
                MakeError(E_REJECTED),
                tabletId),
            0,
            cookie),
        nodeIdx);
    UNIT_ASSERT_VALUES_EQUAL(
        E_REJECTED,
        service.RecvStatVolumeResponse()->GetStatus());
}

// Two requests are in flight when the tablet restarts. The first disconnect
// notice answers its request and drops the volume info, the second one is
// returned to be delivered late.
IEventHandlePtr HoldSecondDisconnect(
    TTestActorRuntime& runtime,
    TServiceClient& service,
    ui32 nodeIdx)
{
    const TActorId volumeProxyId =
        runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
    ui64 tabletId = 0;
    ui32 droppedRequests = 0;
    ui32 disconnectCount = 0;
    IEventHandlePtr heldDisconnect;
    auto prevFilter = runtime.SetEventFilter(
        [&](auto&, auto& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvTabletProxy::EvForward: {
                    tabletId =
                        event->template Get<TEvTabletProxy::TEvForward>()
                            ->TabletId;
                    break;
                }
                case TEvService::EvStatVolumeRequest: {
                    if (droppedRequests < 2 &&
                        event->GetRecipientRewrite() != volumeProxyId)
                    {
                        ++droppedRequests;
                        return true;
                    }
                    break;
                }
                case TEvTabletProxy::EvDisconnected: {
                    if (++disconnectCount == 2) {
                        heldDisconnect.reset(event.Release());
                        return true;
                    }
                    break;
                }
            }
            return false;
        });
    Y_DEFER {
        runtime.SetEventFilter(std::move(prevFilter));
    };

    for (ui32 i = 0; i != 2; ++i) {
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
    }
    UNIT_ASSERT(runtime.DispatchEvents(
        TDispatchOptions{
            .CustomFinalCondition = [&]
            {
                return droppedRequests == 2;
            }},
        TDuration::Seconds(5)));

    RebootTablet(runtime, tabletId, service.GetSender(), nodeIdx);
    UNIT_ASSERT_VALUES_EQUAL(
        E_REJECTED,
        service.RecvStatVolumeResponse()->GetStatus());
    UNIT_ASSERT(heldDisconnect);

    return heldDisconnect;
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeProxyTest)
{
    Y_UNIT_TEST(ShouldRouteExternalVolumesByTabletId)
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

        // The tablet id of the request decides, not the disk id.
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

        ui32 describeCount = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        ++describeCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // A data plane request resolves a mapped base disk without a
        // describe, a disk with no mapping with one.
        auto describeBlocks = [&]
        {
            DropVolumeInfo(runtime, service, nodeIdx);
            describeCount = 0;

            auto request =
                std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();
            request->Record.SetDiskId(DefaultDiskId);
            request->Record.SetStartIndex(0);
            request->Record.SetBlocksCount(1);
            service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
            service.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            return describeCount;
        };

        UNIT_ASSERT_VALUES_EQUAL(0, describeBlocks());

        // The mapping is counted: the first clear leaves it in place.
        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                DefaultDiskId));
        UNIT_ASSERT_VALUES_EQUAL(0, describeBlocks());

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            std::make_unique<TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                DefaultDiskId));
        UNIT_ASSERT_VALUES_EQUAL(1, describeBlocks());
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
            }
            return false;
        });

        service1.CreateVolume();
        service1.WaitForVolume();

        auto nodeIdx2 = SetupTestEnv(env);
        TServiceClient service2(runtime, nodeIdx2);

        service2.StatVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx2);
        ui32 describeCount = 0;
        bool requestDropped = false;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeRequest &&
                    event->Sender == volumeProxyId)
                {
                    ++describeCount;
                }
                if (!requestDropped &&
                    event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() != volumeProxyId)
                {
                    requestDropped = true;
                    return true;
                }
                return false;
            });

        service2.SendRequest(
            MakeVolumeProxyServiceId(),
            service2.CreateStatVolumeRequest());
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return requestDropped;
                }},
            TDuration::Seconds(5)));

        RebootTablet(runtime, volumeTabletId, service1.GetSender(), nodeIdx1);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service2.RecvStatVolumeResponse()->GetStatus());

        service2.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);
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
                        if (failConnects &&
                            msg->TabletId == volumeTabletId &&
                            msg->Status == NKikimrProto::OK)
                        {
                            runtime.Send(
                                new IEventHandle(
                                    event->Recipient,
                                    event->Sender,
                                    new TEvTabletPipe::TEvClientConnected(
                                        msg->TabletId,
                                        NKikimrProto::ERROR,
                                        msg->ClientId,
                                        msg->ServerId,
                                        msg->Leader,
                                        msg->Dead,
                                        msg->Generation),
                                    0,
                                    event->Cookie),
                                nodeIdx);
                            return true;
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

    Y_UNIT_TEST(ShouldRemoveOldRealDiskIdMappingsOnErase)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        DropVolumeInfo(runtime, service, nodeIdx);

        const TString oldRealDiskId = "old-real-disk";
        const TString newRealDiskId = "new-real-disk";
        ui32 describeCount = 0;
        ui64 tabletId = 0;
        IEventHandlePtr heldDescribeResponse;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeResponse &&
                    event->GetRecipientRewrite() == volumeProxyId)
                {
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

        DropVolumeInfo(runtime, service, nodeIdx);

        resolveWithRealDiskId(newRealDiskId);
        UNIT_ASSERT_VALUES_EQUAL(2, describeCount);

        auto request = service.CreateStatVolumeRequest(oldRealDiskId);
        request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        service.SendRequest(MakeVolumeProxyServiceId(), std::move(request));
        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(3, describeCount);
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldCancelRequestsForAllVolumeInfosSharingTablet)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        DropVolumeInfo(runtime, service, nodeIdx);

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

        TVector<IEventHandlePtr> describeResponses;
        ui64 volumeTabletId = 0;
        TActorId volumeActorId;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvService::EvStatVolumeRequest &&
                    event->GetRecipientRewrite() != volumeProxyId)
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

        sendStatRequest(false, 1);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return describeResponses.size() == 1;
                }},
            TDuration::Seconds(5)));

        sendStatRequest(true, 2);
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
            "first volume info failed");
        runtime.Send(describeResponses[1].release(), nodeIdx);
        UNIT_ASSERT_C(
            SUCCEEDED(service.RecvStatVolumeResponse()->GetStatus()),
            "second volume info failed");
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

        sendStatRequest(false, 3);
        sendStatRequest(true, 4);
        UNIT_ASSERT(runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                {
                    return droppedRequests == 2;
                }},
            TDuration::Seconds(5)));

        RebootTablet(runtime, volumeTabletId, service.GetSender(), nodeIdx);
        TVector<ui64> cookies;
        for (int i = 0; i < 2; ++i) {
            auto response = runtime.GrabEdgeEvent<
                TEvService::TEvStatVolumeResponse>(
                service.GetSender(),
                TDuration::Seconds(5));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->Get()->GetStatus());
            cookies.push_back(response->Cookie);
        }
        Sort(cookies);
        UNIT_ASSERT_VALUES_EQUAL(3, cookies[0]);
        UNIT_ASSERT_VALUES_EQUAL(4, cookies[1]);
    }

    Y_UNIT_TEST(ShouldNotEraseNewVolumeInfoOnLateDisconnect)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto heldDisconnect = HoldSecondDisconnect(runtime, service, nodeIdx);

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

        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);

        runtime.Send(heldDisconnect.release(), nodeIdx);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service.RecvStatVolumeResponse()->GetStatus());

        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(1, describeCount);
    }

    Y_UNIT_TEST(ShouldAnswerOldRequestAfterFailedRedescribe)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto heldDisconnect = HoldSecondDisconnect(runtime, service, nodeIdx);

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeVolumeResponse &&
                    event->GetRecipientRewrite() == volumeProxyId)
                {
                    runtime.Send(
                        new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            new TEvSSProxy::TEvDescribeVolumeResponse(
                                MakeError(E_FAIL, "describe failed")),
                            0,
                            event->Cookie),
                        nodeIdx);
                    return true;
                }
                return false;
            });

        service.SendRequest(
            MakeVolumeProxyServiceId(),
            service.CreateStatVolumeRequest());
        UNIT_ASSERT_VALUES_EQUAL(
            E_FAIL,
            service.RecvStatVolumeResponse()->GetStatus());

        runtime.Send(heldDisconnect.release(), nodeIdx);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service.RecvStatVolumeResponse()->GetStatus());
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

    Y_UNIT_TEST(ShouldRecoverAfterIdleTabletRestart)
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
                }
                return false;
            });

        service.StatVolume();
        UNIT_ASSERT(tabletId);

        // Nothing is in flight: the tablet keeps its id, the next request
        // goes to it over a new pipe with no error and no describe.
        RebootTablet(runtime, tabletId, service.GetSender(), nodeIdx);
        service.StatVolume();
        UNIT_ASSERT_VALUES_EQUAL(0, describeCount);
    }

    Y_UNIT_TEST(ShouldForgetVolumeInfoOnDestroy)
    {
        TTestEnv env;
        const ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        ui64 tabletId = 0;
        runtime.SetEventFilter(
            [&](auto&, auto& event)
            {
                if (event->GetTypeRewrite() == TEvTabletProxy::EvForward) {
                    tabletId =
                        event->template Get<TEvTabletProxy::TEvForward>()
                            ->TabletId;
                }
                return false;
            });

        service.StatVolume();
        const ui64 oldTabletId = tabletId;
        UNIT_ASSERT(oldTabletId);

        // Destroy makes the volume proxy forget the volume: the next
        // requests resolve the disk id again instead of going to the
        // destroyed tablet.
        service.DestroyVolume();
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

    Y_UNIT_TEST(ShouldForgetVolumeInfoAtOnceWithZeroTimeout)
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

        for (ui32 i = 1; i != 4; ++i) {
            // let the check of the previous request run
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            service.StatVolume();
            UNIT_ASSERT_VALUES_EQUAL(i, describeCount);
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
        DropVolumeInfo(runtime, service, nodeIdx);

        bool describe = false;
        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        if (describe) {
                            break;
                        }
                        describe = true;
                        runtime.Send(
                            new IEventHandle(
                                event->Recipient,
                                event->Sender,
                                new TEvSSProxy::TEvDescribeVolumeResponse(
                                    MakeError(E_REJECTED, "SS is dead")),
                                0,
                                event->Cookie),
                            nodeIdx);
                        return true;
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

        const TActorId volumeProxyId =
            runtime.GetLocalServiceId(MakeVolumeProxyServiceId(), nodeIdx);
        ui64 describeCount = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        if (event->GetRecipientRewrite() == volumeProxyId) {
                            ++describeCount;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.CreateVolume("Disk-1-copy");
        service.WaitForVolume("Disk-1-copy");

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
            // forward to the old disk, since the existing volume info has
            // not expired.
            auto statResponse = service.StatVolume("Disk-1");
            UNIT_ASSERT_VALUES_EQUAL(
                "Disk-1-copy",
                statResponse->Record.GetVolume().GetDiskId());
        }

        // adjust time to drop the unused volume info
        runtime.AdvanceCurrentTime(TDuration::Minutes(2));
        // give a chance to internal messages to complete
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        {
            // A request that does not require an exact name match should be
            // forward to the new disk, since the existing volume info has
            // expired.
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
        DropVolumeInfo(runtime, service, nodeIdx);

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
