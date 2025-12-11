#include "service_ut.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

using EChangeBindingOp =
    TEvService::TEvChangeVolumeBindingRequest::EChangeBindingOp;

////////////////////////////////////////////////////////////////////////////////

void PushVolume(TServiceClient& service, const TString& diskId)
{
    service.CreateVolume(diskId);
    service.MountVolume(diskId);

    using TRequest = TEvService::TEvChangeVolumeBindingRequest;
    using TResponse = TEvService::TEvChangeVolumeBindingResponse;

    {
        auto request = std::make_unique<TRequest>(
            diskId,
            EChangeBindingOp::RELEASE_TO_HIVE,
            NProto::EPreemptionSource::SOURCE_MANUAL);
        service.SendRequest(MakeStorageServiceId(), std::move(request));
        auto response = service.RecvResponse<TResponse>();
    }

    service.WaitForVolume(diskId);

    TDispatchOptions options;
    options.FinalEvents.emplace_back(
        TEvServicePrivate::EvSyncManuallyPreemptedVolumesComplete,
        1);
}

void PushVolumeNoSync(
    TTestActorRuntime& runtime,
    TServiceClient& service,
    const TString& diskId)
{
    service.CreateVolume(diskId);
    service.MountVolume(diskId);

    using TRequest = TEvService::TEvChangeVolumeBindingRequest;
    using TResponse = TEvService::TEvChangeVolumeBindingResponse;

    bool syncSeen = false;
    runtime.SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvServicePrivate::
                    EvSyncManuallyPreemptedVolumesComplete: {
                    syncSeen = true;
                    break;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

    {
        auto request = std::make_unique<TRequest>(
            diskId,
            EChangeBindingOp::RELEASE_TO_HIVE,
            NProto::EPreemptionSource::SOURCE_MANUAL);
        service.SendRequest(MakeStorageServiceId(), std::move(request));
        auto response = service.RecvResponse<TResponse>();
    }

    service.WaitForVolume(diskId);
    UNIT_ASSERT_VALUES_EQUAL(false, syncSeen);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TManuallyPreemptedVolumesTest)
{
    Y_UNIT_TEST(ShouldTriggerLivenessCheckAtStartup)
    {
        const auto livenessCheckPeriod = TDuration::Seconds(5);
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");
        config.SetManuallyPreemptedVolumeLivenessCheckPeriod(
            livenessCheckPeriod.MilliSeconds());

        auto& runtime = env.GetRuntime();
        bool responseSeen = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvRunVolumesLivenessCheckResponse: {
                        responseSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        SetupTestEnv(env, config);

        runtime.AdvanceCurrentTime(livenessCheckPeriod);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_C(responseSeen, "Response was not been sent");
    }

    Y_UNIT_TEST(ShouldTriggerLivenessCheckAtAfterDelay)
    {
        const auto livenessCheckPeriod = TDuration::Seconds(5);
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");
        config.SetManuallyPreemptedVolumeLivenessCheckPeriod(
            livenessCheckPeriod.MilliSeconds());

        auto& runtime = env.GetRuntime();
        bool responseSeen = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvRunVolumesLivenessCheckResponse: {
                        responseSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        SetupTestEnv(env, config);

        runtime.AdvanceCurrentTime(livenessCheckPeriod);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_C(responseSeen, "Response was not been sent");

        responseSeen = false;
        runtime.AdvanceCurrentTime(livenessCheckPeriod);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_C(responseSeen, "Second Response was not been sent");
    }

    Y_UNIT_TEST(ShouldQueryPreemptedVolumes)
    {
        const auto livenessCheckPeriod = TDuration::Seconds(5);
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");
        config.SetManuallyPreemptedVolumeLivenessCheckPeriod(
            livenessCheckPeriod.MilliSeconds());

        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        service.MountVolume();

        auto& runtime = env.GetRuntime();
        bool syncSeen = false;
        TAutoPtr<IEventHandle> syncComplete;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::
                        EvSyncManuallyPreemptedVolumesComplete: {
                        syncSeen = true;
                        syncComplete = event;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        using TRequest = TEvService::TEvChangeVolumeBindingRequest;
        using TResponse = TEvService::TEvChangeVolumeBindingResponse;

        {
            auto request = std::make_unique<TRequest>(
                DefaultDiskId,
                EChangeBindingOp::RELEASE_TO_HIVE,
                NProto::EPreemptionSource::SOURCE_MANUAL);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TResponse>();
        }

        service.WaitForVolume(DefaultDiskId);

        bool responseSeen = false;
        bool describeSeen = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        describeSeen = true;
                        break;
                    }
                    case TEvService::EvRunVolumesLivenessCheckResponse: {
                        responseSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        runtime.AdvanceCurrentTime(livenessCheckPeriod);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_C(describeSeen, "DescribeVolume was not sent");
        UNIT_ASSERT_C(responseSeen, "Response was not been sent");
    }

    Y_UNIT_TEST(ShouldSaveManuallyPreemptedVolumesToFile)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");

        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        PushVolume(service, DefaultDiskId);
    }

    Y_UNIT_TEST(ShouldNotStartSyncIfAnotherIsInProgress)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");

        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        service.MountVolume();

        auto& runtime = env.GetRuntime();
        bool syncSeen = false;
        TAutoPtr<IEventHandle> syncComplete;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::
                        EvSyncManuallyPreemptedVolumesComplete: {
                        syncSeen = true;
                        syncComplete = event;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        using TRequest = TEvService::TEvChangeVolumeBindingRequest;
        using TResponse = TEvService::TEvChangeVolumeBindingResponse;

        {
            auto request = std::make_unique<TRequest>(
                DefaultDiskId,
                EChangeBindingOp::RELEASE_TO_HIVE,
                NProto::EPreemptionSource::SOURCE_MANUAL);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TResponse>();
        }

        service.WaitForVolume(DefaultDiskId);

        UNIT_ASSERT_VALUES_EQUAL(true, syncSeen);
        UNIT_ASSERT(syncComplete);

        syncSeen = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::
                        EvSyncManuallyPreemptedVolumesComplete: {
                        syncSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        {
            auto request = std::make_unique<TRequest>(
                DefaultDiskId,
                EChangeBindingOp::ACQUIRE_FROM_HIVE,
                NProto::EPreemptionSource::SOURCE_MANUAL);
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvResponse<TResponse>();
        }

        service.WaitForVolume(DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL(false, syncSeen);

        runtime.Send(syncComplete.Release(), nodeIdx);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvServicePrivate::EvSyncManuallyPreemptedVolumesComplete,
            1);
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(ShouldTrackRequestsToSS)
    {
        const auto livenessCheckPeriod = TDuration::Seconds(5);
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");
        config.SetManuallyPreemptedVolumeLivenessCheckPeriod(
            livenessCheckPeriod.MilliSeconds());

        ui32 nodeIdx = SetupTestEnv(env, config);
        auto& runtime = env.GetRuntime();
        TServiceClient service(env.GetRuntime(), nodeIdx);

        PushVolume(service, "volume1");
        PushVolume(service, "volume2");

        TVector<TAutoPtr<IEventHandle>> describeResponses;
        TVector<ui64> cookies;
        THashMap<ui64, TString> cookieToDisk;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        const auto* msg =
                            event->Get<TEvSSProxy::TEvDescribeVolumeRequest>();
                        UNIT_ASSERT_C(
                            cookieToDisk.find(event->Cookie) ==
                                cookieToDisk.end(),
                            "Cookie already exists");
                        cookieToDisk[event->Cookie] = msg->DiskId;
                        break;
                    }
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        describeResponses.emplace_back(event.Release());

                        if (describeResponses.size() == 2) {
                            cookies.push_back(
                                describeResponses[1].Get()->Cookie);
                            runtime.Send(
                                describeResponses[1].Release(),
                                nodeIdx);
                            cookies.push_back(
                                describeResponses[0].Get()->Cookie);
                            runtime.Send(
                                describeResponses[0].Release(),
                                nodeIdx);
                        }
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    case TEvService::EvRunVolumesLivenessCheckResponse: {
                        using TResponse =
                            TEvService::TEvRunVolumesLivenessCheckResponse;
                        const auto* msg = event->Get<TResponse>();
                        UNIT_ASSERT_VALUES_EQUAL(msg->LiveVolumes.size(), 2);
                        UNIT_ASSERT_VALUES_EQUAL(
                            msg->LiveVolumes[0],
                            cookieToDisk[cookies[0]]);
                        UNIT_ASSERT_VALUES_EQUAL(
                            msg->LiveVolumes[1],
                            cookieToDisk[cookies[1]]);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvService::EvRunVolumesLivenessCheckResponse,
            1);
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(ShouldNotRunStoreAndLivenessCheckIfDisabled)
    {
        const auto livenessCheckPeriod = TDuration::Seconds(1);
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");
        config.SetManuallyPreemptedVolumeLivenessCheckPeriod(
            livenessCheckPeriod.MilliSeconds());
        config.SetDisableManuallyPreemptedVolumesTracking(true);

        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        PushVolumeNoSync(runtime, service, "volume1");
        PushVolumeNoSync(runtime, service, "volume2");

        bool describeSeen = false;
        ui32 checkReplyCnt = 0;
        ui32 checkRequestCnt = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        describeSeen = true;
                        break;
                    }
                    case TEvService::EvRunVolumesLivenessCheckResponse: {
                        ++checkReplyCnt;
                        break;
                    }
                    case TEvService::EvRunVolumesLivenessCheckRequest: {
                        ++checkRequestCnt;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        runtime.DispatchEvents({}, 2 * livenessCheckPeriod);

        UNIT_ASSERT_C(!describeSeen, "Unexpected DescribeVolumes was sent");
        UNIT_ASSERT_VALUES_UNEQUAL(0, checkReplyCnt);
        UNIT_ASSERT_VALUES_UNEQUAL(0, checkRequestCnt);
    }

    Y_UNIT_TEST(ShouldProperlyMountVolumesAtStartup)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");
        config.SetDisableManuallyPreemptedVolumesTracking(true);

        TVector<std::pair<TString, TInstant>> volumes = {{"volume1", {}}};

        ui32 nodeIdx = SetupTestEnvWithManuallyPreemptedVolumes(
            env,
            std::move(config),
            std::move(volumes));

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("volume1");

        ui32 startVolumeRequestCnt = 0;
        ui32 startVolumeresponseCnt = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvStartVolumeRequest: {
                        ++startVolumeRequestCnt;
                        break;
                    }
                    case TEvServicePrivate::EvStartVolumeResponse: {
                        ++startVolumeresponseCnt;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.MountVolume("volume1");
        service.WaitForVolume("volume1");
        service.MountVolume("volume1");

        UNIT_ASSERT_VALUES_EQUAL(0, startVolumeRequestCnt);
        UNIT_ASSERT_VALUES_EQUAL(0, startVolumeresponseCnt);
    }

    Y_UNIT_TEST(ShouldProperlyHandleRebindForManuallyPreemptedVolume)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetManuallyPreemptedVolumesFile("somefile");
        config.SetDisableManuallyPreemptedVolumesTracking(false);

        auto manuallyPreemptedVolumes = CreateManuallyPreemptedVolumes();

        ui32 nodeIdx = SetupTestEnvWithManuallyPreemptedVolumes(
            env,
            std::move(config),
            manuallyPreemptedVolumes);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvService::EvRunVolumesLivenessCheckResponse,
                1);
            runtime.DispatchEvents(options);
        }

        service.CreateVolume("volume1");
        service.CreateVolume("volume2");

        service.MountVolume("volume1");
        service.MountVolume("volume2");

        {
            NPrivateProto::TRebindVolumesRequest request;
            request.SetBinding(2);   // REMOTE

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("rebindvolumes", buf);

            {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(
                    TEvServicePrivate::EvUpdateManuallyPreemptedVolume,
                    2);
                runtime.DispatchEvents(options);
            }

            service.RecvExecuteActionResponse();

            UNIT_ASSERT_VALUES_EQUAL(manuallyPreemptedVolumes->GetSize(), 0);
        }

        {
            NPrivateProto::TRebindVolumesRequest request;
            request.SetBinding(1);   // LOCAL

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("rebindvolumes", buf);

            {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(
                    TEvServicePrivate::EvUpdateManuallyPreemptedVolume,
                    2);
                runtime.DispatchEvents(options);
            }

            service.RecvExecuteActionResponse();

            UNIT_ASSERT_VALUES_EQUAL(manuallyPreemptedVolumes->GetSize(), 0);
        }

        {
            service.MountVolume("volume2");

            using TRequest = TEvService::TEvChangeVolumeBindingRequest;
            using TResponse = TEvService::TEvChangeVolumeBindingResponse;

            {
                auto request = std::make_unique<TRequest>(
                    "volume2",
                    EChangeBindingOp::RELEASE_TO_HIVE,
                    NProto::EPreemptionSource::SOURCE_MANUAL);
                service.SendRequest(MakeStorageServiceId(), std::move(request));
                auto response = service.RecvResponse<TResponse>();
            }

            service.WaitForVolume("volume2");

            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvServicePrivate::EvSyncManuallyPreemptedVolumesComplete,
                1);
        }

        UNIT_ASSERT_VALUES_EQUAL(manuallyPreemptedVolumes->GetSize(), 1);
    }
}
}   // namespace NCloud::NBlockStore::NStorage
