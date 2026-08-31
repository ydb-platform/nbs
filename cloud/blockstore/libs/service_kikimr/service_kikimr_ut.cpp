#include "service_kikimr.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service_kikimr/ut/kikimr_test_env.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NBlockStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TKikimrServiceConfig MakeConfig(ui32 permanentActorCount)
{
    NProto::TKikimrServiceConfig config;
    config.SetPermanentActorCount(permanentActorCount);
    return config;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestServiceActor final: public TActor<TTestServiceActor>
{
    TTestServiceActor()
        : TActor(&TThis::StateWork)
    {}

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ns)                                  \
    using T##name##ResponsePtr = std::unique_ptr<ns::TEv##name##Response>;     \
    using T##name##Handler = std::function<T##name##ResponsePtr(               \
        const ns::TEv##name##Request::TPtr& ev)>;                              \
    T##name##Handler name##Handler;                                            \
                                                                               \
    void Handle##name(                                                         \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        if (auto response = name##Handler(ev)) {                               \
            NCloud::Reply(ctx, *ev, std::move(response));                      \
        }                                                                      \
    }                                                                          \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD, TEvService)
    BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_IMPLEMENT_METHOD, TEvService)

#undef BLOCKSTORE_IMPLEMENT_METHOD

    STFUNC(StateWork)
    {
        if (!HandleRequests(ev)) {
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
        }
    }

    bool HandleRequests(STFUNC_SIG)
    {
        switch (ev->GetTypeRewrite()) {
            BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_HANDLE_REQUEST, TEvService)
            BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvService)

            default:
                return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKikimrServiceTest)
{
    void DoTestShouldHandleRequests(ui32 permanentActorCount)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->PingHandler =
            [](const TEvService::TEvPingRequest::TPtr& ev)
        {
            Y_UNUSED(ev);
            return std::make_unique<TEvService::TEvPingResponse>();
        };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service =
            CreateKikimrService(actorSystem, MakeConfig(permanentActorCount));
        service->Start();

        auto request = std::make_shared<NProto::TPingRequest>();
        request->MutableHeaders()->SetRequestTimeout(100);   // ms

        auto future =
            service->Ping(MakeIntrusive<TCallContext>(), std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(5));

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        service->Stop();
        actorSystem->Stop();
    }

    Y_UNIT_TEST(ShouldHandleRequests)
    {
        DoTestShouldHandleRequests(/*permanentActorCount=*/ 0);
    }

    Y_UNIT_TEST(ShouldHandleRequestsWithPermanentActors)
    {
        DoTestShouldHandleRequests(/*permanentActorCount=*/ 4);
    }

    Y_UNIT_TEST(ShouldReusePermanentActors)
    {
        constexpr ui32 PermanentActorCount = 4;
        constexpr ui32 RequestCount = 16;

        TSet<TActorId> requestActorIds;
        TVector<IEventHandlePtr> requests;
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->PingHandler =
            [&requestActorIds, &requests](
                const TEvService::TEvPingRequest::TPtr& ev)
        {
            requestActorIds.insert(ev->Sender);
            requests.emplace_back(ev.Release());
            return nullptr;
        };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));
        const ui64 registrationsBeforeStart =
            actorSystem->GetRegistrationCount();

        auto service =
            CreateKikimrService(actorSystem, MakeConfig(PermanentActorCount));
        service->Start();

        const ui64 registrationsAfterStart =
            actorSystem->GetRegistrationCount();
        UNIT_ASSERT_VALUES_EQUAL(
            registrationsBeforeStart + PermanentActorCount,
            registrationsAfterStart);

        TVector<TFuture<NProto::TPingResponse>> futures;
        for (ui32 i = 0; i < RequestCount; ++i) {
            auto request = std::make_shared<NProto::TPingRequest>();
            request->MutableHeaders()->SetRequestId(i + 1);
            futures.push_back(service->Ping(
                MakeIntrusive<TCallContext>(),
                std::move(request)));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            registrationsAfterStart,
            actorSystem->GetRegistrationCount());

        actorSystem->DispatchEvents(TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(RequestCount, requests.size());
        for (const auto& future: futures) {
            UNIT_ASSERT(!future.HasValue());
        }

        for (auto it = requests.rbegin(); it != requests.rend(); ++it) {
            auto* request = (*it)->Get<TEvService::TEvPingRequest>();
            auto response = std::make_unique<TEvService::TEvPingResponse>();
            response->Record.SetLastRequestCount(
                request->Record.GetHeaders().GetRequestId());
            actorSystem->Send(std::make_unique<IEventHandle>(
                (*it)->Sender,
                TActorId(),
                response.release(),
                /*flags=*/ 0,
                (*it)->Cookie));
        }

        actorSystem->DispatchEvents(TDuration::Seconds(5));

        for (ui32 i = 0; i < RequestCount; ++i) {
            const auto& response =
                futures[i].GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_VALUES_EQUAL(i + 1, response.GetLastRequestCount());
        }
        UNIT_ASSERT_VALUES_EQUAL(PermanentActorCount, requestActorIds.size());
        UNIT_ASSERT_VALUES_EQUAL(
            registrationsAfterStart,
            actorSystem->GetRegistrationCount());

        service->Stop();
        actorSystem->Stop();
    }

    void DoTestShouldHandleWriteAndZeroRequestTimeout(ui32 permanentActorCount)
    {
        IEventHandlePtr writeBlocksEvent;
        IEventHandlePtr writeBlocksLocalEvent;
        IEventHandlePtr zeroBlocksEvent;

        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->WriteBlocksHandler =
            [&writeBlocksEvent](
                const TEvService::TEvWriteBlocksRequest::TPtr& ev)
        {
            writeBlocksEvent.reset(ev.Release());
            return nullptr;
        };
        serviceActor->WriteBlocksLocalHandler =
            [&writeBlocksLocalEvent](
                const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev)
        {
            writeBlocksLocalEvent.reset(ev.Release());
            return nullptr;
        };
        serviceActor->ZeroBlocksHandler =
            [&zeroBlocksEvent](const TEvService::TEvZeroBlocksRequest::TPtr& ev)
        {
            zeroBlocksEvent.reset(ev.Release());
            return nullptr;
        };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto counter = counters->GetCounter(
            "AppCriticalEvents/ServiceProxyWakeupTimerHit",
            true);

        auto service =
            CreateKikimrService(actorSystem, MakeConfig(permanentActorCount));
        service->Start();

        {
            auto request = std::make_shared<NProto::TWriteBlocksRequest>();
            auto& headers = *request->MutableHeaders();
            headers.SetRequestTimeout(100);   // ms

            auto future = service->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());

            actorSystem->Send(std::make_unique<IEventHandle>(
                writeBlocksEvent->Sender,
                TActorId(),
                new TEvService::TEvWriteBlocksResponse(),
                /*flags=*/ 0,
                writeBlocksEvent->Cookie));
            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!HasError(future.GetValue(TDuration::Seconds(5))));
        }

        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            auto& headers = *request->MutableHeaders();
            headers.SetRequestTimeout(100);   // ms

            auto future = service->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(2, counter->Val());

            actorSystem->Send(std::make_unique<IEventHandle>(
                writeBlocksLocalEvent->Sender,
                TActorId(),
                new TEvService::TEvWriteBlocksLocalResponse(),
                /*flags=*/ 0,
                writeBlocksLocalEvent->Cookie));
            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!HasError(future.GetValue(TDuration::Seconds(5))));
        }

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            auto& headers = *request->MutableHeaders();
            headers.SetRequestTimeout(100);   // ms

            auto future = service->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(3, counter->Val());

            actorSystem->Send(std::make_unique<IEventHandle>(
                zeroBlocksEvent->Sender,
                TActorId(),
                new TEvService::TEvZeroBlocksResponse(),
                /*flags=*/ 0,
                zeroBlocksEvent->Cookie));
            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!HasError(future.GetValue(TDuration::Seconds(5))));
        }

        service->Stop();
        actorSystem->Stop();
    }

    Y_UNIT_TEST(ShouldHandleWriteAndZeroRequestTimeout)
    {
        DoTestShouldHandleWriteAndZeroRequestTimeout(
            /*permanentActorCount=*/ 0);
    }

    Y_UNIT_TEST(ShouldHandleWriteAndZeroRequestTimeoutWithPermanentActors)
    {
        DoTestShouldHandleWriteAndZeroRequestTimeout(
            /*permanentActorCount=*/ 4);
    }

    void DoTestShouldHandleOtherRequestTimeout(ui32 permanentActorCount)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->PingHandler =
            [](const TEvService::TEvPingRequest::TPtr& ev)
        {
            Y_UNUSED(ev);
            return nullptr;
        };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service =
            CreateKikimrService(actorSystem, MakeConfig(permanentActorCount));
        service->Start();

        auto request = std::make_shared<NProto::TPingRequest>();
        auto& headers = *request->MutableHeaders();
        headers.SetRequestTimeout(100);   // ms

        auto future =
            service->Ping(MakeIntrusive<TCallContext>(), std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(5));

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_TIMEOUT);

        service->Stop();
        actorSystem->Stop();
    }

    Y_UNIT_TEST(ShouldHandleOtherRequestTimeout)
    {
        DoTestShouldHandleOtherRequestTimeout(/*permanentActorCount=*/ 0);
    }

    Y_UNIT_TEST(ShouldHandleOtherRequestTimeoutWithPermanentActors)
    {
        DoTestShouldHandleOtherRequestTimeout(/*permanentActorCount=*/ 4);
    }

    void DoTestShouldCompleteRequestWhenShuttingDown(ui32 permanentActorCount)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->PingHandler =
            [](const TEvService::TEvPingRequest::TPtr& ev)
        {
            Y_UNUSED(ev);
            return nullptr;
        };
        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service =
            CreateKikimrService(actorSystem, MakeConfig(permanentActorCount));
        service->Start();

        auto request = std::make_shared<NProto::TPingRequest>();

        auto future =
            service->Ping(MakeIntrusive<TCallContext>(), std::move(request));

        TFuture<NProto::TPingResponse> reentrantFuture;
        if (permanentActorCount) {
            future.Subscribe([&](const auto&) {
                reentrantFuture = service->Ping(
                    MakeIntrusive<TCallContext>(),
                    std::make_shared<NProto::TPingRequest>());
            });
        }

        actorSystem->Stop();

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_REJECTED);

        if (permanentActorCount) {
            UNIT_ASSERT(reentrantFuture.Initialized());
            const auto& reentrantResponse =
                reentrantFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_EQUAL(
                reentrantResponse.GetError().GetCode(),
                E_REJECTED);
        }
    }

    Y_UNIT_TEST(ShouldCompleteRequestWhenShuttingDown)
    {
        DoTestShouldCompleteRequestWhenShuttingDown(
            /*permanentActorCount=*/ 0);
    }

    Y_UNIT_TEST(ShouldCompleteRequestWhenShuttingDownWithPermanentActors)
    {
        DoTestShouldCompleteRequestWhenShuttingDown(
            /*permanentActorCount=*/ 4);
    }
}

}   // namespace NCloud::NBlockStore::NServer
