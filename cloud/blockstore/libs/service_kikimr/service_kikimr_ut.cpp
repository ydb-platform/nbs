#include "service_kikimr.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service_kikimr/ut/kikimr_test_env.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NBlockStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TKikimrServiceConfig DefaultConfig()
{
    // TODO
    return {};
}

////////////////////////////////////////////////////////////////////////////////

struct TTestServiceActor final
    : public TActor<TTestServiceActor>
{
    TTestServiceActor()
        : TActor(&TThis::StateWork)
    {}

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ns)                                  \
    using T##name##ResponsePtr = std::unique_ptr<ns::TEv##name##Response>;     \
    using T##name##Handler = std::function<                                    \
        T##name##ResponsePtr(const ns::TEv##name##Request::TPtr& ev)>;         \
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
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->PingHandler =
            [] (const TEvService::TEvPingRequest::TPtr& ev) {
                Y_UNUSED(ev);
                return std::make_unique<TEvService::TEvPingResponse>();
            };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service = CreateKikimrService(
            actorSystem,
            DefaultConfig());

        auto request = std::make_shared<NProto::TPingRequest>();

        auto future = service->Ping(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(5));

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));
    }

    Y_UNIT_TEST(ShouldHandleWriteAndZeroRequestTimeout)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->WriteBlocksHandler =
            [] (const TEvService::TEvWriteBlocksRequest::TPtr& ev) {
                Y_UNUSED(ev);
                return nullptr;
            };
        serviceActor->WriteBlocksLocalHandler =
            [] (const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev) {
                Y_UNUSED(ev);
                return nullptr;
            };
        serviceActor->ZeroBlocksHandler =
            [] (const TEvService::TEvZeroBlocksRequest::TPtr& ev) {
                Y_UNUSED(ev);
                return nullptr;
            };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        NMonitoring::TDynamicCountersPtr counters
            = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto counter = counters->GetCounter(
            "AppCriticalEvents/ServiceProxyWakeupTimerHit", true);

        auto service = CreateKikimrService(
            actorSystem,
            DefaultConfig());

        {
            auto request = std::make_shared<NProto::TWriteBlocksRequest>();
            auto& headers = *request->MutableHeaders();
            headers.SetRequestTimeout(100); // ms

            auto future = service->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        }

        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            auto& headers = *request->MutableHeaders();
            headers.SetRequestTimeout(100); // ms

            auto future = service->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(2, counter->Val());
        }

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            auto& headers = *request->MutableHeaders();
            headers.SetRequestTimeout(100); // ms

            auto future = service->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(5));

            UNIT_ASSERT(!future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(3, counter->Val());
        }
    }

    Y_UNIT_TEST(ShouldHandleOtherRequestTimeout)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->PingHandler =
            [] (const TEvService::TEvPingRequest::TPtr& ev) {
                Y_UNUSED(ev);
                return nullptr;
            };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service = CreateKikimrService(
            actorSystem,
            DefaultConfig());

        auto request = std::make_shared<NProto::TPingRequest>();
        auto& headers = *request->MutableHeaders();
        headers.SetRequestTimeout(100); // ms

        auto future = service->Ping(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(5));

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_TIMEOUT);
    }

    Y_UNIT_TEST(ShouldCompleteRequestWhenShuttingDown)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->PingHandler =
            [] (const TEvService::TEvPingRequest::TPtr& ev) {
                Y_UNUSED(ev);
                return nullptr;
            };


        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service = CreateKikimrService(
            actorSystem,
            DefaultConfig());

        auto request = std::make_shared<NProto::TPingRequest>();

        auto future = service->Ping(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        actorSystem->Stop();

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_REJECTED);
    }
}

}   // namespace NCloud::NBlockStore::NServer
