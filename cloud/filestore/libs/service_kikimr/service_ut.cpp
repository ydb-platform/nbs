#include "service.h"

#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/service_kikimr/ut/kikimr_test_env.h>
#include <cloud/filestore/libs/storage/api/service.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

struct TTestServiceActor final
    : public TActor<TTestServiceActor>
{
    TTestServiceActor()
        : TActor(&TThis::StateWork)
    {}

#define FILESTORE_IMPLEMENT_METHOD(name, ns)                                   \
    using T##name##Handler = std::function<                                    \
        std::unique_ptr<ns::TEv##name##Response>(                              \
            const ns::TEv##name##Request::TPtr& ev)                            \
        >;                                                                     \
                                                                               \
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
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_REMOTE_SERVICE(FILESTORE_IMPLEMENT_METHOD, TEvService)

#undef FILESTORE_IMPLEMENT_METHOD

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            FILESTORE_REMOTE_SERVICE(FILESTORE_HANDLE_REQUEST, TEvService)
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKikimrFileStore)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->CreateFileStoreHandler =
            [] (const TEvService::TEvCreateFileStoreRequest::TPtr& ev) {
                Y_UNUSED(ev);
                return std::make_unique<TEvService::TEvCreateFileStoreResponse>();
            };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service = CreateKikimrFileStore(actorSystem);
        service->Start();

        auto context = MakeIntrusive<TCallContext>();
        auto request = std::make_shared<NProto::TCreateFileStoreRequest>();

        auto future = service->CreateFileStore(
            std::move(context),
            std::move(request));

        actorSystem->DispatchEvents(WaitTimeout);

        const auto& response = future.GetValue(WaitTimeout);
        UNIT_ASSERT(!HasError(response));

        service->Stop();
    }

    Y_UNIT_TEST(ShouldHandleFsyncRequestsOutsideActorSystem)
    {
        auto serviceActor = std::make_unique<TTestServiceActor>();

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service = CreateKikimrFileStore(actorSystem);
        service->Start();

        {
            auto context = MakeIntrusive<TCallContext>();
            auto request = std::make_shared<NProto::TFsyncRequest>();

            auto future = service->Fsync(
                std::move(context),
                std::move(request));

            actorSystem->DispatchEvents(WaitTimeout);

            const auto& response = future.GetValue(WaitTimeout);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        {
            auto context = MakeIntrusive<TCallContext>();
            auto request = std::make_shared<NProto::TFsyncDirRequest>();

            auto future = service->FsyncDir(
                std::move(context),
                std::move(request));

            actorSystem->DispatchEvents(WaitTimeout);

            const auto& response = future.GetValue(WaitTimeout);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        service->Stop();
    }

    Y_UNIT_TEST(ShouldGetSessionEventsStream)
    {
        TActorId eventHandler;

        auto serviceActor = std::make_unique<TTestServiceActor>();
        serviceActor->GetSessionEventsHandler =
            [&] (const TEvService::TEvGetSessionEventsRequest::TPtr& ev) {
                Y_ABORT_UNLESS(ev->Cookie == TEvService::StreamCookie);
                eventHandler = ev->Sender;
                return std::make_unique<TEvService::TEvGetSessionEventsResponse>();
            };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestService(std::move(serviceActor));

        auto service = CreateKikimrFileStore(actorSystem);
        service->Start();

        auto context = MakeIntrusive<TCallContext>();
        auto request = std::make_shared<NProto::TGetSessionEventsRequest>();

        auto responseHandler = std::make_shared<TResponseHandler>();

        service->GetSessionEventsStream(
            std::move(context),
            std::move(request),
            responseHandler);

        actorSystem->DispatchEvents(WaitTimeout);
        Y_ABORT_UNLESS(eventHandler);

        auto response = std::make_unique<TEvService::TEvGetSessionEventsResponse>();
        auto* event = response->Record.AddEvents();
        event->SetSeqNo(1);

        actorSystem->Send(eventHandler, std::move(response));

        actorSystem->DispatchEvents(WaitTimeout);
        UNIT_ASSERT(responseHandler->GotResponse());

        service->Stop();
    }
}

}   // namespace NCloud::NFileStore
