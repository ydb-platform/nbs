#include "auth_provider_kikimr.h"

#include <cloud/filestore/libs/service/auth_provider.h>
#include <cloud/filestore/libs/service/service_auth.h>
#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/service_kikimr/ut/kikimr_test_env.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/api/authorizer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestAuthorizerActor
    : public TActor<TTestAuthorizerActor>
{
public:
    std::function<std::unique_ptr<bool>(
        const TEvAuth::TEvAuthorizationRequest::TPtr&)> AuthorizeHandler;

public:
    TTestAuthorizerActor()
        : TActor(&TThis::StateWork)
    {}

private:
    STRICT_STFUNC(
        StateWork,
        HFunc(TEvAuth::TEvAuthorizationRequest, HandleAuthorize);
    )

    void HandleAuthorize(
        const TEvAuth::TEvAuthorizationRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_ABORT_UNLESS(AuthorizeHandler);
        auto response = AuthorizeHandler(ev);
        if (response) {
            bool allow = *response;

            NProto::TError error;
            if (!allow) {
                error.SetCode(E_UNAUTHORIZED);
            }

            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvAuth::TEvAuthorizationResponse>(std::move(error)));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKikimrAuthProviderTest)
{
    Y_UNIT_TEST(CheckPermissionForCreateFileSystem)
    {
        int volumeHandlerCount = 0;
        auto testService = std::make_shared<TFileStoreTest>();
        testService->CreateFileStoreHandler =
            [&] (TCallContextPtr callContext,
                std::shared_ptr<NProto::TCreateFileStoreRequest> request)
            {
                Y_UNUSED(callContext);
                Y_UNUSED(request);
                ++volumeHandlerCount;
                return MakeFuture<NProto::TCreateFileStoreResponse>();
            };

        const TString authToken = "TEST_AUTH_TOKEN";

        bool authorizeResult = false;
        int authorizeHandlerCount = 0;
        auto authorizerActor = std::make_unique<TTestAuthorizerActor>();
        authorizerActor->AuthorizeHandler =
            [&] (const TEvAuth::TEvAuthorizationRequest::TPtr& ev) {
                ++authorizeHandlerCount;
                UNIT_ASSERT_EQUAL(ev->Get()->Token, authToken);
                UNIT_ASSERT_EQUAL(
                    ev->Get()->Permissions,
                    CreatePermissionList({EPermission::Create}));
                return std::make_unique<bool>(authorizeResult);
            };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestAuthorizer(std::move(authorizerActor));

        auto service = CreateAuthService(
            testService,
            CreateKikimrAuthProvider(actorSystem),
            {});

        // When requiring authorization and failing it, we fail the request.
        {
            auto request = std::make_shared<NProto::TCreateFileStoreRequest>();
            request->MutableHeaders()->MutableInternal()->
                SetRequestSource(NProto::SOURCE_SECURE_CONTROL_CHANNEL);
            request->MutableHeaders()->MutableInternal()->
                SetAuthToken(authToken);

            auto callContext = MakeIntrusive<TCallContext>();
            callContext->RequestType = EFileStoreRequest::CreateFileStore;
            auto future = service->CreateFileStore(
                std::move(callContext),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(0));

            const auto& response = future.GetValue(TDuration::Seconds(0));
            UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_UNAUTHORIZED);
            UNIT_ASSERT_EQUAL(volumeHandlerCount, 0);
            UNIT_ASSERT_EQUAL(authorizeHandlerCount, 1);
        }

        // When not requiring authorization, we skip it.
        {
            auto request = std::make_shared<NProto::TCreateFileStoreRequest>();
            request->MutableHeaders()->MutableInternal()->
                SetRequestSource(NProto::SOURCE_FD_DATA_CHANNEL);
            request->MutableHeaders()->MutableInternal()->
                SetAuthToken(TString());

            auto callContext = MakeIntrusive<TCallContext>();
            callContext->RequestType = EFileStoreRequest::CreateFileStore;
            auto future = service->CreateFileStore(
                std::move(callContext),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(0));

            const auto& response = future.GetValue(TDuration::Seconds(0));
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_EQUAL(volumeHandlerCount, 1);
            UNIT_ASSERT_EQUAL(authorizeHandlerCount, 1);
        }

        // When requiring authorization and succeeding, we allow the request.
        {
            authorizeResult = true;
            auto request = std::make_shared<NProto::TCreateFileStoreRequest>();
            request->MutableHeaders()->MutableInternal()->
                SetRequestSource(NProto::SOURCE_SECURE_CONTROL_CHANNEL);
            request->MutableHeaders()->MutableInternal()->
                SetAuthToken(authToken);

            auto callContext = MakeIntrusive<TCallContext>();
            callContext->RequestType = EFileStoreRequest::CreateFileStore;
            auto future = service->CreateFileStore(
                std::move(callContext),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(0));

            const auto& response = future.GetValue(TDuration::Seconds(0));
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_EQUAL(volumeHandlerCount, 2);
            UNIT_ASSERT_EQUAL(authorizeHandlerCount, 2);
        }
    }

    Y_UNIT_TEST(ShouldHandleRequestTimeout)
    {
        auto authorizerActor = std::make_unique<TTestAuthorizerActor>();
        authorizerActor->AuthorizeHandler =
            [] (const TEvAuth::TEvAuthorizationRequest::TPtr& ev) {
                Y_UNUSED(ev);
                return nullptr;
            };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestAuthorizer(std::move(authorizerActor));

        auto service = CreateAuthService(
            std::make_shared<TFileStoreTest>(),
            CreateKikimrAuthProvider(actorSystem),
            {});

        auto request = std::make_shared<NProto::TCreateFileStoreRequest>();
        auto& headers = *request->MutableHeaders();
        headers.SetRequestTimeout(100); // ms

        auto callContext = MakeIntrusive<TCallContext>();
        callContext->RequestType = EFileStoreRequest::CreateFileStore;
        auto future = service->CreateFileStore(
            std::move(callContext),
            std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(5));

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_REJECTED);
    }
}

}   // namespace NCloud::NFileStore
