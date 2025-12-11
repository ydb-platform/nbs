#include "auth_provider_kikimr.h"

#include <cloud/blockstore/libs/service/auth_provider.h>
#include <cloud/blockstore/libs/service/service_auth.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/service_kikimr/ut/kikimr_test_env.h>

#include <cloud/storage/core/libs/api/authorizer.h>
#include <cloud/storage/core/libs/auth/authorizer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NBlockStore;
using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestAuthorizerActor: public TActor<TTestAuthorizerActor>
{
public:
    std::function<std::unique_ptr<bool>(
        const TEvAuth::TEvAuthorizationRequest::TPtr&)>
        AuthorizeHandler;

public:
    TTestAuthorizerActor()
        : TActor(&TThis::StateWork)
    {}

private:
    STRICT_STFUNC(StateWork,
                  HFunc(TEvAuth::TEvAuthorizationRequest, HandleAuthorize);)

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
                std::make_unique<TEvAuth::TEvAuthorizationResponse>(
                    std::move(error)));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKikimrAuthProviderTest)
{
    Y_UNIT_TEST(CheckPermissionForCreateVolume)
    {
        int volumeHandlerCount = 0;
        auto testService = std::make_shared<TTestService>();
        testService->CreateVolumeHandler =
            [&](std::shared_ptr<NProto::TCreateVolumeRequest> request)
        {
            Y_UNUSED(request);
            ++volumeHandlerCount;
            return MakeFuture<NProto::TCreateVolumeResponse>();
        };

        const TString authToken = "TEST_AUTH_TOKEN";

        bool authorizeResult = false;
        int authorizeHandlerCount = 0;
        auto authorizerActor = std::make_unique<TTestAuthorizerActor>();
        authorizerActor->AuthorizeHandler =
            [&](const TEvAuth::TEvAuthorizationRequest::TPtr& ev)
        {
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
            CreateKikimrAuthProvider(actorSystem));

        // When requiring authorization and failing it, we fail the request.
        {
            auto request = std::make_shared<NProto::TCreateVolumeRequest>();
            request->MutableHeaders()->MutableInternal()->SetRequestSource(
                NProto::SOURCE_SECURE_CONTROL_CHANNEL);
            request->MutableHeaders()->MutableInternal()->SetAuthToken(
                authToken);

            auto future = service->CreateVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(0));

            const auto& response = future.GetValue(TDuration::Seconds(0));
            UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_UNAUTHORIZED);
            UNIT_ASSERT_EQUAL(volumeHandlerCount, 0);
            UNIT_ASSERT_EQUAL(authorizeHandlerCount, 1);
        }

        // When not requiring authorization, we skip it.
        {
            auto request = std::make_shared<NProto::TCreateVolumeRequest>();
            request->MutableHeaders()->MutableInternal()->SetRequestSource(
                NProto::SOURCE_TCP_DATA_CHANNEL);
            request->MutableHeaders()->MutableInternal()->SetAuthToken(
                TString());

            auto future = service->CreateVolume(
                MakeIntrusive<TCallContext>(),
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
            auto request = std::make_shared<NProto::TCreateVolumeRequest>();
            request->MutableHeaders()->MutableInternal()->SetRequestSource(
                NProto::SOURCE_SECURE_CONTROL_CHANNEL);
            request->MutableHeaders()->MutableInternal()->SetAuthToken(
                authToken);

            auto future = service->CreateVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            actorSystem->DispatchEvents(TDuration::Seconds(0));

            const auto& response = future.GetValue(TDuration::Seconds(0));
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_EQUAL(volumeHandlerCount, 2);
            UNIT_ASSERT_EQUAL(authorizeHandlerCount, 2);
        }
    }

    Y_UNIT_TEST(DoNotCheckPermissionForRead)
    {
        int readHandlerCount = 0;
        auto testService = std::make_shared<TTestService>();
        testService->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest> request)
        {
            Y_UNUSED(request);
            ++readHandlerCount;
            return MakeFuture<NProto::TReadBlocksResponse>();
        };

        const TString authToken = "TEST_AUTH_TOKEN";

        bool authorizeResult = false;
        int authorizeHandlerCount = 0;
        auto authorizerActor = std::make_unique<TTestAuthorizerActor>();
        authorizerActor->AuthorizeHandler =
            [&](const TEvAuth::TEvAuthorizationRequest::TPtr& ev)
        {
            ++authorizeHandlerCount;
            UNIT_ASSERT_EQUAL(ev->Get()->Token, authToken);
            UNIT_ASSERT(ev->Get()->Permissions.Empty());
            return std::make_unique<bool>(authorizeResult);
        };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestAuthorizer(std::move(authorizerActor));

        auto service = CreateAuthService(
            testService,
            CreateKikimrAuthProvider(actorSystem));

        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        request->MutableHeaders()->MutableInternal()->SetRequestSource(
            NProto::SOURCE_SECURE_CONTROL_CHANNEL);
        request->MutableHeaders()->MutableInternal()->SetAuthToken(authToken);

        auto future = service->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(0));

        const auto& response = future.GetValue(TDuration::Seconds(0));
        UNIT_ASSERT(!HasError(response));
        UNIT_ASSERT_EQUAL(readHandlerCount, 1);
        UNIT_ASSERT_EQUAL(authorizeHandlerCount, 0);
    }

    Y_UNIT_TEST(CheckPermissionForReadWriteMount)
    {
        int mountHandlerCount = 0;
        auto testService = std::make_shared<TTestService>();
        testService->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            Y_UNUSED(request);
            ++mountHandlerCount;
            return MakeFuture<NProto::TMountVolumeResponse>();
        };

        const TString authToken = "TEST_AUTH_TOKEN";

        bool authorizeResult = true;
        int authorizeHandlerCount = 0;
        auto authorizerActor = std::make_unique<TTestAuthorizerActor>();
        authorizerActor->AuthorizeHandler =
            [&](const TEvAuth::TEvAuthorizationRequest::TPtr& ev)
        {
            ++authorizeHandlerCount;
            UNIT_ASSERT_EQUAL(ev->Get()->Token, authToken);
            UNIT_ASSERT_EQUAL(
                ev->Get()->Permissions,
                CreatePermissionList({EPermission::Read, EPermission::Write}));
            return std::make_unique<bool>(authorizeResult);
        };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestAuthorizer(std::move(authorizerActor));

        auto service = CreateAuthService(
            testService,
            CreateKikimrAuthProvider(actorSystem));

        auto request = std::make_shared<NProto::TMountVolumeRequest>();
        request->MutableHeaders()->MutableInternal()->SetRequestSource(
            NProto::SOURCE_SECURE_CONTROL_CHANNEL);
        request->MutableHeaders()->MutableInternal()->SetAuthToken(authToken);
        request->SetVolumeAccessMode(
            NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_WRITE);

        auto future = service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(0));

        const auto& response = future.GetValue(TDuration::Seconds(0));
        UNIT_ASSERT(!HasError(response));
        UNIT_ASSERT_EQUAL(mountHandlerCount, 1);
        UNIT_ASSERT_EQUAL(authorizeHandlerCount, 1);
    }

    Y_UNIT_TEST(CheckPermissionForReadOnlyMount)
    {
        int mountHandlerCount = 0;
        auto testService = std::make_shared<TTestService>();
        testService->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            Y_UNUSED(request);
            ++mountHandlerCount;
            return MakeFuture<NProto::TMountVolumeResponse>();
        };

        const TString authToken = "TEST_AUTH_TOKEN";

        bool authorizeResult = true;
        int authorizeHandlerCount = 0;
        auto authorizerActor = std::make_unique<TTestAuthorizerActor>();
        authorizerActor->AuthorizeHandler =
            [&](const TEvAuth::TEvAuthorizationRequest::TPtr& ev)
        {
            ++authorizeHandlerCount;
            UNIT_ASSERT_EQUAL(ev->Get()->Token, authToken);
            UNIT_ASSERT_EQUAL(
                ev->Get()->Permissions,
                CreatePermissionList({EPermission::Read}));
            return std::make_unique<bool>(authorizeResult);
        };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestAuthorizer(std::move(authorizerActor));

        auto service = CreateAuthService(
            testService,
            CreateKikimrAuthProvider(actorSystem));

        auto request = std::make_shared<NProto::TMountVolumeRequest>();
        request->MutableHeaders()->MutableInternal()->SetRequestSource(
            NProto::SOURCE_SECURE_CONTROL_CHANNEL);
        request->MutableHeaders()->MutableInternal()->SetAuthToken(authToken);
        request->SetVolumeAccessMode(
            NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_ONLY);

        auto future = service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(0));

        const auto& response = future.GetValue(TDuration::Seconds(0));
        UNIT_ASSERT(!HasError(response));
        UNIT_ASSERT_EQUAL(mountHandlerCount, 1);
        UNIT_ASSERT_EQUAL(authorizeHandlerCount, 1);
    }

    Y_UNIT_TEST(ShouldHandleRequestTimeout)
    {
        auto authorizerActor = std::make_unique<TTestAuthorizerActor>();
        authorizerActor->AuthorizeHandler =
            [](const TEvAuth::TEvAuthorizationRequest::TPtr& ev)
        {
            Y_UNUSED(ev);
            return nullptr;
        };

        auto actorSystem = MakeIntrusive<TTestActorSystem>();
        actorSystem->RegisterTestAuthorizer(std::move(authorizerActor));

        auto service = CreateAuthService(
            std::make_shared<TTestService>(),
            CreateKikimrAuthProvider(actorSystem));

        auto request = std::make_shared<NProto::TCreateVolumeRequest>();
        auto& headers = *request->MutableHeaders();
        headers.SetRequestTimeout(100);   // ms

        auto future = service->CreateVolume(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        actorSystem->DispatchEvents(TDuration::Seconds(5));

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_EQUAL(response.GetError().GetCode(), E_REJECTED);
    }
}

}   // namespace NCloud::NBlockStore::NServer
