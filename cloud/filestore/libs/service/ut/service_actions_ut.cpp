#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/service/service_actions.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TExecuteActionResponse ExecuteAction(
    const IFileStoreServicePtr& service,
    const TString& action,
    const TString& input)
{
    auto request = std::make_shared<NProto::TExecuteActionRequest>();
    request->SetAction(action);
    request->SetInput(input);
    return service
        ->ExecuteAction(MakeIntrusive<TCallContext>(), std::move(request))
        .GetValueSync();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TActionsServiceTest)
{
    Y_UNIT_TEST(ShouldExecuteOwnActions)
    {
        auto inner = std::make_shared<TFileStoreTest>();
        inner->ExecuteActionHandler =
            [](auto, std::shared_ptr<NProto::TExecuteActionRequest>)
        {
            UNIT_FAIL("must not be forwarded");
            return MakeFuture<NProto::TExecuteActionResponse>();
        };

        TVector<TString> inputs;
        auto service = CreateActionsService(
            inner,
            {{"OwnAction",
              [&](const TString& input)
              {
                  inputs.push_back(input);
                  NProto::TExecuteActionResponse response;
                  response.SetOutput("own");
                  return MakeFuture(std::move(response));
              }}});

        auto response = ExecuteAction(service, "ownACTION", "{}");
        UNIT_ASSERT_VALUES_EQUAL("own", response.GetOutput());
        UNIT_ASSERT_VALUES_EQUAL(1, inputs.size());
        UNIT_ASSERT_VALUES_EQUAL("{}", inputs[0]);
    }

    Y_UNIT_TEST(ShouldForwardOtherRequests)
    {
        auto inner = std::make_shared<TFileStoreTest>();
        inner->ExecuteActionHandler =
            [](auto, std::shared_ptr<NProto::TExecuteActionRequest> request)
        {
            UNIT_ASSERT_VALUES_EQUAL("otheraction", request->GetAction());
            NProto::TExecuteActionResponse response;
            response.SetOutput("forwarded");
            return MakeFuture(std::move(response));
        };
        inner->PingHandler = [](auto, std::shared_ptr<NProto::TPingRequest>)
        {
            return MakeFuture<NProto::TPingResponse>();
        };

        auto service = CreateActionsService(
            inner,
            {{"ownaction",
              [](const TString&)
              {
                  UNIT_FAIL("must not be called");
                  return MakeFuture<NProto::TExecuteActionResponse>();
              }}});

        auto response = ExecuteAction(service, "otheraction", "");
        UNIT_ASSERT_VALUES_EQUAL("forwarded", response.GetOutput());

        auto ping = service
            ->Ping(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TPingRequest>())
            .GetValueSync();
        UNIT_ASSERT_C(!HasError(ping), FormatError(ping.GetError()));
    }
}

}   // namespace NCloud::NFileStore
