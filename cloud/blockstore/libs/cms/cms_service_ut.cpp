#include "cms_service.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_filtered.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TExecuteActionResponse> ExecuteCms(
    const IBlockStorePtr& service, TString input)
{
    auto request = std::make_shared<NProto::TExecuteActionRequest>();
    request->SetAction("CmS");
    request->SetInput(std::move(input));
    return service->ExecuteAction(
        MakeIntrusive<TCallContext>(), std::move(request));
}

NProto::TCmsActionResponse ParseCmsResponse(
    const NProto::TExecuteActionResponse& response)
{
    NProto::TCmsActionResponse result;
    UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                    response.GetOutput(), &result)
                    .ok());
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCmsServiceTest)
{
    Y_UNIT_TEST(ShouldExecuteCmsAction)
    {
        auto inner = std::make_shared<TTestService>();
        auto promise = NewPromise<NProto::TCmsActionResponse>();
        ui32 requests = 0;
        inner->CmsActionHandler =
            [&](std::shared_ptr<NProto::TCmsActionRequest> request)
        {
            ++requests;
            UNIT_ASSERT_VALUES_EQUAL(2, request->ActionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1234, request->GetHeaders().GetRequestTimeout());
            const auto& host = request->GetActions(0);
            UNIT_ASSERT_EQUAL(NProto::TAction::REMOVE_HOST, host.GetType());
            UNIT_ASSERT_VALUES_EQUAL("localhost", host.GetHost());
            UNIT_ASSERT(!host.GetDryRun());
            const auto& device = request->GetActions(1);
            UNIT_ASSERT_EQUAL(NProto::TAction::REMOVE_DEVICE, device.GetType());
            UNIT_ASSERT_VALUES_EQUAL("otherhost", device.GetHost());
            UNIT_ASSERT_VALUES_EQUAL("/dev/nvme0n1", device.GetDevice());
            UNIT_ASSERT(device.GetDryRun());
            return promise.GetFuture();
        };

        auto service = CreateCmsService(inner);
        auto future = ExecuteCms(service, R"({
            "Headers": {"RequestTimeout": 1234},
            "Actions": [
                {"Type": "REMOVE_HOST", "Host": "localhost"},
                {"Type": "REMOVE_DEVICE", "Host": "otherhost",
                 "Device": "/dev/nvme0n1", "DryRun": true}
            ]
        })");
        UNIT_ASSERT_VALUES_EQUAL(1, requests);
        UNIT_ASSERT(!future.HasValue());

        NProto::TCmsActionResponse cmsResponse;
        auto* result = cmsResponse.AddActionResults();
        *result->MutableResult() = MakeError(E_TRY_AGAIN, "busy");
        result->SetTimeout(42);
        result->AddDependentDisks("vol0");
        cmsResponse.AddActionResults()->MutableResult()->SetCode(S_OK);
        promise.SetValue(cmsResponse);

        const auto response = future.GetValueSync();
        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
        const auto parsed = ParseCmsResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(2, parsed.ActionResultsSize());
        const auto& r = parsed.GetActionResults(0);
        UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, r.GetResult().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("busy", r.GetResult().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(42, r.GetTimeout());
        UNIT_ASSERT_VALUES_EQUAL(1, r.DependentDisksSize());
        UNIT_ASSERT_VALUES_EQUAL("vol0", r.GetDependentDisks(0));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK, parsed.GetActionResults(1).GetResult().GetCode());
    }

    Y_UNIT_TEST(ShouldPropagateCmsError)
    {
        auto inner = std::make_shared<TTestService>();
        const auto error = MakeError(E_REJECTED, "tablet unavailable", 123);
        inner->CmsActionHandler =
            [&](std::shared_ptr<NProto::TCmsActionRequest>)
        {
            NProto::TCmsActionResponse response;
            *response.MutableError() = error;
            return MakeFuture(std::move(response));
        };

        const auto response =
            ExecuteCms(CreateCmsService(inner), "{}").GetValueSync();
        const auto parsed = ParseCmsResponse(response);
        for (const auto* actual: {&response.GetError(), &parsed.GetError()}) {
            UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), actual->GetCode());
            UNIT_ASSERT_VALUES_EQUAL(error.GetMessage(), actual->GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(error.GetFlags(), actual->GetFlags());
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidCmsInput)
    {
        auto inner = std::make_shared<TTestService>();
        auto service = CreateCmsService(inner);
        for (const auto* input: {"not a json", "", "{\"UnknownField\": 1}"}) {
            const auto response = ExecuteCms(service, input).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "Failed to parse input", response.GetError().GetMessage());
            UNIT_ASSERT(response.GetOutput().empty());
        }
    }

    Y_UNIT_TEST(ShouldForwardCmsRequests)
    {
        auto inner = std::make_shared<TTestService>();
        auto request = std::make_shared<NProto::TCmsActionRequest>();
        request->AddActions()->SetType(NProto::TAction::GET_DEPENDENT_DISKS);
        auto promise = NewPromise<NProto::TCmsActionResponse>();
        inner->CmsActionHandler =
            [&](std::shared_ptr<NProto::TCmsActionRequest> forwarded)
        {
            UNIT_ASSERT_EQUAL(request, forwarded);
            return promise.GetFuture();
        };

        auto future = CreateCmsService(inner)->CmsAction(
            MakeIntrusive<TCallContext>(), request);
        UNIT_ASSERT(!future.HasValue());

        NProto::TCmsActionResponse response;
        response.AddActionResults()->AddDependentDisks("disk0");
        promise.SetValue(response);
        UNIT_ASSERT_VALUES_EQUAL(
            "disk0",
            future.GetValueSync().GetActionResults(0).GetDependentDisks(0));
    }

    Y_UNIT_TEST(ShouldForwardOtherRequests)
    {
        auto inner = std::make_shared<TTestService>();
        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        request->SetAction("otheraction");
        request->SetInput("input");
        inner->ExecuteActionHandler =
            [&](std::shared_ptr<NProto::TExecuteActionRequest> forwarded)
        {
            UNIT_ASSERT_EQUAL(request, forwarded);
            NProto::TExecuteActionResponse response;
            response.SetOutput("forwarded");
            return MakeFuture(std::move(response));
        };
        inner->PingHandler = [](std::shared_ptr<NProto::TPingRequest>)
        {
            NProto::TPingResponse response;
            *response.MutableError() = MakeError(E_REJECTED, "ping error");
            return MakeFuture(std::move(response));
        };

        auto service = CreateCmsService(inner);
        const auto response =
            service->ExecuteAction(MakeIntrusive<TCallContext>(), request)
                .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL("forwarded", response.GetOutput());
        const auto ping = service
                              ->Ping(
                                  MakeIntrusive<TCallContext>(),
                                  std::make_shared<NProto::TPingRequest>())
                              .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, ping.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("ping error", ping.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldRespectRequestFilter)
    {
        auto service = CreateFilteredService(
            CreateCmsService(std::make_shared<TTestService>()),
            {EBlockStoreRequest::Ping});
        const auto action = ExecuteCms(service, "{}").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, action.GetError().GetCode());
        const auto cms = service
                             ->CmsAction(
                                 MakeIntrusive<TCallContext>(),
                                 std::make_shared<NProto::TCmsActionRequest>())
                             .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, cms.GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore
