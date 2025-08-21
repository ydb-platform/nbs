#include "describe_volume.h"

#include "remote_storage.h"

#include <cloud/blockstore/config/cells.pb.h>
#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestServiceClient: public IBlockStore
{
    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return {};
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                      \
    std::function<void(const NProto::T##name##Request&)> On##name;  \
    TFuture<NProto::T##name##Response> name(                        \
        TCallContextPtr callContext,                                \
        std::shared_ptr<NProto::T##name##Request> request) override \
    {                                                               \
        Y_UNUSED(callContext);                                      \
        Y_UNUSED(request);                                          \
        ++name##Called;                                             \
        if (On##name) {                                             \
            On##name(*request);                                     \
        }                                                           \
        return name##Promise.GetFuture();                           \
    }                                                               \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

#define BLOCKSTORE_IMPLEMENT_PROMISE(name, ...)         \
    TPromise<NProto::T##name##Response> name##Promise = \
        NewPromise<NProto::T##name##Response>();        \
    ui32 name##Called = 0;                              \
    // BLOCKSTORE_IMPLEMENT_PROMISE

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_PROMISE)

#undef BLOCKSTORE_IMPLEMENT_PROMISE
};

std::shared_ptr<TTestServiceClient> CreateService()
{
    auto service = std::make_shared<TTestServiceClient>();

    auto describeCheck = [](const NProto::TDescribeVolumeRequest& req)
    {
        UNIT_ASSERT_C(
            req.GetHeaders().HasInternal(),
            "Internal should not be set");
    };

    service->OnDescribeVolume = describeCheck;
    return service;
}

std::shared_ptr<TTestServiceClient> CreateCellEndpoint(
    const TString& cellId,
    const TString& host,
    TCellHostEndpointsByCellId& endpoints)
{
    auto describeCheck = [](const NProto::TDescribeVolumeRequest& req)
    {
        UNIT_ASSERT_C(
            !req.GetHeaders().HasInternal(),
            "Internal should not be set");
    };

    auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();
    auto service = std::make_shared<TTestServiceClient>();
    service->OnDescribeVolume = describeCheck;
    endpoints[cellId].emplace_back(
        clientAppConfig,
        host,
        service,
        CreateRemoteStorage(service));
    return service;
}

NProto::TDescribeVolumeRequest CreateDescribeRequest()
{
    NProto::TDescribeVolumeRequest request;
    request.MutableHeaders()->CopyFrom(NProto::THeaders());
    *request.MutableHeaders()->MutableInternal()->MutableAuthToken() = "auth";
    return request;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDescribeVolumeTest)
{
    Y_UNIT_TEST(ShouldDescribeRemoteVolume)
    {
        TCellHostEndpointsByCellId endpoints;

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("cell1disk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(Max<ui32>()),
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        NProto::TDescribeVolumeResponse msg;
        s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL("cell1", describeResponse.GetCellId());
    }

    Y_UNIT_TEST(ShouldDescribeLocalVolume)
    {
        TCellHostEndpointsByCellId endpoints;

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("localdisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(Max<ui32>()),
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        NProto::TDescribeVolumeResponse msg;
        localService->DescribeVolumePromise.SetValue(std::move(msg));

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL("", describeResponse.GetCellId());
    }

    Y_UNIT_TEST(ShouldReturnFatalErrorIfVolumeIsAbsent)
    {
        TCellHostEndpointsByCellId endpoints;

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(Max<ui32>()),
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "lost",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReturnRetriableErrorIfAtLeastOneCellIsNotReachable)
    {
        TCellHostEndpointsByCellId endpoints;

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(Max<ui32>()),
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() =
                std::move(MakeError(E_GRPC_UNAVAILABLE, "connection lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_GRPC_UNAVAILABLE,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "connection lost",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReturnRetribleErrorIfAtLeastOneCellUnavailable)
    {
        TCellHostEndpointsByCellId endpoints;

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            true,
            TDuration::Seconds(Max<ui32>()),
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        const auto& describeResponse = response.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "Not all cells available",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReplyRetriableErrorOnTimeout)
    {
        TCellHostEndpointsByCellId endpoints;

        auto s1h1Client = CreateCellEndpoint("cell1", "s1h1", endpoints);
        auto s2h1Client = CreateCellEndpoint("cell2", "s2h1", endpoints);

        auto request = CreateDescribeRequest();
        request.SetDiskId("celldisk");

        auto localService = CreateService();

        TBootstrap bootstrap;
        bootstrap.Logging = CreateLoggingService("console");
        bootstrap.Scheduler = CreateScheduler();
        bootstrap.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(1),
            bootstrap);

        UNIT_ASSERT_VALUES_EQUAL(1, s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(1, localService->DescribeVolumeCalled);

        const auto& describeResponse = response.GetValue(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "Describe timeout",
            describeResponse.GetError().GetMessage());

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }
    }
}

}   // namespace NCloud::NBlockStore::NCells
