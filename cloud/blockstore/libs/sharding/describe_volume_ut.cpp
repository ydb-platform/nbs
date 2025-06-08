#include "describe_volume.h"

#include "config.h"
#include "sharding_manager.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/config/sharding.pb.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestServiceClient
    : public IBlockStore
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

    #define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                             \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        ++name##Called;                                                        \
        return name##Promise.GetFuture();                                      \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

    #define BLOCKSTORE_IMPLEMENT_PROMISE(name, ...)                            \
    TPromise<NProto::T##name##Response> name##Promise =                        \
        NewPromise<NProto::T##name##Response>();                               \
    ui32 name##Called = 0;                                                     \
// BLOCKSTORE_IMPLEMENT_PROMISE

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_PROMISE)

#undef BLOCKSTORE_IMPLEMENT_PROMISE

};

std::shared_ptr<TTestServiceClient> CreateShardEndpoint(
    const TString& shardId,
    const TString& host,
    TShardsEndpoints& endpoints)
{
    auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();
    auto service = std::make_shared<TTestServiceClient>();
    endpoints[shardId].emplace_back(
        clientAppConfig,
        host,
        service,
        service);
    return service;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDescribeVolumeTest)
{
    Y_UNIT_TEST(ShouldDescribeRemoteVolume)
    {
        TShardsEndpoints endpoints;

        auto s1h1Client = CreateShardEndpoint("shard1", "s1h1", endpoints);
        auto s2h1Client = CreateShardEndpoint("shard2", "s2h1", endpoints);

        NProto::TDescribeVolumeRequest request;
        request.MutableHeaders()->CopyFrom(NProto::THeaders());
        request.SetDiskId("shard1disk");

        auto localService = std::make_shared<TTestServiceClient>();

        TShardingArguments args;
        args.Logging = CreateLoggingService("console");
        args.Scheduler = CreateScheduler();
        args.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(Max<ui32>()),
            args);

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        NProto::TDescribeVolumeResponse msg;
        s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            "shard1",
            describeResponse.GetShardId());
    }

    Y_UNIT_TEST(ShouldDescribeLocalVolume)
    {
        TShardsEndpoints endpoints;

        auto s1h1Client = CreateShardEndpoint("shard1", "s1h1", endpoints);
        auto s2h1Client = CreateShardEndpoint("shard2", "s2h1", endpoints);

        auto localService = std::make_shared<TTestServiceClient>();

        NProto::TDescribeVolumeRequest request;
        request.MutableHeaders()->CopyFrom(NProto::THeaders());
        request.SetDiskId("shard1disk");

        TShardingArguments args;
        args.Logging = CreateLoggingService("console");
        args.Scheduler = CreateScheduler();
        args.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(Max<ui32>()),
            args);

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        NProto::TDescribeVolumeResponse msg;
        s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            "shard1",
            describeResponse.GetShardId());
    }

    Y_UNIT_TEST(ShouldReturnFatalErrorIfVolumeIsAbsent)
    {
        TShardsEndpoints endpoints;

        auto s1h1Client = CreateShardEndpoint("shard1", "s1h1", endpoints);
        auto s2h1Client = CreateShardEndpoint("shard2", "s2h1", endpoints);

        auto localService = std::make_shared<TTestServiceClient>();

        NProto::TDescribeVolumeRequest request;
        request.MutableHeaders()->CopyFrom(NProto::THeaders());
        request.SetDiskId("shard1disk");

        TShardingArguments args;
        args.Logging = CreateLoggingService("console");
        args.Scheduler = CreateScheduler();
        args.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(Max<ui32>()),
            args);

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

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

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "lost",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReturnRetribleErrorIfAtLeastOneShardIsNotReachable)
    {
        TShardsEndpoints endpoints;

        auto s1h1Client = CreateShardEndpoint("shard1", "s1h1", endpoints);
        auto s2h1Client = CreateShardEndpoint("shard2", "s2h1", endpoints);

        auto localService = std::make_shared<TTestServiceClient>();

        NProto::TDescribeVolumeRequest request;
        request.MutableHeaders()->CopyFrom(NProto::THeaders());
        request.SetDiskId("shard1disk");

        TShardingArguments args;
        args.Logging = CreateLoggingService("console");
        args.Scheduler = CreateScheduler();
        args.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(Max<ui32>()),
            args);

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(
                E_GRPC_UNAVAILABLE,
                "connection lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_GRPC_UNAVAILABLE,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "connection lost",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReturnRetribleErrorIfAtLeastOneShardUnavailable)
    {
        TShardsEndpoints endpoints;

        auto s1h1Client = CreateShardEndpoint("shard1", "s1h1", endpoints);

        auto localService = std::make_shared<TTestServiceClient>();

        NProto::TDescribeVolumeRequest request;
        request.MutableHeaders()->CopyFrom(NProto::THeaders());
        request.SetDiskId("shard1disk");

        TShardingArguments args;
        args.Logging = CreateLoggingService("console");
        args.Scheduler = CreateScheduler();
        args.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            true,
            TDuration::Seconds(Max<ui32>()),
            args);

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

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

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "Not all shards available",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReplyRetriableErrorOnTimeout)
    {
        TShardsEndpoints endpoints;

        auto s1h1Client = CreateShardEndpoint("shard1", "s1h1", endpoints);
        auto s2h1Client = CreateShardEndpoint("shard2", "s2h1", endpoints);

        NProto::TDescribeVolumeRequest request;
        request.MutableHeaders()->CopyFrom(NProto::THeaders());
        request.SetDiskId("shard1disk");

        auto localService = std::make_shared<TTestServiceClient>();

        TShardingArguments args;
        args.Logging = CreateLoggingService("console");
        args.Scheduler = CreateScheduler();
        args.Scheduler->Start();

        auto response = DescribeVolume(
            request,
            localService,
            endpoints,
            false,
            TDuration::Seconds(1),
            args);

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        auto describeResponse = response->GetValue(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "Describe timeout",
            describeResponse.GetError().GetMessage());
    }
}

}   // namespace NCloud::NBlockStore::NSharding
