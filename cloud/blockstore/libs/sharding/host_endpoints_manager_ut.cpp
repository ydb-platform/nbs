#include "host_endpoints_manager.h"

#include "config.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/config/sharding.pb.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TestHostEndpointsSetupProvider
    : public IHostEndpointsSetupProvider
{
    using IHostEndpointsSetupProvider::TSetupGrpcEndpointFuture;
    using IHostEndpointsSetupProvider::TSetupRdmaEndpointFuture;

    TPromise<NClient::IMultiClientEndpointPtr> GrpcSetupPromise = NewPromise<NClient::IMultiClientEndpointPtr>();
    TPromise<IBlockStorePtr> RdmaSetupPromise = NewPromise<IBlockStorePtr>();

    TSetupGrpcEndpointFuture SetupHostGrpcEndpoint(
        const TShardingArguments& args,
        const TShardHostConfig& config) override
    {
        Y_UNUSED(args);
        Y_UNUSED(config);

        return GrpcSetupPromise.GetFuture();
    };

    TSetupRdmaEndpointFuture SetupHostRdmaEndpoint(
        const TShardingArguments& args,
        const TShardHostConfig& config,
        IBlockStorePtr client) override
    {
        Y_UNUSED(args);
        Y_UNUSED(config);
        Y_UNUSED(client);

        return RdmaSetupPromise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore
    : public IBlockStore
{
    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void Start() override
    {}

    void Stop() override
    {}

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>();                        \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

struct TTestGrpcClient
    : public NClient::IClient
{
    void Start() override
    {}

    void Stop() override
    {}

    IBlockStorePtr CreateEndpoint() override
    {
        return {};
    }

    virtual IBlockStorePtr CreateEndpoint(
        const TString& host,
        ui32 port,
        bool isSecure) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Y_UNUSED(isSecure);
        return std::make_shared<TTestBlockStore>();
    }

    IBlockStorePtr CreateDataEndpoint()  override
    {
        return {};
    }

    virtual IBlockStorePtr CreateDataEndpoint(
        const TString& host,
        ui32 port,
        bool isSecure) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Y_UNUSED(isSecure);
        return {};
    }


    virtual IBlockStorePtr CreateDataEndpoint(
        const TString& socketPath) override
    {
        Y_UNUSED(socketPath);
        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THostEndpointsManagerTest)
{
    Y_UNIT_TEST(ShouldStartGrpcEndpoint)
    {
        NProto::TShardConfig shardCfg;
        shardCfg.SetGrpcPort(1);
        shardCfg.SetTransport(NProto::GRPC);
        TShardConfig shard{shardCfg};

        NProto::TShardHostConfig hostCfg;
        hostCfg.SetFqdn("host");
        TShardHostConfig hostConfig{hostCfg, shard};

        auto setup = std::make_shared<TestHostEndpointsSetupProvider>();

        TShardingArguments args;
        args.EndpointsSetup = setup;

        auto manager = std::make_shared<THostEndpointsManager>(
            hostConfig,
            args);

        UNIT_ASSERT_VALUES_EQUAL(false, (bool)manager->GrpcHostEndpoint);
        UNIT_ASSERT_VALUES_EQUAL(false, (bool)manager->RdmaHostEndpoint);

        auto started = manager->Start();

        auto grpcEndpoint = NClient::CreateMultiClientEndpoint(
            std::make_shared<TTestGrpcClient>(),
            "host",
            9766,
            false);

        auto clientEndpoint = grpcEndpoint->CreateClientEndpoint("", "");

        setup->GrpcSetupPromise.SetValue(grpcEndpoint);
        UNIT_ASSERT_C(started.HasValue(), "started not set");

        UNIT_ASSERT(manager->IsReady(NProto::GRPC));

        // check GRPC
        {
            auto clientConfig = std::make_shared<NClient::TClientAppConfig>();
            auto response = manager->GetHostEndpoint(
                clientConfig,
                NProto::GRPC,
                false);

            UNIT_ASSERT_C(!HasError(response.GetError()), "should not fail");
            auto endpoint = response.GetResult();
            UNIT_ASSERT_C(
                clientEndpoint == endpoint.GetService(),
                "Services do not match");
            UNIT_ASSERT_C(
                nullptr != endpoint.GetStorage(),
                "Storage should not be null");
        }

        // check fallback to GRPC
        {
            auto clientConfig = std::make_shared<NClient::TClientAppConfig>();

            auto failedResponse = manager->GetHostEndpoint(
                clientConfig,
                NProto::RDMA,
                false);

            UNIT_ASSERT_C(
                HasError(failedResponse.GetError()),
                "should fail without fallback");

            auto response = manager->GetHostEndpoint(
                clientConfig,
                NProto::RDMA,
                true);

            UNIT_ASSERT_C(
                !HasError(response.GetError()),
                "should fail with fallback");

            UNIT_ASSERT_C(!HasError(response.GetError()), "should not fail");
            auto endpoint = response.GetResult();
            UNIT_ASSERT_C(
                clientEndpoint == endpoint.GetService(),
                "Services do not match");
            UNIT_ASSERT_C(
                nullptr != endpoint.GetStorage(),
                "Storage should not be null");
        }
    }

    Y_UNIT_TEST(ShouldStartRdmaEndpoint)
    {
        NProto::TShardConfig shardCfg;
        shardCfg.SetGrpcPort(1);
        shardCfg.SetTransport(NProto::RDMA);
        TShardConfig shard{shardCfg};

        NProto::TShardHostConfig hostCfg;
        hostCfg.SetFqdn("host");
        TShardHostConfig hostConfig{hostCfg, shard};

        auto setup = std::make_shared<TestHostEndpointsSetupProvider>();

        TShardingArguments args;
        args.EndpointsSetup = setup;

        auto manager = std::make_shared<THostEndpointsManager>(
            hostConfig,
            args);

        UNIT_ASSERT_VALUES_EQUAL(false, (bool)manager->GrpcHostEndpoint);
        UNIT_ASSERT_VALUES_EQUAL(false, (bool)manager->RdmaHostEndpoint);

        auto started = manager->Start();

        auto grpcEndpoint = NClient::CreateMultiClientEndpoint(
            std::make_shared<TTestGrpcClient>(),
            "host",
            9766,
            false);

        auto clientEndpoint = grpcEndpoint->CreateClientEndpoint("", "");

        setup->GrpcSetupPromise.SetValue(grpcEndpoint);
        UNIT_ASSERT_C(started.HasValue(), "started not set");

        UNIT_ASSERT(!manager->IsReady(NProto::RDMA));

        setup->RdmaSetupPromise.SetValue(std::make_shared<TTestBlockStore>());

        UNIT_ASSERT(manager->IsReady(NProto::RDMA));
        UNIT_ASSERT(manager->IsReady(NProto::GRPC));

        // check GRPC

        {
            auto clientConfig = std::make_shared<NClient::TClientAppConfig>();
            auto response = manager->GetHostEndpoint(
                clientConfig,
                NProto::GRPC,
                false);

            UNIT_ASSERT_C(!HasError(response.GetError()), "should not fail");
            auto endpoint = response.GetResult();
            UNIT_ASSERT_C(
                clientEndpoint == endpoint.GetService(),
                "Services do not match");
            UNIT_ASSERT_C(
                nullptr != endpoint.GetStorage(),
                "Storage should not be null");
        }

        // check RDMA

        {
            auto clientConfig = std::make_shared<NClient::TClientAppConfig>();
            auto response = manager->GetHostEndpoint(
                clientConfig,
                NProto::RDMA,
                false);

            UNIT_ASSERT_C(!HasError(response.GetError()), "should not fail");
            auto endpoint = response.GetResult();
            UNIT_ASSERT_C(
                clientEndpoint == endpoint.GetService(),
                "Services do not match");
            UNIT_ASSERT_C(
                nullptr != endpoint.GetStorage(),
                "Storage should not be null");
        }

    }
}

}   // namespace NCloud::NBlockStore::NSharding
