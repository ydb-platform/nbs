#include "cell_impl.h"
#include "endpoint_bootstrap.h"

#include <cloud/blockstore/config/cells.pb.h>
#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCellHostInfo
{
    TString Fqdn;
    NProto::ECellDataTransport Transport;
    TPromise<NClient::IMultiClientEndpointPtr> GrpcSetupPromise =
        NewPromise<NClient::IMultiClientEndpointPtr>();
    TPromise<TResultOrError<IBlockStorePtr>> RdmaSetupPromise =
        NewPromise<TResultOrError<IBlockStorePtr>>();
};

using TCellHosts = THashMap<TString, TCellHostInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TTestHostEndpointsSetupProvider: public ICellHostEndpointBootstrap
{
    using ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture;
    using ICellHostEndpointBootstrap::TRdmaEndpointBootstrapFuture;

    TCellHosts Hosts;

    explicit TTestHostEndpointsSetupProvider(TCellHosts hosts)
        : Hosts(std::move(hosts))
    {}

    TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) override
    {
        Y_UNUSED(bootstrap);

        return Hosts[config.GetFqdn()].GrpcSetupPromise.GetFuture();
    }

    TRdmaEndpointBootstrapFuture SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        IBlockStorePtr client) override
    {
        Y_UNUSED(bootstrap);
        Y_UNUSED(client);

        return Hosts[config.GetFqdn()].RdmaSetupPromise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore: public IBlockStore
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

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                        \
    TFuture<NProto::T##name##Response> name(                        \
        TCallContextPtr callContext,                                \
        std::shared_ptr<NProto::T##name##Request> request) override \
    {                                                               \
        Y_UNUSED(callContext);                                      \
        Y_UNUSED(request);                                          \
        return MakeFuture<NProto::T##name##Response>();             \
    }                                                               \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

struct TTestGrpcClient: public NClient::IMultiHostClient
{
    void Start() override
    {}

    void Stop() override
    {}

    virtual IBlockStorePtr
    CreateEndpoint(const TString& host, ui32 port, bool isSecure) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Y_UNUSED(isSecure);
        return std::make_shared<TTestBlockStore>();
    }

    virtual IBlockStorePtr
    CreateDataEndpoint(const TString& host, ui32 port, bool isSecure) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Y_UNUSED(isSecure);
        return {};
    }
};

void ConfigureHosts(NProto::TCellConfig& proto, const TCellHosts& hosts)
{
    for (const auto& host: hosts) {
        NProto::TCellHostConfig cfg;
        cfg.SetFqdn(host.second.Fqdn);
        cfg.SetTransport(host.second.Transport);
        *proto.AddHosts() = std::move(cfg);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellTest)
{
    Y_UNIT_TEST(ShouldAllocateEndpoints)
    {
        TCellHosts hosts{
            {{"h1",
              {.Fqdn = "h1", .Transport = NProto::CELL_DATA_TRANSPORT_GRPC}},
             {"h2",
              {.Fqdn = "h2", .Transport = NProto::CELL_DATA_TRANSPORT_GRPC}},
             {"h3",
              {.Fqdn = "h3", .Transport = NProto::CELL_DATA_TRANSPORT_GRPC}}}};

        NProto::TCellConfig proto;
        proto.SetMinCellConnections(1);
        proto.SetTransport(NProto::CELL_DATA_TRANSPORT_GRPC);
        ConfigureHosts(proto, hosts);
        TCellConfig config{std::move(proto)};

        TBootstrap bootstrap;
        bootstrap.GrpcClient = std::make_shared<TTestGrpcClient>();
        bootstrap.EndpointsSetup =
            std::make_shared<TTestHostEndpointsSetupProvider>(hosts);

        auto cell = std::make_shared<TCell>(bootstrap, config);

        auto clientConfig = std::make_shared<NClient::TClientAppConfig>();

        {
            auto result = cell->GetCellClient(clientConfig);

            UNIT_ASSERT_C(HasError(result), "No cells ready, should fail");
        }

        auto grpc = NClient::CreateMultiClientEndpoint(
            std::make_shared<TTestGrpcClient>(),
            "h1",
            9766,
            false);

        hosts[cell->GetActivatingHosts().begin()->first]
            .GrpcSetupPromise.SetValue(grpc);

        {
            auto result = cell->GetCellClient(clientConfig);

            UNIT_ASSERT_C(!HasError(result), "Should not fail");
        }
    }

    Y_UNIT_TEST(ShouldAllocateMultipleEndpoints)
    {
        TCellHosts hosts{
            {{"h1",
              {.Fqdn = "h1", .Transport = NProto::CELL_DATA_TRANSPORT_GRPC}},
             {"h2",
              {.Fqdn = "h2", .Transport = NProto::CELL_DATA_TRANSPORT_GRPC}},
             {"h3",
              {.Fqdn = "h3", .Transport = NProto::CELL_DATA_TRANSPORT_GRPC}}}};

        NProto::TCellConfig proto;
        proto.SetDescribeVolumeHostCount(3);
        proto.SetMinCellConnections(3);
        proto.SetTransport(NProto::CELL_DATA_TRANSPORT_GRPC);
        ConfigureHosts(proto, hosts);
        TCellConfig config{std::move(proto)};

        TBootstrap bootstrap;
        bootstrap.GrpcClient = std::make_shared<TTestGrpcClient>();
        bootstrap.EndpointsSetup =
            std::make_shared<TTestHostEndpointsSetupProvider>(hosts);

        auto cell = std::make_shared<TCell>(bootstrap, config);
        cell->Start();

        auto clientConfig = std::make_shared<NClient::TClientAppConfig>();

        {
            auto result = cell->GetCellClients(clientConfig);

            UNIT_ASSERT_VALUES_EQUAL(0, result.size());
        }

        auto h1grpc = NClient::CreateMultiClientEndpoint(
            std::make_shared<TTestGrpcClient>(),
            "h1",
            9766,
            false);

        auto h2grpc = NClient::CreateMultiClientEndpoint(
            std::make_shared<TTestGrpcClient>(),
            "h2",
            9766,
            false);

        auto h3grpc = NClient::CreateMultiClientEndpoint(
            std::make_shared<TTestGrpcClient>(),
            "h3",
            9766,
            false);

        hosts["h1"].GrpcSetupPromise.SetValue(h1grpc);
        hosts["h2"].GrpcSetupPromise.SetValue(h1grpc);
        hosts["h3"].GrpcSetupPromise.SetValue(h1grpc);

        {
            auto result = cell->GetCellClients(clientConfig);

            UNIT_ASSERT_VALUES_EQUAL(3, result.size());
        }
    }
}

}   // namespace NCloud::NBlockStore::NCells
