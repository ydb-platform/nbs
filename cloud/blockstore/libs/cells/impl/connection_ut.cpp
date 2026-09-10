#include "connection.h"

#include "endpoint_bootstrap.h"
#include "host_pool.h"

#include <cloud/blockstore/config/cells.pb.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpointBootstrap: public ICellHostEndpointBootstrap
{
    TPromise<NClient::IMultiClientEndpointPtr> GrpcSetupPromise =
        NewPromise<NClient::IMultiClientEndpointPtr>();

    TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) override
    {
        Y_UNUSED(bootstrap);
        Y_UNUSED(config);
        return GrpcSetupPromise.GetFuture();
    }

    TPromise<TResultOrError<IBlockStorePtr>> RdmaSetupPromise =
        NewPromise<TResultOrError<IBlockStorePtr>>();

    // what the synchronous form hands back; the test decides when the endpoint
    // reports itself connected
    TResultOrError<IBlockStorePtr> RdmaSetupResult =
        MakeError(E_REJECTED, "no rdma endpoint in this test");

    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr RdmaHandler;

    TRdmaEndpointBootstrapFuture SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) override
    {
        Y_UNUSED(bootstrap);
        Y_UNUSED(config);

        return RdmaSetupPromise.GetFuture();
    }

    TRdmaEndpointBootstrapResult SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler) override
    {
        Y_UNUSED(bootstrap);
        Y_UNUSED(config);

        RdmaHandler = std::move(handler);
        return RdmaSetupResult;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore: public TBlockStoreImpl<TTestBlockStore, IBlockStore>
{
    TString TabletHostToReport;
    ui32 RequestCount = 0;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void Start() override
    {}

    void Stop() override
    {}

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        ++RequestCount;

        typename TMethod::TResponse response;
        if constexpr (std::is_same_v<TMethod, TBlockStoreMountVolumeMethod>) {
            response.SetTabletHost(TabletHostToReport);
        }

        return MakeFuture(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestGrpcClient: public NClient::IMultiHostClient
{
    std::shared_ptr<TTestBlockStore> Service =
        std::make_shared<TTestBlockStore>();

    void Start() override
    {}

    void Stop() override
    {}

    IBlockStorePtr
    CreateEndpoint(const TString& host, ui32 port, bool isSecure) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Y_UNUSED(isSecure);
        return Service;
    }

    IBlockStorePtr
    CreateDataEndpoint(const TString& host, ui32 port, bool isSecure) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Y_UNUSED(isSecure);
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestObserver: public ICellConnectionObserver
{
    TVector<TString> Reported;

    void OnTabletHostChanged(TString fqdn) noexcept override
    {
        Reported.push_back(std::move(fqdn));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    std::shared_ptr<TTestEndpointBootstrap> EndpointsSetup =
        std::make_shared<TTestEndpointBootstrap>();
    std::shared_ptr<TTestGrpcClient> GrpcClient =
        std::make_shared<TTestGrpcClient>();
    std::shared_ptr<TTestObserver> Observer =
        std::make_shared<TTestObserver>();

    std::shared_ptr<TTestBlockStore> RdmaService =
        std::make_shared<TTestBlockStore>();

    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>(TInstant::Zero());

    TCellConfigPtr CellConfig;
    TCellHostPoolPtr Pool;
    TBootstrap Bootstrap;

    explicit TTestEnv(
        NProto::ECellDataTransport transport =
            NProto::CELL_DATA_TRANSPORT_GRPC,
        bool grpcDataFallback = false,
        ui32 rdmaSettleTimeMs = 0)
    {
        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        proto.SetGrpcPort(9766);
        proto.SetTransport(transport);
        proto.SetGrpcDataFallbackEnabled(grpcDataFallback);
        proto.SetRdmaSettleTimeMs(rdmaSettleTimeMs);
        proto.AddHosts()->SetFqdn("host-a");
        CellConfig = std::make_shared<TCellConfig>(std::move(proto));

        // the synchronous form hands the endpoint back before it connects
        EndpointsSetup->RdmaSetupResult = IBlockStorePtr(RdmaService);

        Bootstrap.EndpointsSetup = EndpointsSetup;
        Bootstrap.GrpcClient = GrpcClient;
        Bootstrap.Logging = CreateLoggingService("console");
        Bootstrap.Timer = Timer;
        Bootstrap.Scheduler = Scheduler;

        Pool = std::make_shared<TCellHostPool>(CellConfig, Bootstrap);
    }

    TCellConnectionFuture ConnectAsync(const TString& fqdn)
    {
        auto future = CreateCellConnection(
            Pool,
            Pool->MakeHostConfig(fqdn),
            Bootstrap,
            std::make_shared<NClient::TClientAppConfig>(),
            Observer);

        EndpointsSetup->GrpcSetupPromise.TrySetValue(
            NClient::CreateMultiClientEndpoint(
                GrpcClient,
                fqdn,
                9766,
                false));

        return future;
    }

    ICellConnectionPtr Connect(const TString& fqdn)
    {
        auto future = ConnectAsync(fqdn);

        UNIT_ASSERT_C(future.HasValue(), "connection was not established");

        auto result = future.GetValue();
        UNIT_ASSERT_C(!HasError(result), result.GetError());
        return result.GetResult();
    }

    static void Read(const ICellConnectionPtr& connection)
    {
        connection->GetStorage()->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TReadBlocksLocalRequest>());
    }

    void Mount(const ICellConnectionPtr& connection)
    {
        auto response = connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        UNIT_ASSERT(response.HasValue());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellConnectionTest)
{
    Y_UNIT_TEST(ShouldReportOnlyForeignTabletHost)
    {
        TTestEnv env;
        auto connection = env.Connect("host-a");
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // an older cell reports nothing
        env.GrpcClient->Service->TabletHostToReport = "";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Observer->Reported.size());

        // the tablet already lives where we are
        env.GrpcClient->Service->TabletHostToReport = "host-a";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Observer->Reported.size());

        env.GrpcClient->Service->TabletHostToReport = "host-z";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Observer->Reported.size());
        UNIT_ASSERT_VALUES_EQUAL("host-z", env.Observer->Reported[0]);
    }

    Y_UNIT_TEST(ShouldOutliveItsOwnHandleWhileServiceIsInUse)
    {
        TTestEnv env;

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        // the endpoint above drops the handle while requests still drain
        connection.reset();

        env.GrpcClient->Service->TabletHostToReport = "host-z";
        auto response = service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        UNIT_ASSERT(response.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(1, env.Observer->Reported.size());
    }

    Y_UNIT_TEST(ShouldServeDataOverGrpcWhileRdmaIsBeingSetUp)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, true);

        auto connection = env.Connect("host-a");

        const auto mountRequests = env.GrpcClient->Service->RequestCount;

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(
            mountRequests + 1,
            env.GrpcClient->Service->RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.RdmaService->RequestCount);

        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);
        env.EndpointsSetup->RdmaHandler->HandleConnected();

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaService->RequestCount);
    }

    Y_UNIT_TEST(ShouldWaitForRdmaWhenGrpcDataFallbackIsDisabled)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false);

        auto future = env.ConnectAsync("host-a");
        UNIT_ASSERT(!future.HasValue());

        env.EndpointsSetup->RdmaSetupPromise.SetValue(
            TResultOrError<IBlockStorePtr>(env.RdmaService));

        UNIT_ASSERT(future.HasValue());
        auto result = future.GetValue();
        UNIT_ASSERT_C(!HasError(result), result.GetError());

        TTestEnv::Read(result.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaService->RequestCount);
    }

    Y_UNIT_TEST(ShouldMoveDataBackToGrpcWhenRdmaBreaks)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, true);

        auto connection = env.Connect("host-a");

        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);
        env.EndpointsSetup->RdmaHandler->HandleConnected();

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaService->RequestCount);

        env.EndpointsSetup->RdmaHandler->HandleDisconnected();

        const auto grpcRequests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaService->RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(
            grpcRequests + 1,
            env.GrpcClient->Service->RequestCount);
    }

    Y_UNIT_TEST(ShouldHoldDataOnGrpcUntilRdmaSettles)
    {
        // the settle time has to travel from the proto all the way into the
        // switcher, so drive it through a real connection
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, true, 10000);

        auto connection = env.Connect("host-a");

        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);
        env.EndpointsSetup->RdmaHandler->HandleConnected();

        // the first connect moves the data over at once, so make the link drop
        // and come back - only then does the settle time apply
        env.EndpointsSetup->RdmaHandler->HandleDisconnected();
        env.EndpointsSetup->RdmaHandler->HandleConnected();

        auto grpcRequests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(0, env.RdmaService->RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(
            grpcRequests + 1,
            env.GrpcClient->Service->RequestCount);

        env.Timer->AdvanceTime(TDuration::Seconds(10));
        env.Scheduler->AdvanceTime(TDuration::Seconds(10));
        env.Scheduler->RunAllScheduledTasks();

        grpcRequests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaService->RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(
            grpcRequests,
            env.GrpcClient->Service->RequestCount);
    }
}

}   // namespace NCloud::NBlockStore::NCells
