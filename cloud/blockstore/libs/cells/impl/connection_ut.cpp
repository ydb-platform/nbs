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

#include <util/generic/hash.h>
#include <util/generic/yexception.h>

#include <functional>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore: public TBlockStoreImpl<TTestBlockStore, IBlockStore>
{
    TString TabletHostToReport;
    ui32 RequestCount = 0;

    // the cell id header of the last request served, so a test can check
    // what the connection stamped on it
    TString LastRequestCellId;

    // when set, a mount is answered only once the test says so
    bool DeferMount = false;
    TPromise<NProto::TMountVolumeResponse> MountPromise =
        NewPromise<NProto::TMountVolumeResponse>();

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
        LastRequestCellId = request->GetHeaders().GetCellId();

        typename TMethod::TResponse response;
        if constexpr (std::is_same_v<TMethod, TBlockStoreMountVolumeMethod>) {
            if (DeferMount) {
                return MountPromise.GetFuture();
            }
            response.SetTabletHost(TabletHostToReport);
        }

        return MakeFuture(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpointBootstrap: public ICellHostEndpointBootstrap
{
    // set by the env: a channel belongs to one host, and a test that cannot
    // tell them apart cannot tell a move from staying put
    NClient::IMultiHostClientPtr GrpcClient;

    // counts calls rather than distinct hosts: a pool that kept a channel
    // warm would never call this again for the same fqdn, so a rise here
    // after a release is proof a brand new channel was set up
    ui32 GrpcSetupCallCount = 0;

    // when set, the channel setup for this fqdn throws instead of handing
    // one back - which is what the production path does when the gRPC
    // client cannot make a channel to the host
    TString ThrowForFqdn;

    // when set, the setup for this fqdn hands back a future nobody has
    // resolved yet, so a test can catch a migration mid-flight
    TString DeferForFqdn;
    TPromise<NClient::IMultiClientEndpointPtr> DeferredSetupPromise =
        NewPromise<NClient::IMultiClientEndpointPtr>();

    TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) override
    {
        Y_UNUSED(bootstrap);
        ++GrpcSetupCallCount;

        if (ThrowForFqdn && config.GetFqdn() == ThrowForFqdn) {
            ythrow yexception() << "no channel to " << config.GetFqdn();
        }

        if (DeferForFqdn && config.GetFqdn() == DeferForFqdn) {
            return DeferredSetupPromise.GetFuture();
        }

        return MakeFuture<NClient::IMultiClientEndpointPtr>(
            NClient::CreateMultiClientEndpoint(
                GrpcClient,
                config.GetFqdn(),
                9766,
                false));
    }

    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr RdmaHandler;

    // one service per fqdn: a move can only be told apart from staying put
    // if the host it lands on answers through a different service
    THashMap<TString, std::shared_ptr<TTestBlockStore>> RdmaServices;

    // the host whose rdma endpoint cannot be built, as the real one fails
    // when the rdma client has nothing to give
    TString RefuseRdmaFor;

    // fired synchronously, with the fqdn being set up, from inside the
    // handler-based overload below - lets a test reach into the exact
    // window in which a new host's switcher is being armed, while an
    // abandoned binding may still be alive on the call stack above
    std::function<void(const TString&)> OnRdmaHandlerCreated;

    TRdmaEndpointBootstrapResult SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler) override
    {
        Y_UNUSED(bootstrap);

        if (RefuseRdmaFor && config.GetFqdn() == RefuseRdmaFor) {
            return MakeError(E_REJECTED, "no rdma endpoint for this host");
        }

        RdmaHandler = std::move(handler);

        auto& service = RdmaServices[config.GetFqdn()];
        if (!service) {
            service = std::make_shared<TTestBlockStore>();
        }

        if (OnRdmaHandlerCreated) {
            OnRdmaHandlerCreated(config.GetFqdn());
        }

        return TResultOrError<IBlockStorePtr>(IBlockStorePtr(service));
    }
};

////////////////////////////////////////////////////////////////////////////////

// Forwards to the one shared service, but remembers whose channel it is:
// control requests are otherwise indistinguishable between hosts.
struct TRecordingControlService
    : public TBlockStoreImpl<TRecordingControlService, IBlockStore>
{
    const TString Host;
    const std::shared_ptr<TTestBlockStore> Impl;
    TString* const LastHost;

    TRecordingControlService(
            TString host,
            std::shared_ptr<TTestBlockStore> impl,
            TString* lastHost)
        : Host(std::move(host))
        , Impl(std::move(impl))
        , LastHost(lastHost)
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Impl->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        *LastHost = Host;
        return TMethod::Execute(
            Impl.get(),
            std::move(callContext),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestGrpcClient: public NClient::IMultiHostClient
{
    std::shared_ptr<TTestBlockStore> Service =
        std::make_shared<TTestBlockStore>();

    TString LastControlHost;

    void Start() override
    {}

    void Stop() override
    {}

    IBlockStorePtr
    CreateEndpoint(const TString& host, ui32 port, bool isSecure) override
    {
        Y_UNUSED(port);
        Y_UNUSED(isSecure);
        return std::make_shared<TRecordingControlService>(
            host,
            Service,
            &LastControlHost);
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

    // runs on the thread that completes the mount, which is also the one
    // that acts on the response - a way for a test to get between the two
    std::function<void(const TString&)> OnReported;

    void OnTabletHostChanged(TString fqdn) noexcept override
    {
        Reported.push_back(fqdn);
        if (OnReported) {
            OnReported(fqdn);
        }
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

    // owned by the bootstrap double, kept here under the name the tests use
    THashMap<TString, std::shared_ptr<TTestBlockStore>>& RdmaServices =
        EndpointsSetup->RdmaServices;

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
        ui32 rdmaSettleTimeMs = 0,
        bool hostMigrationEnabled = false,
        // a second place to move to, so that a test can tell "stay where the
        // move put you" from "move again"
        bool spareHost = false)
    {
        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        proto.SetGrpcPort(9766);
        proto.SetTransport(transport);
        proto.SetGrpcDataFallbackEnabled(grpcDataFallback);
        proto.SetRdmaSettleTime(rdmaSettleTimeMs);
        proto.SetHostMigrationEnabled(hostMigrationEnabled);
        proto.AddHosts()->SetFqdn("host-a");
        if (hostMigrationEnabled) {
            // a place to move to once a host is declared dead
            proto.AddHosts()->SetFqdn("host-b");
        }
        if (spareHost) {
            proto.AddHosts()->SetFqdn("host-c");
        }
        CellConfig = std::make_shared<TCellConfig>(std::move(proto));

        EndpointsSetup->GrpcClient = GrpcClient;

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
        UNIT_ASSERT_VALUES_EQUAL(0, env.RdmaServices["host-a"]->RequestCount);

        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);
        env.EndpointsSetup->RdmaHandler->HandleConnected();

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaServices["host-a"]->RequestCount);
    }

    Y_UNIT_TEST(ShouldNotWaitForRdmaWhenGrpcDataFallbackIsDisabled)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false);

        // the endpoint is handed back before it has connected, so the
        // connection is ready at once and its data path is rdma from the
        // start - requests fail retriably until the link comes up
        auto connection = env.Connect("host-a");

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaServices["host-a"]->RequestCount);
    }

    Y_UNIT_TEST(ShouldMoveAwayWhenRdmaWillNotComeUpWithoutFallback)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        auto connection = env.Connect("host-a");
        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);

        // no fallback means this host carries nothing while its rdma is
        // down, so there is no reason to sit on it
        env.EndpointsSetup->RdmaHandler->HandleUnavailable();

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldStayOnRdmaOnlyHostWhenMigrationIsDisabled)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, false);

        auto connection = env.Connect("host-a");
        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);

        env.EndpointsSetup->RdmaHandler->HandleUnavailable();

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldMoveDataBackToGrpcWhenRdmaBreaks)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, true);

        auto connection = env.Connect("host-a");

        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);
        env.EndpointsSetup->RdmaHandler->HandleConnected();

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaServices["host-a"]->RequestCount);

        env.EndpointsSetup->RdmaHandler->HandleDisconnected();

        const auto grpcRequests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaServices["host-a"]->RequestCount);
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
        UNIT_ASSERT_VALUES_EQUAL(0, env.RdmaServices["host-a"]->RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(
            grpcRequests + 1,
            env.GrpcClient->Service->RequestCount);

        env.Timer->AdvanceTime(TDuration::Seconds(10));
        env.Scheduler->AdvanceTime(TDuration::Seconds(10));
        env.Scheduler->RunAllScheduledTasks();

        grpcRequests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaServices["host-a"]->RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(
            grpcRequests,
            env.GrpcClient->Service->RequestCount);
    }

    Y_UNIT_TEST(ShouldMoveToAnotherHostWhenItsHostDies)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        env.Pool->SetHostAlive("host-a", false);

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        const auto requests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(
            requests + 1,
            env.GrpcClient->Service->RequestCount);
    }

    Y_UNIT_TEST(ShouldStayWhenNoLiveHostIsLeft)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        env.Pool->SetHostAlive("host-b", false);
        env.Pool->SetHostAlive("host-a", false);

        // nowhere to go: dropping the connection would lose it entirely
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldReleaseTheChannelOfTheHostItLeft)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        // host-c is discovered rather than configured: the pool only keeps
        // its channel warm for as long as somebody holds it, which makes
        // its fate observable in a way a configured host's channel isn't -
        // PickHost never returns it, so staying connected to it
        // is never mistaken for having released it
        auto connection = env.Connect("host-c");
        UNIT_ASSERT_VALUES_EQUAL("host-c", connection->GetHost());

        // leave only one configured host alive, so the move below is
        // deterministic
        env.Pool->SetHostAlive("host-b", false);

        env.Pool->SetHostAlive("host-c", false);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        const auto setupsBefore = env.EndpointsSetup->GrpcSetupCallCount;

        // a channel the pool still kept warm for host-c would be reused
        // rather than set up again
        env.Pool->AcquireControlChannel("host-c");
        UNIT_ASSERT_VALUES_EQUAL(
            setupsBefore + 1,
            env.EndpointsSetup->GrpcSetupCallCount);
    }

    Y_UNIT_TEST(ShouldNotLetTheOldSwitcherPullDataBack)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, true, 0, true);

        auto connection = env.Connect("host-a");
        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);

        auto staleHandler = env.EndpointsSetup->RdmaHandler;

        // fired synchronously from inside host-b's rdma setup, i.e. while
        // host-b's switcher is being armed - exactly the window in which
        // the abandoned host-a binding may still be alive on the call
        // stack above, so this reaches the real stale switcher and sink
        // instead of ones already destroyed
        env.EndpointsSetup->OnRdmaHandlerCreated =
            [staleHandler](const TString& fqdn)
            {
                if (fqdn == "host-b") {
                    staleHandler->HandleConnected();
                }
            };

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(0, env.RdmaServices["host-a"]->RequestCount);
    }

    Y_UNIT_TEST(ShouldFollowTheTabletHost)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        env.GrpcClient->Service->TabletHostToReport = "host-b";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldFollowTheTabletHostOutsideTheConfig)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        // the tablet host need not be a configured host of the cell: the pool
        // makes a channel for it on demand
        env.GrpcClient->Service->TabletHostToReport = "host-z";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-z", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldStayWhenTheTabletHostIsTheCurrentOne)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        env.GrpcClient->Service->TabletHostToReport = "host-a";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotFollowTheTabletHostWhenMigrationIsDisabled)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, false);

        auto connection = env.Connect("host-a");

        env.GrpcClient->Service->TabletHostToReport = "host-b";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldStayUsableWhenAMoveFails)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        // the only place to go cannot be reached, and the setup says so by
        // throwing - straight through OnHostUnavailable, which is noexcept
        env.EndpointsSetup->ThrowForFqdn = "host-b";
        env.Pool->SetHostAlive("host-a", false);

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // the host we are on is dead, but the connection is whole: requests
        // still go out and come back the way they did before
        const auto requests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(
            requests + 1,
            env.GrpcClient->Service->RequestCount);

        // and a failed attempt does not block the next one: the pinger says
        // the host is gone on every sweep exactly so that this can happen
        env.EndpointsSetup->ThrowForFqdn = "";
        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldFoldTwoReasonsIntoOneBinding)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        // host-z's channel stays unresolved, so the move to it is still in
        // flight when the second reason arrives
        env.EndpointsSetup->DeferForFqdn = "host-z";

        env.GrpcClient->Service->TabletHostToReport = "host-z";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        env.GrpcClient->Service->TabletHostToReport = "host-y";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-z",
                9766,
                false));

        // the second reason was neither lost nor raced with the first: the
        // connection sits where the last one asked for
        UNIT_ASSERT_VALUES_EQUAL("host-y", connection->GetHost());

        // and one binding lived at a time: host-z is a discovered host, so
        // the pool drops its channel the moment the last holder lets go,
        // and a fresh setup call is proof nobody still holds it
        const auto setupsBefore = env.EndpointsSetup->GrpcSetupCallCount;
        env.Pool->AcquireControlChannel("host-z");
        UNIT_ASSERT_VALUES_EQUAL(
            setupsBefore + 1,
            env.EndpointsSetup->GrpcSetupCallCount);
    }

    Y_UNIT_TEST(ShouldNotBeDraggedBackByTheHostItLeft)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        env.GrpcClient->Service->TabletHostToReport = "host-z";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL("host-z", connection->GetHost());

        // the death of the host we left is none of our business any more;
        // still being subscribed to it would send us back into the cell
        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-z", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldIgnoreDeathOfTheHostItAlreadyLeft)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        // leave host-a for a reason of its own, so that host-a stays alive
        // and would be picked again by a move
        env.GrpcClient->Service->TabletHostToReport = "host-b";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // the pool turns its weak watchers into strong ones before dropping
        // its lock, so a move racing that snapshot still gets the call - for
        // a host this connection is no longer on. Acting on it would send us
        // straight back to host-a
        dynamic_cast<ICellHostWatcher&>(*connection)
            .OnHostUnavailable("host-a", env.Pool->GetChannelEpoch("host-a"));

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldIgnoreAMountAnsweredByTheHostItLeft)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        // a mount goes out while we are still on host-a and is held there;
        // the router keeps its target until the request completes
        env.GrpcClient->Service->DeferMount = true;
        auto mount = service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        UNIT_ASSERT(!mount.HasValue());

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // host-a answers at last, naming the tablet host as it saw it. That
        // is news about a world we have already left, and following it would
        // send us back onto the host the pinger just buried
        NProto::TMountVolumeResponse response;
        response.SetTabletHost("host-a");
        env.GrpcClient->Service->MountPromise.SetValue(std::move(response));

        UNIT_ASSERT(mount.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldStayWholeWhenTheNewHostsDataPathCannotBeBuilt)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaServices["host-a"]->RequestCount);

        // host-b will not give us an endpoint, and without a fallback there
        // is no other data path to put in its place
        env.EndpointsSetup->RefuseRdmaFor = "host-b";
        env.Pool->SetHostAlive("host-a", false);

        // nothing moved: a half-done move would leave control on host-b and
        // data still going to host-a
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        UNIT_ASSERT_VALUES_EQUAL("host-a", env.GrpcClient->LastControlHost);

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(2, env.RdmaServices["host-a"]->RequestCount);
        UNIT_ASSERT(!env.RdmaServices.contains("host-b"));
    }

    Y_UNIT_TEST(ShouldNotBounceBetweenHostsWhoseRdmaIsDown)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        auto connection = env.Connect("host-a");
        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);

        env.EndpointsSetup->RdmaHandler->HandleUnavailable();
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // host-b is no better, and host-a has already said what it had to
        // say: going back would start a loop that costs a remount a lap
        env.EndpointsSetup->RdmaHandler->HandleUnavailable();
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotFollowTheTabletHostOntoAnUnusableHost)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        env.EndpointsSetup->RdmaHandler->HandleUnavailable();
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // the tablet is where we came from, and its gRPC answers fine - but
        // its rdma is what drove us away, and following the tablet would
        // undo the move
        env.GrpcClient->Service->TabletHostToReport = "host-a";
        service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldTryAnUnusableHostAgainAfterItsCooldown)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        env.EndpointsSetup->RdmaHandler->HandleUnavailable();
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // what we learned about host-a has an expiry date: a host repaired
        // meanwhile must not be written off for the life of the connection
        env.Timer->AdvanceTime(TDuration::Minutes(2));

        env.GrpcClient->Service->TabletHostToReport = "host-a";
        service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldActOnAMountAnsweredByTheHostItMovedTo)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // the other side of the stale-response guard: what the host we are
        // on now tells us has to be acted on, or a wrong tag would quietly
        // turn every mount into news from nowhere
        env.GrpcClient->Service->TabletHostToReport = "host-z";
        service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-z", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotQueueASecondMoveAwayWhileOneIsRunning)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true, true);

        auto connection = env.Connect("host-a");

        // host-c is out of the running for now, so the first move can only
        // go to host-b - and host-b's channel never resolves, so it stays in
        // flight while the pinger keeps talking
        env.Pool->SetHostAlive("host-c", false);
        env.EndpointsSetup->DeferForFqdn = "host-b";

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // host-b, the move's own target, stays alive so that landing on it
        // is a stable outcome; host-c is revived as the only other live
        // host, so a spurious second "get off host-a" would show up as a
        // move to host-c. host-a is buried again, the repeated signal
        env.Pool->SetHostAlive("host-c", true);
        env.Pool->SetHostAlive("host-a", false);

        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-b",
                9766,
                false));

        // the move that was already running did what the repeated signal
        // asked for; it must not be followed by a move off its own result
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldStayWholeWhenRdmaGivesUpDuringSetup)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        // the rdma client can report the link hopeless the moment it is
        // handed the handler - which is while the connection is still being
        // built and has not installed its first binding yet
        env.EndpointsSetup->OnRdmaHandlerCreated =
            [&](const TString& fqdn)
            {
                if (fqdn == "host-a") {
                    env.EndpointsSetup->RdmaHandler->HandleUnavailable();
                }
            };

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        // whatever the connection decided, it has to have decided it for all
        // of itself: a host, a control path and a data path that disagree
        // are worse than either outcome
        const auto host = connection->GetHost();

        service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        UNIT_ASSERT_VALUES_EQUAL(host, env.GrpcClient->LastControlHost);

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaServices[host]->RequestCount);
    }

    Y_UNIT_TEST(ShouldNotFollowTheTabletHostOntoADeadHost)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // the tablet still lives where the pinger just buried us, and
        // following it would undo the pinger's decision - and then the next
        // failed ping would undo ours, round and round
        env.GrpcClient->Service->TabletHostToReport = "host-a";
        env.Mount(connection);

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldCancelADeferredMoveWhenRdmaComesUpAfterAll)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        // both arrive while the connection is still being built: the link
        // was declared hopeless and then came up anyway
        env.EndpointsSetup->OnRdmaHandlerCreated =
            [&](const TString& fqdn)
            {
                if (fqdn == "host-a") {
                    env.EndpointsSetup->RdmaHandler->HandleUnavailable();
                    env.EndpointsSetup->RdmaHandler->HandleConnected();
                }
            };

        auto connection = env.Connect("host-a");

        // by the time setup finished there was nothing left to run from
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaServices["host-a"]->RequestCount);
    }

    Y_UNIT_TEST(ShouldActOnRdmaGivingUpOnTheHostItIsMovingTo)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true, true);

        auto connection = env.Connect("host-a");

        // host-b says its link is hopeless while the move to it is still
        // being prepared - before the binding that owns it is current
        env.EndpointsSetup->OnRdmaHandlerCreated =
            [&](const TString& fqdn)
            {
                if (fqdn == "host-b") {
                    env.EndpointsSetup->RdmaHandler->HandleUnavailable();
                }
            };

        // host-c is out of the running, so the move can only go to host-b,
        // and host-b's channel is held so that host-c can be back in the
        // running by the time the move lands
        env.Pool->SetHostAlive("host-c", false);
        env.EndpointsSetup->DeferForFqdn = "host-b";

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        env.Pool->SetHostAlive("host-c", true);

        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-b",
                9766,
                false));

        // the signal was neither acted on too early nor dropped: once the
        // move landed on host-b, it took the connection off it
        UNIT_ASSERT_VALUES_EQUAL("host-c", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotReturnToADeadDiscoveredHost)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        // a warm configured channel, so that letting go of the discovered
        // one drops it from the pool entirely
        env.Pool->Start();

        auto connection = env.Connect("host-z");

        env.Pool->SetHostAlive("host-z", false);

        const auto fallback = connection->GetHost();
        UNIT_ASSERT_VALUES_UNEQUAL("host-z", fallback);

        // the pool has forgotten host-z along with its channel, so nobody
        // there remembers it was buried a moment ago - the connection has to
        env.GrpcClient->Service->TabletHostToReport = "host-z";
        env.Mount(connection);

        UNIT_ASSERT_VALUES_EQUAL(fallback, connection->GetHost());
    }

    Y_UNIT_TEST(ShouldStayOnTheHostItMovedToWhenRdmaComesUpThere)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true, true);

        auto connection = env.Connect("host-a");

        // host-b despairs and then comes up, both while the move to it is
        // still being prepared
        env.EndpointsSetup->OnRdmaHandlerCreated =
            [&](const TString& fqdn)
            {
                if (fqdn == "host-b") {
                    env.EndpointsSetup->RdmaHandler->HandleUnavailable();
                    env.EndpointsSetup->RdmaHandler->HandleConnected();
                }
            };

        // host-c is out of the running, so the move can only go to host-b,
        // and host-b's channel is held so that it is still being prepared
        // when the two callbacks land
        env.Pool->SetHostAlive("host-c", false);
        env.EndpointsSetup->DeferForFqdn = "host-b";

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // host-c is back, so leaving host-b would have somewhere to go
        env.Pool->SetHostAlive("host-c", true);

        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-b",
                9766,
                false));

        // by the time host-b became ours there was nothing to run from
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotStrandADeferredMoveBehindANoOpPendingTarget)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true, true);

        auto connection = env.Connect("host-a");

        env.EndpointsSetup->OnRdmaHandlerCreated =
            [&](const TString& fqdn)
            {
                if (fqdn == "host-b") {
                    env.EndpointsSetup->RdmaHandler->HandleUnavailable();
                }
            };

        // host-c is out of the running, so the move can only go to host-b,
        // and host-b's channel is held so that everything below lands while
        // the move is still running
        env.Pool->SetHostAlive("host-c", false);
        env.EndpointsSetup->DeferForFqdn = "host-b";

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // a mount names the very host we are moving to: it queues as a
        // target that will turn out to be a no-op
        env.GrpcClient->Service->TabletHostToReport = "host-b";
        env.Mount(connection);

        env.Pool->SetHostAlive("host-c", true);

        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-b",
                9766,
                false));

        // the queued target was already ours and changed nothing, which must
        // not swallow what host-b's rdma said about itself
        UNIT_ASSERT_VALUES_EQUAL("host-c", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldRunAMoveAwayAskedForDuringSetup)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        env.EndpointsSetup->OnRdmaHandlerCreated =
            [&](const TString& fqdn)
            {
                if (fqdn == "host-a") {
                    env.EndpointsSetup->RdmaHandler->HandleUnavailable();
                }
            };

        auto connection = env.Connect("host-a");

        // staying whole is necessary but not enough: the request was made,
        // and dropping it would leave the connection on a host that said it
        // cannot serve until the rdma client happens to repeat itself
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldKeepThePingersVerdictWhenRdmaComesUpOnTheDeadHost)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        env.Pool->Start();
        auto connection = env.Connect("host-z");

        // host-b is out of the running, so the way off host-z can only be
        // host-a - and host-a's channel is held so that the move is still
        // running when host-z's rdma has its say
        env.Pool->SetHostAlive("host-b", false);
        env.EndpointsSetup->DeferForFqdn = "host-a";

        env.Pool->SetHostAlive("host-z", false);
        UNIT_ASSERT_VALUES_EQUAL("host-z", connection->GetHost());

        // rdma on host-z reconnects meanwhile. That is news about the data
        // path only; the pinger found the control path dead, and nothing
        // rdma says can take that back
        env.EndpointsSetup->RdmaHandler->HandleConnected();

        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-a",
                9766,
                false));
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // the pool has dropped host-z along with its verdict; the tablet
        // still lives there; only this connection's own memory stands in
        // the way of going back
        env.GrpcClient->Service->TabletHostToReport = "host-z";
        env.Mount(connection);

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotLetRdmaCancelAMoveThePingerAskedFor)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true, true);

        auto connection = env.Connect("host-a");

        // a move to host-b is running, and will fail; host-c is held back
        // for now so that nothing else is a candidate until it is time
        env.Pool->SetHostAlive("host-c", false);
        env.EndpointsSetup->DeferForFqdn = "host-b";
        env.GrpcClient->Service->TabletHostToReport = "host-b";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // the pinger buries host-a while the move is running: noted, to be
        // acted on when the move settles
        env.Pool->SetHostAlive("host-a", false);

        // then host-a's rdma gives up and comes back. Rdma coming up takes
        // back what rdma said - and only that: the pinger's verdict stands,
        // whether or not rdma spoke in between
        env.EndpointsSetup->RdmaHandler->HandleUnavailable();
        env.EndpointsSetup->RdmaHandler->HandleConnected();

        env.Pool->SetHostAlive("host-c", true);
        env.Pool->SetHostAlive("host-b", false);

        // the move to host-b fails, so the connection settles back on
        // host-a - where the pinger's request is still waiting
        env.EndpointsSetup->DeferredSetupPromise.SetValue(nullptr);

        UNIT_ASSERT_VALUES_EQUAL("host-c", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldRememberRdmaFailingOnAHostItPassedThrough)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true, true);

        auto connection = env.Connect("host-a");

        // counted, because the end state alone cannot tell "never went back
        // to host-b" from "went back and was bounced off again by the same
        // callback": only the number of arrivals can
        ui32 arrivalsAtB = 0;
        env.EndpointsSetup->OnRdmaHandlerCreated =
            [&](const TString& fqdn)
            {
                if (fqdn == "host-b") {
                    ++arrivalsAtB;
                    env.EndpointsSetup->RdmaHandler->HandleUnavailable();
                }
            };

        // host-c is out of the running, so the move can only go to host-b,
        // whose channel is held so that a second target can queue behind it
        env.Pool->SetHostAlive("host-c", false);
        env.EndpointsSetup->DeferForFqdn = "host-b";

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // the tablet turns out to live on host-c: queued, since a move is
        // running
        env.Pool->SetHostAlive("host-c", true);
        env.GrpcClient->Service->TabletHostToReport = "host-c";
        env.Mount(connection);

        // the move lands on host-b, whose rdma says it is hopeless on the
        // way in; the queued target then carries the connection straight
        // on to host-c, before that could be acted on
        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-b",
                9766,
                false));
        UNIT_ASSERT_VALUES_EQUAL("host-c", connection->GetHost());

        // what host-b said about itself must have outlived the request to
        // leave it, or the tablet naming it again sends us right back
        env.GrpcClient->Service->TabletHostToReport = "host-b";
        env.Mount(connection);

        UNIT_ASSERT_VALUES_EQUAL("host-c", connection->GetHost());
        UNIT_ASSERT_VALUES_EQUAL(1, arrivalsAtB);
    }

    Y_UNIT_TEST(ShouldNotActOnAVerdictThatRanOutWhileItWaited)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true, true);

        auto connection = env.Connect("host-a");

        // a move to host-b is running, and will fail; host-c is held back
        // for now so that nothing else is a candidate until it is time
        env.Pool->SetHostAlive("host-c", false);
        env.EndpointsSetup->DeferForFqdn = "host-b";
        env.GrpcClient->Service->TabletHostToReport = "host-b";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // the pinger buries host-a while the move is running: noted, to be
        // acted on when the move settles
        env.Pool->SetHostAlive("host-a", false);

        // but the move takes so long that host-a recovers and the verdict
        // against it runs out before anything settles
        env.Pool->SetHostAlive("host-a", true);
        env.Timer->AdvanceTime(TDuration::Minutes(2));

        env.Pool->SetHostAlive("host-c", true);
        env.Pool->SetHostAlive("host-b", false);

        // the move to host-b fails, so the connection settles back on
        // host-a - and there is no longer any reason to leave it
        env.EndpointsSetup->DeferredSetupPromise.SetValue(nullptr);

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotLetAMountAnsweredByTheHostItLeftMoveItOnFromTheNext)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        // the pinger buries host-a while a mount host-a answered is being
        // acted on: after the observer has been told, before the move the
        // response asks for is started. host-b is the only place to go
        env.Observer->OnReported = [&](const TString& fqdn)
        {
            if (fqdn == "host-z") {
                env.Pool->SetHostAlive("host-a", false);
            }
        };

        env.GrpcClient->Service->TabletHostToReport = "host-z";
        env.Mount(connection);

        // what host-a said about the tablet must not steer the connection
        // once it is no longer on host-a
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldKeepTheOlderRequestWhenTheNewerBindingIsAbandoned)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true, true);

        auto connection = env.Connect("host-a");

        // a move to host-b is running; its channel is held so that the
        // pinger gets a word in while it is
        env.EndpointsSetup->DeferForFqdn = "host-b";
        env.GrpcClient->Service->TabletHostToReport = "host-b";
        env.Mount(connection);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // the pinger buries host-a meanwhile: noted, to be acted on when
        // the move settles
        env.Pool->SetHostAlive("host-a", false);

        // host-b's rdma gives up the moment it is handed its handler - a
        // request about a binding newer than the one the pinger spoke of -
        // and then that binding cannot be built at all
        env.EndpointsSetup->OnRdmaHandlerCreated =
            [&](const TString& fqdn)
            {
                if (fqdn == "host-b") {
                    env.EndpointsSetup->RdmaHandler->HandleUnavailable();
                    ythrow yexception() << "no rdma endpoint after all";
                }
            };

        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-b",
                9766,
                false));

        // the move was abandoned, so the connection is still on host-a,
        // where the pinger's request still stands: the newer one must not
        // have pushed it out. With host-b held against too, host-c is the
        // only way out
        UNIT_ASSERT_VALUES_EQUAL("host-c", connection->GetHost());
    }
    Y_UNIT_TEST(ShouldMoveOnWhenTheTargetDiesDuringTheMove)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true, true);

        auto connection = env.Connect("host-a");

        // host-c is out of the running, so the move off host-a can only go
        // to host-b; host-b's channel is held so the move stays in flight
        env.Pool->SetHostAlive("host-c", false);
        env.EndpointsSetup->DeferForFqdn = "host-b";

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        // the pinger buries host-b while we are moving onto it - the death
        // lands before the connection subscribes as its watcher, so the
        // notification reaches nobody. host-c is revived as the one live
        // place left to go
        env.Pool->SetHostAlive("host-b", false);
        env.Pool->SetHostAlive("host-c", true);

        env.EndpointsSetup->DeferredSetupPromise.SetValue(
            NClient::CreateMultiClientEndpoint(
                env.GrpcClient,
                "host-b",
                9766,
                false));

        // landing on a host already known dead must not strand the
        // connection there until the next sweep: it moves straight on to
        // host-c
        UNIT_ASSERT_VALUES_EQUAL("host-c", connection->GetHost());
    }
    Y_UNIT_TEST(ShouldNotDropAChannelSharedWithALiveConnectionOnSetupError)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, false, 0, true);

        // a live configured channel, so a discovered host with no references
        // left is eligible to be dropped - which is what makes a stray
        // second release visible
        auto configured = env.Connect("host-a");

        // a live connection onto a discovered host (host-z is not in the
        // cell config), so its channel lives only as long as somebody holds
        // it
        auto connection = env.Connect("host-z");
        UNIT_ASSERT_VALUES_EQUAL(1, env.Pool->GetWatcherCount("host-z"));

        // a second connection onto the same host whose rdma endpoint cannot
        // be built, so its creation fails after the connection object - and
        // the channel - already exist
        env.EndpointsSetup->RefuseRdmaFor = "host-z";
        auto future = env.ConnectAsync("host-z");
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(HasError(future.GetValue()));

        // the failed second connection released the channel exactly once, so
        // the one the first still holds is not dropped out from under it
        UNIT_ASSERT_VALUES_EQUAL(1, env.Pool->GetWatcherCount("host-z"));
    }
    Y_UNIT_TEST(ShouldIgnoreALivenessVerdictAboutAnOlderIncarnationOfItsHost)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true, true);

        // a connection onto a discovered host, whose channel is rebuilt with
        // a fresh epoch every time it is dropped and taken again
        auto connection = env.Connect("host-z");

        const ui64 epoch = env.Pool->GetChannelEpoch("host-z");
        UNIT_ASSERT(epoch > 0);

        // a verdict about an earlier incarnation of host-z (a lower epoch):
        // stale news that a later move back onto a rebuilt host-z has
        // outrun. It must not move the connection off the host it is on now
        dynamic_cast<ICellHostWatcher&>(*connection)
            .OnHostUnavailable("host-z", epoch - 1);

        UNIT_ASSERT_VALUES_EQUAL("host-z", connection->GetHost());
    }
    Y_UNIT_TEST(ShouldStampCellIdOnMountAndUnmount)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC);

        auto connection = env.Connect("host-a");
        auto service = connection->GetService();

        service->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());
        UNIT_ASSERT_VALUES_EQUAL(
            "cell-1",
            env.GrpcClient->Service->LastRequestCellId);

        env.GrpcClient->Service->LastRequestCellId.clear();
        service->UnmountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TUnmountVolumeRequest>());
        UNIT_ASSERT_VALUES_EQUAL(
            "cell-1",
            env.GrpcClient->Service->LastRequestCellId);
    }
}

}   // namespace NCloud::NBlockStore::NCells
