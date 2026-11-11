#include "host_pool.h"

#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TCellConfigPtr MakeCellConfig()
{
    NProto::TCellConfig proto;
    proto.SetCellId("cell-1");
    proto.SetGrpcPort(9766);
    proto.SetRdmaPort(10020);
    proto.SetTransport(NProto::CELL_DATA_TRANSPORT_RDMA);
    proto.AddHosts()->SetFqdn("host-a");
    proto.AddHosts()->SetFqdn("host-b");

    return std::make_shared<TCellConfig>(std::move(proto));
}

////////////////////////////////////////////////////////////////////////////////

struct TPingableService: public TTestService
{
    ui32 Pings = 0;
    NProto::TError PingError;

    // when set, a ping is answered only once the test says so
    bool DeferPing = false;
    TPromise<NProto::TPingResponse> PingPromise =
        NewPromise<NProto::TPingResponse>();
    TString LastClientId;
    // kept alive, not just noted: two requests that do not overlap in time
    // can land on the same address
    std::shared_ptr<NProto::TPingRequest> LastRequest;

    TPingableService()
    {
        PingHandler = [this](std::shared_ptr<NProto::TPingRequest> request)
        {
            ++Pings;
            LastClientId = request->GetHeaders().GetClientId();
            LastRequest = request;

            if (DeferPing) {
                return PingPromise.GetFuture();
            }

            NProto::TPingResponse response;
            *response.MutableError() = PingError;
            return MakeFuture(std::move(response));
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

// Minimal ICellHostEndpointBootstrap: the pool only needs a grpc endpoint
// future to exist, never resolved here. Not part of the brief's pasted
// block - added because AcquireControlChannel dereferences
// Bootstrap.EndpointsSetup, and the existing tests' TBootstrap{} leaves it
// null.
// One service per fqdn, so a test can fail pings for a single host and
// count them per host - a single shared service could not tell hosts apart.
// Deviates from the brief's TTestEndpointBootstrap (a lone `Service`
// field): the brief's ping tests key off
// `EndpointsSetup->Services["host-a"]`, which needs this shape to make
// sense at all.
struct TTestEndpointBootstrap: public ICellHostEndpointBootstrap
{
    THashMap<TString, std::shared_ptr<TPingableService>> Services;

    // one call per channel built: a second call for the same host means the
    // pool had dropped the first one
    THashMap<TString, ui32> SetupCalls;

    // the host whose channel setup throws, as the real gRPC client does when
    // it cannot build a channel
    TString ThrowForFqdn;

    TPingableService& Service(const TString& fqdn)
    {
        auto& service = Services[fqdn];
        if (!service) {
            service = std::make_shared<TPingableService>();
        }
        return *service;
    }

    TResultOrError<IBlockStorePtr> SetupHostIcV2Endpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        NCloud::IClientEndpointHandlerPtr handler) override
    {
        Y_UNUSED(bootstrap);
        Y_UNUSED(config);
        Y_UNUSED(handler);

        return MakeError(E_NOT_IMPLEMENTED, "IC V2 is not used in this test");
    }

    TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) override
    {
        Y_UNUSED(bootstrap);

        ++SetupCalls[config.GetFqdn()];

        if (ThrowForFqdn && config.GetFqdn() == ThrowForFqdn) {
            ythrow yexception() << "no channel to " << config.GetFqdn();
        }

        auto& service = Services[config.GetFqdn()];
        if (!service) {
            service = std::make_shared<TPingableService>();
        }

        return MakeFuture<IBlockStorePtr>(service);
    }

    TRdmaEndpointBootstrapResult SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        NCloud::IClientEndpointHandlerPtr handler) override
    {
        Y_UNUSED(bootstrap);
        Y_UNUSED(config);
        Y_UNUSED(handler);
        return MakeError(E_REJECTED, "not used in host pool tests");
    }
};

////////////////////////////////////////////////////////////////////////////////

TBootstrap MakeBootstrap(std::shared_ptr<TTestEndpointBootstrap> endpoints)
{
    TBootstrap bootstrap;
    bootstrap.EndpointsSetup = std::move(endpoints);
    bootstrap.Logging = CreateLoggingService("console");
    // the pinger schedules itself at start; nobody advances these, so it
    // never runs here
    bootstrap.Timer = std::make_shared<TTestTimer>();
    bootstrap.Scheduler = std::make_shared<TTestScheduler>(TInstant::Zero());
    return bootstrap;
}

////////////////////////////////////////////////////////////////////////////////

struct TPingEnv
{
    std::shared_ptr<TTestEndpointBootstrap> EndpointsSetup =
        std::make_shared<TTestEndpointBootstrap>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>(TInstant::Zero());

    TBootstrap Bootstrap;

    TPingEnv()
    {
        Bootstrap.EndpointsSetup = EndpointsSetup;
        Bootstrap.Timer = Timer;
        Bootstrap.Scheduler = Scheduler;
        Bootstrap.Logging = CreateLoggingService("console");
    }

    // spareHost adds a third configured host that the pool has no room to
    // warm, so that topping up has somewhere to go
    TCellHostPoolPtr MakePool(bool spareHost = false)
    {
        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        proto.SetGrpcPort(9766);
        proto.SetHostPingPeriod(1000);
        proto.SetHostPingTimeout(500);
        proto.AddHosts()->SetFqdn("host-a");
        proto.AddHosts()->SetFqdn("host-b");
        if (spareHost) {
            proto.AddHosts()->SetFqdn("host-c");
        }
        proto.SetMinCellConnections(2);

        auto pool = std::make_shared<TCellHostPool>(
            std::make_shared<TCellConfig>(std::move(proto)),
            Bootstrap);
        pool->Start();
        return pool;
    }

    void AdvanceTime(TDuration duration)
    {
        Timer->AdvanceTime(duration);
        Scheduler->AdvanceTime(duration);
        Scheduler->RunAllScheduledTasksUntilNow();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellHostPoolTest)
{
    Y_UNIT_TEST(ShouldMakeHostConfigForUnlistedFqdn)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        // the tablet host arrives at runtime and need not be in the config;
        // the ports are cell-wide, so the fqdn alone is enough
        auto host = pool.MakeHostConfig("host-z");
        UNIT_ASSERT_VALUES_EQUAL("host-z", host.GetFqdn());
        UNIT_ASSERT_VALUES_EQUAL(9766, host.GetGrpcPort());
        UNIT_ASSERT_VALUES_EQUAL(10020, host.GetRdmaPort());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::CELL_DATA_TRANSPORT_RDMA),
            static_cast<int>(host.GetTransport()));
    }

    Y_UNIT_TEST(ShouldPickOnlyConfiguredHosts)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool.PickHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());

            const auto& fqdn = picked.GetResult().GetFqdn();
            UNIT_ASSERT_C(
                fqdn == "host-a" || fqdn == "host-b",
                "unexpected host " + fqdn);
        }
    }

    Y_UNIT_TEST(ShouldNotPickDeadHost)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        pool.SetHostAlive("host-a", false);
        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool.PickHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_EQUAL("host-b", picked.GetResult().GetFqdn());
        }

        pool.SetHostAlive("host-b", false);
        auto picked = pool.PickHost();
        UNIT_ASSERT(HasError(picked));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, picked.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldReviveHost)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);
        UNIT_ASSERT(HasError(pool.PickHost()));

        pool.SetHostAlive("host-a", true);
        auto picked = pool.PickHost();
        UNIT_ASSERT_C(!HasError(picked), picked.GetError());
        UNIT_ASSERT_VALUES_EQUAL("host-a", picked.GetResult().GetFqdn());
    }

    Y_UNIT_TEST(ShouldIgnoreLivenessOfUnlistedHost)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        // marking a host we have never heard of must not resurrect it into
        // the configured population
        pool.SetHostAlive("host-z", true);
        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);

        UNIT_ASSERT(HasError(pool.PickHost()));
    }

    Y_UNIT_TEST(ShouldHaveLivenessDefaults)
    {
        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        TCellConfig config(std::move(proto));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(5),
            config.GetHostPingPeriod());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(2),
            config.GetHostPingTimeout());
    }

    Y_UNIT_TEST(ShouldNotifyWatchersWhenHostDies)
    {
        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable(
                const TString& fqdn,
                ui64 epoch) noexcept override
            {
                Y_UNUSED(fqdn);
                Y_UNUSED(epoch);
                ++Notifications;
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        auto watcher = std::make_shared<TWatcher>();
        pool.AcquireControlChannel("host-a");
        Y_UNUSED(pool.WatchHost("host-a", watcher));

        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(1, watcher->Notifications);

        // a host that is dead and stays dead keeps saying so: a connection
        // whose migration failed has to get another chance
        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(2, watcher->Notifications);

        pool.SetHostAlive("host-a", true);
        UNIT_ASSERT_VALUES_EQUAL(2, watcher->Notifications);
    }

    Y_UNIT_TEST(ShouldNotNotifyWatchersOfOtherHosts)
    {
        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable(
                const TString& fqdn,
                ui64 epoch) noexcept override
            {
                Y_UNUSED(fqdn);
                Y_UNUSED(epoch);
                ++Notifications;
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        auto watcher = std::make_shared<TWatcher>();
        pool.AcquireControlChannel("host-a");
        Y_UNUSED(pool.WatchHost("host-a", watcher));

        pool.SetHostAlive("host-b", false);
        UNIT_ASSERT_VALUES_EQUAL(0, watcher->Notifications);
    }

    Y_UNIT_TEST(ShouldForgetReleasedWatcher)
    {
        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable(
                const TString& fqdn,
                ui64 epoch) noexcept override
            {
                Y_UNUSED(fqdn);
                Y_UNUSED(epoch);
                ++Notifications;
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        auto watcher = std::make_shared<TWatcher>();
        pool.AcquireControlChannel("host-a");
        Y_UNUSED(pool.WatchHost("host-a", watcher));
        pool.UnwatchHost("host-a", watcher);

        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(0, watcher->Notifications);
    }

    Y_UNIT_TEST(ShouldTrackLivenessOfDiscoveredHost)
    {
        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable(
                const TString& fqdn,
                ui64 epoch) noexcept override
            {
                Y_UNUSED(fqdn);
                Y_UNUSED(epoch);
                ++Notifications;
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        // host-z is not configured: it is the tablet host of somebody's
        // volume, and a connection sits on it all the same
        auto watcher = std::make_shared<TWatcher>();
        pool.AcquireControlChannel("host-z");
        Y_UNUSED(pool.WatchHost("host-z", watcher));

        pool.SetHostAlive("host-z", false);
        UNIT_ASSERT_VALUES_EQUAL(1, watcher->Notifications);
    }

    Y_UNIT_TEST(ShouldNotifyWatchersOutsideTheLock)
    {
        struct TReenteringWatcher: public ICellHostWatcher
        {
            TCellHostPool& Pool;
            TString PickedHost;

            explicit TReenteringWatcher(TCellHostPool& pool)
                : Pool(pool)
            {}

            void OnHostUnavailable(
                const TString& fqdn,
                ui64 epoch) noexcept override
            {
                Y_UNUSED(fqdn);
                Y_UNUSED(epoch);

                // what a real connection does: look for somewhere else to go
                // and take a channel there. Deadlocks if the pool notifies
                // while holding its own lock, which is not recursive
                auto picked = Pool.PickHost();
                if (!HasError(picked)) {
                    PickedHost = picked.GetResult().GetFqdn();
                    Pool.AcquireControlChannel(PickedHost);
                }
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        auto watcher = std::make_shared<TReenteringWatcher>(pool);
        pool.AcquireControlChannel("host-a");
        Y_UNUSED(pool.WatchHost("host-a", watcher));

        pool.SetHostAlive("host-a", false);

        // reaching this line at all is the point: a notification under the
        // lock would have hung above
        UNIT_ASSERT_VALUES_EQUAL("host-b", watcher->PickedHost);
    }

    Y_UNIT_TEST(ShouldPingHostsPeriodically)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->Services["host-a"]->Pings);

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.EndpointsSetup->Services["host-a"]->Pings);
    }

    Y_UNIT_TEST(ShouldMarkHostDeadWhenPingFails)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        env.EndpointsSetup->Services["host-a"]->PingError =
            MakeError(E_REJECTED, "host is down");

        env.AdvanceTime(TDuration::Seconds(1));

        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool->PickHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_EQUAL("host-b", picked.GetResult().GetFqdn());
        }
    }

    Y_UNIT_TEST(ShouldReviveHostWhenPingSucceedsAgain)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        auto& service = env.EndpointsSetup->Services["host-a"];
        service->PingError = MakeError(E_REJECTED, "host is down");
        env.AdvanceTime(TDuration::Seconds(1));

        service->PingError = {};
        env.AdvanceTime(TDuration::Seconds(1));

        bool sawHostA = false;
        for (ui32 i = 0; i < 100; ++i) {
            auto picked = pool->PickHost();
            sawHostA |= picked.GetResult().GetFqdn() == "host-a";
        }
        UNIT_ASSERT_C(sawHostA, "a revived host must be pickable again");
    }

    Y_UNIT_TEST(ShouldStopPingingWhenPoolIsGone)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->Services["host-a"]->Pings);

        pool.reset();

        env.AdvanceTime(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->Services["host-a"]->Pings);
    }

    Y_UNIT_TEST(ShouldStopPingingWhenStopped)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->Services["host-a"]->Pings);

        // the sweep outliving the client it talks to would declare every
        // host dead and move every connection while the server shuts down
        pool->Stop();

        env.AdvanceTime(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->Services["host-a"]->Pings);
    }

    Y_UNIT_TEST(ShouldSendEveryHostItsOwnIdentifiedPing)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        env.AdvanceTime(TDuration::Seconds(1));

        // the pool's endpoint does not go through PrepareRequest, which is
        // where every other cells request gets its client id, and the gRPC
        // client aborts the process on an empty one
        UNIT_ASSERT_VALUES_EQUAL(
            "cell-1",
            env.EndpointsSetup->Services["host-a"]->LastClientId);
        UNIT_ASSERT_VALUES_EQUAL(
            "cell-1",
            env.EndpointsSetup->Services["host-b"]->LastClientId);

        // and one request per host: the endpoint writes the request id and
        // the timestamp into the very request it is handed, so a shared one
        // would be mutated by several rpcs at once
        UNIT_ASSERT(
            env.EndpointsSetup->Services["host-a"]->LastRequest.get() !=
            env.EndpointsSetup->Services["host-b"]->LastRequest.get());
    }

    Y_UNIT_TEST(ShouldDropDestroyedWatcher)
    {
        struct TWatcher: public ICellHostWatcher
        {
            std::shared_ptr<ui32> Notifications;

            explicit TWatcher(std::shared_ptr<ui32> notifications)
                : Notifications(std::move(notifications))
            {}

            void OnHostUnavailable(
                const TString& fqdn,
                ui64 epoch) noexcept override
            {
                Y_UNUSED(fqdn);
                Y_UNUSED(epoch);
                ++*Notifications;
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        auto notifications = std::make_shared<ui32>(0);
        auto watcher = std::make_shared<TWatcher>(notifications);
        pool.AcquireControlChannel("host-a");
        Y_UNUSED(pool.WatchHost("host-a", watcher));
        UNIT_ASSERT_VALUES_EQUAL(1, pool.GetWatcherCount("host-a"));

        // a connection cannot unwatch from its own destructor, so nothing
        // but the pool noticing the expired weak_ptr reclaims the slot - and
        // on a host that keeps answering only the alive path ever runs
        watcher.reset();
        pool.SetHostAlive("host-a", true);
        UNIT_ASSERT_VALUES_EQUAL(0, pool.GetWatcherCount("host-a"));

        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(0, *notifications);
    }

    Y_UNIT_TEST(ShouldDropDiscoveredHostWhileConfiguredOnesLive)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.AcquireControlChannel("host-z");
        pool.ReleaseControlChannel("host-z");

        // the configured hosts are fine, so a host we only heard about once
        // is not worth keeping
        pool.AcquireControlChannel("host-z");
        UNIT_ASSERT_VALUES_EQUAL(2, endpoints->SetupCalls["host-z"]);
    }

    Y_UNIT_TEST(ShouldKeepDiscoveredHostWhenConfiguredOnesAreDead)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        // a host is only known dead once it has a channel to be pinged over
        pool.AcquireControlChannel("host-a");
        pool.AcquireControlChannel("host-b");
        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);

        pool.AcquireControlChannel("host-z");
        pool.ReleaseControlChannel("host-z");

        // now it is the only way back into the cell
        pool.AcquireControlChannel("host-z");
        UNIT_ASSERT_VALUES_EQUAL(1, endpoints->SetupCalls["host-z"]);
    }

    Y_UNIT_TEST(ShouldDropRetainedDiscoveredHostWhenConfiguredOnesRecover)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.AcquireControlChannel("host-a");
        pool.AcquireControlChannel("host-b");
        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);

        // both configured hosts are down, so the discovered host is kept as
        // the only way back into the cell even after its last user lets go
        pool.AcquireControlChannel("host-z");
        pool.ReleaseControlChannel("host-z");

        // a configured host comes back: the retained discovered host has no
        // reason to linger, and is dropped - a fresh acquire builds it anew
        pool.SetHostAlive("host-a", true);

        pool.AcquireControlChannel("host-z");
        UNIT_ASSERT_VALUES_EQUAL(2, endpoints->SetupCalls["host-z"]);
    }

    Y_UNIT_TEST(ShouldTopUpImmediatelyWhenAWarmHostDies)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);

        // three configured hosts, warm minimum two: exactly one is left cold
        // at start, and which two are warmed is up to hash order - so find
        // the spare and a warm victim by what got built
        const TVector<TString> hosts{"host-a", "host-b", "host-c"};
        TString spare;
        TString victim;
        for (const auto& h: hosts) {
            if (env.EndpointsSetup->SetupCalls[h]) {
                victim = h;
            } else {
                spare = h;
            }
        }

        // the victim's ping fails: the sweep that buries it must warm the
        // spare in the same pass, not leave the pool short until the next one
        env.EndpointsSetup->Services[victim]->PingError =
            MakeError(E_REJECTED, "host is down");
        env.AdvanceTime(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->SetupCalls[spare]);
    }

    Y_UNIT_TEST(ShouldCountConnectionsHoldingAHost)
    {
        TPingEnv env;
        // nobody watches the host, but connections still hold a reference to
        // its channel, and that is what the count must reflect
        auto pool = env.MakePool();

        Y_UNUSED(pool->AcquireControlChannel("host-a"));
        Y_UNUSED(pool->AcquireControlChannel("host-a"));

        bool found = false;
        for (const auto& status: pool->GetHostStatuses()) {
            if (status.Fqdn == "host-a") {
                UNIT_ASSERT_VALUES_EQUAL(2, status.Connections);
                found = true;
            }
        }
        UNIT_ASSERT(found);
    }

    Y_UNIT_TEST(ShouldSurviveAChannelSetupThrowDuringASweep)
    {
        TPingEnv env;

        // host-c can never build a channel; with the warm minimum at two,
        // the pool settles on host-a and host-b - whichever order it visits
        // them in, host-c is the one it never keeps - and only reaches for
        // host-c once one of the two dies
        env.EndpointsSetup->ThrowForFqdn = "host-c";
        auto pool = env.MakePool(true);

        // host-a dies, so the next sweep drops below the minimum and tries
        // to top up with host-c, whose setup throws - on the scheduler
        // thread, whose task is noexcept
        pool->SetHostAlive("host-a", false);

        const auto before = env.EndpointsSetup->Services["host-b"]->Pings;
        env.AdvanceTime(TDuration::Seconds(1));

        // the throw neither took down the process nor stopped the pinger:
        // the live host keeps being pinged on the sweeps that follow
        UNIT_ASSERT_VALUES_EQUAL(
            before + 1,
            env.EndpointsSetup->Services["host-b"]->Pings);
    }

    Y_UNIT_TEST(ShouldDropDiscoveredHostOnceItDiesItself)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.AcquireControlChannel("host-a");
        pool.AcquireControlChannel("host-b");
        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);

        pool.AcquireControlChannel("host-z");
        pool.ReleaseControlChannel("host-z");

        // a retained host that died holds a slot without earning it
        pool.SetHostAlive("host-z", false);

        pool.AcquireControlChannel("host-z");
        UNIT_ASSERT_VALUES_EQUAL(2, endpoints->SetupCalls["host-z"]);
    }

    Y_UNIT_TEST(ShouldPickDiscoveredHostOnlyWhenNoConfiguredHostIsLive)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.AcquireControlChannel("host-a");
        pool.AcquireControlChannel("host-b");
        pool.AcquireControlChannel("host-z");

        for (ui32 i = 0; i < 50; ++i) {
            auto picked = pool.PickHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_C(
                picked.GetResult().GetFqdn() != "host-z",
                "a discovered host must not compete with configured ones");
        }

        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);

        auto picked = pool.PickHost();
        UNIT_ASSERT_C(!HasError(picked), picked.GetError());
        UNIT_ASSERT_VALUES_EQUAL("host-z", picked.GetResult().GetFqdn());
    }

    Y_UNIT_TEST(ShouldNotPickDeadDiscoveredHost)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.AcquireControlChannel("host-a");
        pool.AcquireControlChannel("host-b");
        pool.AcquireControlChannel("host-z");

        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);
        pool.SetHostAlive("host-z", false);

        auto picked = pool.PickHost();
        UNIT_ASSERT(HasError(picked));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, picked.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldFallBackToDiscoveredHostsForDescribe)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        auto clientConfig = std::make_shared<NClient::TClientAppConfig>();

        pool.AcquireControlChannel("host-a");
        pool.AcquireControlChannel("host-b");
        pool.AcquireControlChannel("host-z");

        auto configured = pool.GetDescribeEndpoints(clientConfig);
        UNIT_ASSERT_VALUES_EQUAL(1, configured.size());
        UNIT_ASSERT_C(
            !configured[0].GetLogTag().Contains("host-z"),
            "configured hosts come first while they are alive");

        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);

        auto discovered = pool.GetDescribeEndpoints(clientConfig);
        UNIT_ASSERT_VALUES_EQUAL(1, discovered.size());
        UNIT_ASSERT_C(
            discovered[0].GetLogTag().Contains("host-z"),
            "a discovered host is all that is left to ask");
    }

    Y_UNIT_TEST(ShouldWarmAnotherConfiguredHostWhenAWarmOneDies)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);

        // MinCellConnections is 2, so two of the three configured hosts are
        // warmed and the third stays cold
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, env.EndpointsSetup->SetupCalls.size());

        TString warm;
        for (const auto& [fqdn, calls]: env.EndpointsSetup->SetupCalls) {
            warm = fqdn;
            break;
        }

        env.EndpointsSetup->Service(warm).PingError =
            MakeError(E_REJECTED, "host is down");

        env.AdvanceTime(TDuration::Seconds(1));
        env.AdvanceTime(TDuration::Seconds(1));

        // a dead host does not count towards the minimum, so the pool warms
        // the one it had not tried yet
        UNIT_ASSERT_VALUES_EQUAL(3, env.EndpointsSetup->SetupCalls.size());
    }

    Y_UNIT_TEST(ShouldPingWithTheConfiguredTimeout)
    {
        TPingEnv env;

        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        proto.SetGrpcPort(9766);
        proto.SetHostPingPeriod(1000);
        proto.SetHostPingTimeout(5000);
        proto.AddHosts()->SetFqdn("host-a");
        proto.SetMinCellConnections(1);

        auto pool = std::make_shared<TCellHostPool>(
            std::make_shared<TCellConfig>(std::move(proto)),
            env.Bootstrap);
        pool->Start();

        env.AdvanceTime(TDuration::Seconds(1));

        const auto& request = env.EndpointsSetup->Service("host-a").LastRequest;
        UNIT_ASSERT(request);

        // a deadline longer than the period is the operator's business: a
        // channel with a ping still in flight is skipped rather than probed
        // again, so nothing piles up
        UNIT_ASSERT_VALUES_EQUAL(
            5000,
            request->GetHeaders().GetRequestTimeout());
    }

    Y_UNIT_TEST(ShouldIgnorePingsThatOutliveTheStop)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable(
                const TString& fqdn,
                ui64 epoch) noexcept override
            {
                Y_UNUSED(fqdn);
                Y_UNUSED(epoch);
                ++Notifications;
            }
        };

        auto watcher = std::make_shared<TWatcher>();
        Y_UNUSED(pool->WatchHost("host-a", watcher));

        auto& service = env.EndpointsSetup->Service("host-a");
        service.DeferPing = true;

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, service.Pings);

        pool->Stop();

        // the gRPC client goes down right after the pool, so a ping still in
        // flight comes back as an error - which must not read as "the host
        // died" while everything is shutting down
        NProto::TPingResponse response;
        *response.MutableError() = MakeError(E_REJECTED, "client is down");
        service.PingPromise.SetValue(std::move(response));

        UNIT_ASSERT_VALUES_EQUAL(0, watcher->Notifications);
        UNIT_ASSERT_C(
            !pool->IsHostKnownDead("host-a"),
            "the host must not have been buried");
    }

    Y_UNIT_TEST(ShouldIgnoreAPingFromAChannelThatWasRebuilt)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        pool->AcquireControlChannel("host-z");

        auto& service = env.EndpointsSetup->Service("host-z");
        service.DeferPing = true;

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, service.Pings);

        // the connection leaves host-z and comes straight back to it, so the
        // channel the ping went out on is gone and another stands in its
        // place
        pool->ReleaseControlChannel("host-z");
        pool->AcquireControlChannel("host-z");
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.EndpointsSetup->SetupCalls["host-z"]);

        NProto::TPingResponse response;
        *response.MutableError() = MakeError(E_REJECTED, "host is down");
        service.PingPromise.SetValue(std::move(response));

        // only live configured hosts stand between us and host-z
        pool->SetHostAlive("host-a", false);
        pool->SetHostAlive("host-b", false);

        auto picked = pool->PickHost();
        UNIT_ASSERT_C(!HasError(picked), picked.GetError());
        UNIT_ASSERT_VALUES_EQUAL("host-z", picked.GetResult().GetFqdn());
    }

    Y_UNIT_TEST(ShouldKeepOnlyOnePingPerChannelInFlight)
    {
        TPingEnv env;
        auto pool = env.MakePool();

        auto& service = env.EndpointsSetup->Service("host-a");
        service.DeferPing = true;

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, service.Pings);

        // the first answer is still on its way, and a second probe would
        // race it: two answers for one channel can arrive in either order,
        // and the older one would have the last word
        env.AdvanceTime(TDuration::Seconds(1));
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, service.Pings);

        NProto::TPingResponse response;
        service.PingPromise.SetValue(std::move(response));

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, service.Pings);
    }

    Y_UNIT_TEST(ShouldCountHostsFoundDead)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});
        const auto& counters = pool.GetCounters();
        UNIT_ASSERT_VALUES_EQUAL(2, counters.HostsConfigured->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.HostsUnavailable->Val());

        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HostsUnavailable->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HostBecameUnavailable->Val());

        // the pinger says it on every sweep; only the change is an event
        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HostBecameUnavailable->Val());

        pool.SetHostAlive("host-a", true);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.HostsUnavailable->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HostBecameAvailable->Val());
    }

    // Which of the three configured hosts get the two warm channels follows
    // the hash order of the config, so a test asks rather than assumes.
    struct TWarmth
    {
        TVector<TString> Warm;
        TString Cold;
    };

    TWarmth SortByWarmth(const TCellHostPool& pool)
    {
        TWarmth result;
        for (const auto& status: pool.GetHostStatuses()) {
            if (status.Warm) {
                result.Warm.push_back(status.Fqdn);
            } else {
                result.Cold = status.Fqdn;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(2, result.Warm.size());
        UNIT_ASSERT(result.Cold);
        return result;
    }

    EHostLiveness LivenessOf(const TCellHostPool& pool, const TString& fqdn)
    {
        for (const auto& status: pool.GetHostStatuses()) {
            if (status.Fqdn == fqdn) {
                return status.Liveness;
            }
        }
        UNIT_FAIL("no status for " + fqdn);
        return EHostLiveness::Unknown;
    }

    Y_UNIT_TEST(ShouldStartUnknownUntilPinged)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);
        const auto hosts = SortByWarmth(*pool);

        // nothing has answered yet, so nothing is vouched for
        for (const auto& status: pool->GetHostStatuses()) {
            UNIT_ASSERT_EQUAL_C(
                EHostLiveness::Unknown,
                status.Liveness,
                status.Fqdn);
        }

        env.AdvanceTime(TDuration::Seconds(1));

        // only the warm hosts are pinged; the cold one stays unknown
        UNIT_ASSERT_EQUAL(EHostLiveness::Alive, LivenessOf(*pool, hosts.Warm[0]));
        UNIT_ASSERT_EQUAL(EHostLiveness::Alive, LivenessOf(*pool, hosts.Warm[1]));
        UNIT_ASSERT_EQUAL(EHostLiveness::Unknown, LivenessOf(*pool, hosts.Cold));
    }

    Y_UNIT_TEST(ShouldPreferVerifiedHostOnFailover)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);
        const auto hosts = SortByWarmth(*pool);
        env.AdvanceTime(TDuration::Seconds(1));

        // leaving one warm host: the other answered a ping, the cold one
        // never did
        for (ui32 i = 0; i < 50; ++i) {
            auto picked = pool->PickHostExcept({hosts.Warm[0]});
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_EQUAL(
                hosts.Warm[1],
                picked.GetResult().GetFqdn());
        }
    }

    Y_UNIT_TEST(ShouldFallBackToUnverifiedHostsWhenNoneVerified)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);
        const auto hosts = SortByWarmth(*pool);
        env.AdvanceTime(TDuration::Seconds(1));

        for (const auto& fqdn: hosts.Warm) {
            env.EndpointsSetup->Service(fqdn).PingError =
                MakeError(E_REJECTED, "host is down");
        }
        env.AdvanceTime(TDuration::Seconds(1));

        // the cold host was warmed in their place but has not been pinged yet
        UNIT_ASSERT_EQUAL(EHostLiveness::Unknown, LivenessOf(*pool, hosts.Cold));
        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool->PickHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_EQUAL(hosts.Cold, picked.GetResult().GetFqdn());
        }
    }

    Y_UNIT_TEST(ShouldPreferVerifiedDiscoveredHostOverUnknownConfiguredOnFailover)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);
        const auto hosts = SortByWarmth(*pool);
        env.AdvanceTime(TDuration::Seconds(1));

        // a tablet host this cell never listed; its channel gets pinged too
        pool->AcquireControlChannel("host-z");
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL(EHostLiveness::Alive, LivenessOf(*pool, "host-z"));

        for (const auto& fqdn: hosts.Warm) {
            env.EndpointsSetup->Service(fqdn).PingError =
                MakeError(E_REJECTED, "host is down");
        }
        env.AdvanceTime(TDuration::Seconds(1));

        // the cold host is configured but only just warmed; host-z is known
        // to answer
        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool->PickHostExcept({hosts.Warm[0]});
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_EQUAL("host-z", picked.GetResult().GetFqdn());
        }

        // once the cold host answers, the configured list is back in charge
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL(EHostLiveness::Alive, LivenessOf(*pool, hosts.Cold));
        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool->PickHostExcept({hosts.Warm[0]});
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_EQUAL(hosts.Cold, picked.GetResult().GetFqdn());
        }
    }

    Y_UNIT_TEST(ShouldNotCountFirstPingAsRecovery)
    {
        TPingEnv env;
        auto pool = env.MakePool();
        const auto& counters = pool->GetCounters();

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(0, counters.HostBecameAvailable->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.HostBecameUnavailable->Val());

        auto& service = env.EndpointsSetup->Service("host-a");
        service.PingError = MakeError(E_REJECTED, "host is down");
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HostBecameUnavailable->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HostsUnavailable->Val());

        service.PingError = {};
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HostBecameAvailable->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.HostsUnavailable->Val());
    }

    Y_UNIT_TEST(ShouldKeepRetainedDiscoveredHostOnItsOwnFirstAnswer)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        // no Start(): which configured host it would warm is up to hash order
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));

        pool.AcquireControlChannel("host-a");
        pool.SetHostAlive("host-a", false);

        // the only other channel is dead, so the discovered host is retained
        pool.AcquireControlChannel("host-z");
        pool.ReleaseControlChannel("host-z");

        // a describe opens a channel to host-b, which nobody has pinged yet
        auto clientConfig = std::make_shared<NClient::TClientAppConfig>();
        Y_UNUSED(pool.GetDescribeEndpoints(clientConfig));

        // host-z answers its first ping: an unverified host-b is no reason to
        // let go of the one host known to work
        pool.SetHostAlive("host-z", true);
        pool.SetHostAlive("host-b", false);

        pool.AcquireControlChannel("host-z");
        UNIT_ASSERT_VALUES_EQUAL(1, endpoints->SetupCalls["host-z"]);
    }

    Y_UNIT_TEST(ShouldPlaceNewConnectionsOnUnverifiedConfiguredHostsToo)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);
        const auto hosts = SortByWarmth(*pool);
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL(EHostLiveness::Unknown, LivenessOf(*pool, hosts.Cold));

        // a verified discovered host must not pull placements off the
        // configured list either
        pool->AcquireControlChannel("host-z");
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL(EHostLiveness::Alive, LivenessOf(*pool, "host-z"));

        // nothing is down, so a placement is spread over every configured
        // host: a cold one that is never picked would never be pinged, and
        // so never verified
        THashSet<TString> seen;
        for (ui32 i = 0; i < 200; ++i) {
            auto picked = pool->PickHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            seen.insert(picked.GetResult().GetFqdn());
        }
        UNIT_ASSERT(seen.contains(hosts.Cold));
        UNIT_ASSERT(seen.contains(hosts.Warm[0]));
        UNIT_ASSERT(seen.contains(hosts.Warm[1]));
        UNIT_ASSERT(!seen.contains("host-z"));
    }

    Y_UNIT_TEST(ShouldPlaceOnHostsLearnedFromDiscovery)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.UpdateConfiguredHosts({"host-a", "host-x"});
        UNIT_ASSERT_VALUES_EQUAL(3, pool.GetCounters().HostsConfigured->Val());

        // the static list stays whatever discovery says
        THashSet<TString> seen;
        for (ui32 i = 0; i < 200; ++i) {
            auto picked = pool.PickHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            seen.insert(picked.GetResult().GetFqdn());
        }
        UNIT_ASSERT(seen.contains("host-a"));
        UNIT_ASSERT(seen.contains("host-b"));
        UNIT_ASSERT(seen.contains("host-x"));
    }

    Y_UNIT_TEST(ShouldForgetAColdHostDiscoveryNoLongerLists)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.UpdateConfiguredHosts({"host-x"});
        pool.UpdateConfiguredHosts({});

        // nothing was ever built for it and nobody held it
        for (const auto& status: pool.GetHostStatuses()) {
            UNIT_ASSERT_VALUES_UNEQUAL("host-x", status.Fqdn);
        }
        UNIT_ASSERT_VALUES_EQUAL(2, pool.GetCounters().HostsConfigured->Val());
    }

    Y_UNIT_TEST(ShouldKeepAHeldHostDiscoveryNoLongerListsUntilReleased)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.UpdateConfiguredHosts({"host-x"});
        pool.AcquireControlChannel("host-x");
        pool.UpdateConfiguredHosts({});

        // still there while a connection holds it, but no longer where new
        // connections go while a configured host is available
        bool present = false;
        for (const auto& status: pool.GetHostStatuses()) {
            present |= status.Fqdn == "host-x";
        }
        UNIT_ASSERT(present);
        for (ui32 i = 0; i < 50; ++i) {
            auto picked = pool.PickHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_UNEQUAL("host-x", picked.GetResult().GetFqdn());
        }

        // let go, it is dropped like any discovered host
        pool.ReleaseControlChannel("host-x");
        for (const auto& status: pool.GetHostStatuses()) {
            UNIT_ASSERT_VALUES_UNEQUAL("host-x", status.Fqdn);
        }
    }

    Y_UNIT_TEST(ShouldWarmAHostLearnedFromDiscoveryWhenShortOfLiveChannels)
    {
        auto endpoints = std::make_shared<TTestEndpointBootstrap>();
        TCellHostPool pool(MakeCellConfig(), MakeBootstrap(endpoints));
        pool.Start();

        pool.AcquireControlChannel("host-a");
        pool.AcquireControlChannel("host-b");
        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);

        // the only hosts left are dead, so the new one fills the warm minimum
        pool.UpdateConfiguredHosts({"host-x"});
        UNIT_ASSERT_VALUES_EQUAL(1, endpoints->SetupCalls["host-x"]);
    }
}

}   // namespace NCloud::NBlockStore::NCells
