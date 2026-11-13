#include "host_discovery.h"

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestFetcher: public IEdsHostFetcher
{
    TVector<TString> Hosts;
    NProto::TError Error;
    TVector<TVector<TString>> Asked;

    void Start() override
    {}

    void Stop() override
    {}

    TFuture<TResultOrError<TVector<TString>>> FetchHosts(
        TVector<TString> hostPatterns) override
    {
        Asked.push_back(std::move(hostPatterns));
        if (HasError(Error)) {
            return MakeFuture<TResultOrError<TVector<TString>>>(Error);
        }
        return MakeFuture<TResultOrError<TVector<TString>>>(Hosts);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>(TInstant::Zero());
    std::shared_ptr<TTestFetcher> Fetcher = std::make_shared<TTestFetcher>();
    TCellHostPoolPtr Pool;
    TCellHostDiscoveryPtr Discovery;

    TEnv()
    {
        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        proto.SetGrpcPort(9766);
        proto.AddHosts()->SetFqdn("host-a");
        // nothing to warm, so no endpoint bootstrap is ever asked for
        proto.SetMinCellConnections(0);

        TBootstrap bootstrap;
        bootstrap.Timer = Timer;
        bootstrap.Scheduler = Scheduler;
        bootstrap.Logging = CreateLoggingService("console");

        Pool = std::make_shared<TCellHostPool>(
            std::make_shared<TCellConfig>(std::move(proto)),
            bootstrap);
        Pool->Start();

        Discovery = std::make_shared<TCellHostDiscovery>(
            Pool,
            Fetcher,
            TVector<TString>{"cell-1-*"},
            TDuration::Minutes(5),
            Timer,
            Scheduler,
            bootstrap.Logging);
    }

    THashSet<TString> Hosts() const
    {
        THashSet<TString> hosts;
        for (const auto& status: Pool->GetHostStatuses()) {
            hosts.insert(status.Fqdn);
        }
        return hosts;
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

Y_UNIT_TEST_SUITE(TCellHostDiscoveryTest)
{
    Y_UNIT_TEST(ShouldFeedListedHostsIntoThePool)
    {
        TEnv env;
        env.Fetcher->Hosts = {"host-x", "host-y"};

        env.Discovery->Start();

        UNIT_ASSERT_VALUES_EQUAL(1, env.Fetcher->Asked.size());
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TString>{"cell-1-*"}),
            env.Fetcher->Asked[0]);
        UNIT_ASSERT_EQUAL(
            (THashSet<TString>{"host-a", "host-x", "host-y"}),
            env.Hosts());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            env.Pool->GetCounters().HostsConfigured->Val());
    }

    Y_UNIT_TEST(ShouldFetchAgainAfterTheInterval)
    {
        TEnv env;
        env.Fetcher->Hosts = {"host-x"};
        env.Discovery->Start();

        env.Fetcher->Hosts = {"host-y"};
        env.AdvanceTime(TDuration::Minutes(2));
        UNIT_ASSERT_VALUES_EQUAL(1, env.Fetcher->Asked.size());

        env.AdvanceTime(TDuration::Minutes(3));
        UNIT_ASSERT_VALUES_EQUAL(2, env.Fetcher->Asked.size());
        UNIT_ASSERT_EQUAL((THashSet<TString>{"host-a", "host-y"}), env.Hosts());
    }

    Y_UNIT_TEST(ShouldKeepTheLastListWhenAFetchFails)
    {
        TEnv env;
        env.Fetcher->Hosts = {"host-x"};
        env.Discovery->Start();

        env.Fetcher->Error = MakeError(E_REJECTED, "eds is down");
        env.AdvanceTime(TDuration::Minutes(8));
        const auto asked = env.Fetcher->Asked.size();
        UNIT_ASSERT(asked >= 2);
        UNIT_ASSERT_EQUAL((THashSet<TString>{"host-a", "host-x"}), env.Hosts());

        // and keeps trying
        env.AdvanceTime(TDuration::Minutes(8));
        UNIT_ASSERT(env.Fetcher->Asked.size() > asked);
    }

    Y_UNIT_TEST(ShouldStopFetchingWhenStopped)
    {
        TEnv env;
        env.Discovery->Start();
        env.Discovery->Stop();

        env.AdvanceTime(TDuration::Minutes(10));
        UNIT_ASSERT_VALUES_EQUAL(1, env.Fetcher->Asked.size());
    }

    Y_UNIT_TEST(ShouldKeepTheLastListWhenEdsListsNoHosts)
    {
        TEnv env;
        env.Fetcher->Hosts = {"host-x"};
        env.Discovery->Start();

        env.Fetcher->Hosts = {};
        env.AdvanceTime(TDuration::Minutes(8));
        UNIT_ASSERT(env.Fetcher->Asked.size() >= 2);
        UNIT_ASSERT_EQUAL((THashSet<TString>{"host-a", "host-x"}), env.Hosts());
    }
}

}   // namespace NCloud::NBlockStore::NCells
