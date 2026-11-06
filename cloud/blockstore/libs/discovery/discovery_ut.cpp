#include "discovery.h"

#include "balancing.h"
#include "ban.h"
#include "config.h"
#include "fetch.h"
#include "healthcheck.h"

#include <cloud/blockstore/config/discovery.pb.h>
#include <cloud/blockstore/public/api/protos/discovery.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TDiscoveryConfigPtr Config;

    explicit TEnv(NProto::TDiscoveryServiceConfig config)
    {
        Config = std::make_shared<TDiscoveryConfig>(std::move(config));
    }

    TEnv()
        : TEnv(NProto::TDiscoveryServiceConfig())
    {}
};

TInstanceInfo Instance(TString host, ui16 port)
{
    TInstanceInfo ii;
    ii.Host = std::move(host);
    ii.Port = port;
    ii.IsSecurePort = port >= 10;
    return ii;
}

struct TTestBalancingPolicy
    : IBalancingPolicy
{
    TTestBalancingPolicy(TVector<TString> bestHosts)
        : BestHosts(std::move(bestHosts))
    {
    }

    void Reorder(TInstanceList& instances) override
    {
        with_lock (instances.Lock) {
            for (auto& ii : instances.Instances) {
                auto it = Find(BestHosts.begin(), BestHosts.end(), ii.Host);
                ii.BalancingScore = it == BestHosts.end()
                    ? 0
                    : 1. / (1 + std::distance(BestHosts.begin(), it));
            }

            StableSortBy(instances.Instances, [] (const auto& ii) {
                return -ii.BalancingScore;
            });
        }
    }

    TVector<TString> BestHosts;
};

TString SetupBanFile(std::initializer_list<TString> hosts)
{
    TString filename = "./ban.txt";

    TFile file(filename, EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly);
    TFileOutput(file).Write(JoinSeq("\n", hosts));

    return filename;
}

struct TTestHealthChecker
    : IHealthChecker
{
    NThreading::TPromise<void> Promise = NThreading::NewPromise();

    NThreading::TFuture<void> UpdateStatus(TInstanceList& instances) override
    {
        Y_UNUSED(instances);

        return Promise;
    }

    void Start() override
    {
    }

    void Stop() override
    {
    }
};

struct TDelayedHealthChecker
    : TTestHealthChecker
{
    ui32 CompletedChecks = 0;
    ui32 PendingChecks = 0;

    NThreading::TFuture<void> UpdateStatus(TInstanceList& instances) override
    {
        with_lock (instances.Lock) {
            if (instances.Instances.empty()) {
                ++CompletedChecks;
                return NThreading::MakeFuture();
            }
        }

        ++PendingChecks;
        return Promise;
    }
};

struct TCountingInstanceFetcher
    : IInstanceFetcher
{
    ui32 StartCount = 0;
    ui32 StopCount = 0;
    ui32 FetchCount = 0;

    void Start() override
    {
        ++StartCount;
    }

    void Stop() override
    {
        ++StopCount;
    }

    NThreading::TFuture<void> FetchInstances(TInstanceList& instances) override
    {
        Y_UNUSED(instances);
        ++FetchCount;
        return NThreading::MakeFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(DiscoveryTest)
{
    Y_UNIT_TEST(ShouldServeRequest)
    {
        NProto::TDiscoveryServiceConfig config;
        config.SetBannedInstanceListFile(SetupBanFile({
            "bannedhost\t1",
            "besthost\t555"
        }));

        TEnv env(std::move(config));

        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto discovery = CreateDiscoveryService(
            env.Config,
            timer,
            scheduler,
            logging,
            monitoring,
            CreateBanList(env.Config, logging, monitoring),
            CreateInstanceFetcherStub({
                Instance("host1", 1),
                Instance("bannedhost", 1),
                Instance("besthost", 1),
            }),
            CreateInstanceFetcherStub({
                Instance("secondbesthost", 2),
                Instance("host2", 2),
                Instance("host3", 3),
            }),
            CreateInstanceFetcherStub(),
            CreateHealthCheckerStub(),
            std::make_shared<TTestBalancingPolicy>(
                TVector<TString>{
                    "bannedhost",
                    "besthost",
                    "secondbesthost",
                    "host1",
                }
            )
        );

        discovery->Start();

        // instance fetch
        scheduler->RunAllScheduledTasks();
        // healthcheck & balancing
        scheduler->RunAllScheduledTasks();

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL("besthost", response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(1, response.GetInstances(0).GetPort());
        }

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(2);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(2, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL("besthost", response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(1, response.GetInstances(0).GetPort());

            UNIT_ASSERT_VALUES_EQUAL("secondbesthost", response.GetInstances(1).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(2, response.GetInstances(1).GetPort());
        }

        discovery->Stop();

        // TODO: test scenario should be long and diverse
    }

    Y_UNIT_TEST(ShouldServeRequestOnlyAfterFirstHealthCheck)
    {
        TEnv env;

        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        auto hc = std::make_shared<TTestHealthChecker>();

        auto discovery = CreateDiscoveryService(
            env.Config,
            timer,
            scheduler,
            logging,
            monitoring,
            CreateBanListStub(),
            CreateInstanceFetcherStub({Instance("host1", 1)}),
            CreateInstanceFetcherStub({Instance("host2", 2)}),
            CreateInstanceFetcherStub(),
            hc,
            std::make_shared<TTestBalancingPolicy>(TVector<TString>{})
        );

        discovery->Start();

        // instance fetch
        scheduler->RunAllScheduledTasks();
        // healthcheck & balancing
        scheduler->RunAllScheduledTasks();

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(0, response.InstancesSize());
        }

        hc->Promise.SetValue();

        // instance fetch
        scheduler->RunAllScheduledTasks();
        // healthcheck & balancing
        scheduler->RunAllScheduledTasks();

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());
        }

        discovery->Stop();
    }

    Y_UNIT_TEST(ShouldServeEdsOnlyAfterItsFirstHealthCheck)
    {
        TEnv env;
        auto scheduler = std::make_shared<TTestScheduler>();
        auto healthChecker = std::make_shared<TDelayedHealthChecker>();
        auto discovery = CreateDiscoveryService(
            env.Config,
            CreateWallClockTimer(),
            scheduler,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateBanListStub(),
            CreateInstanceFetcherStub(),
            CreateInstanceFetcherStub(),
            CreateInstanceFetcherStub({
                Instance("eds-host", 1),
                Instance("eds-best", 2),
            }),
            healthChecker,
            std::make_shared<TTestBalancingPolicy>(
                TVector<TString>{"eds-best"})
        );

        discovery->Start();
        scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(2, healthChecker->CompletedChecks);
        UNIT_ASSERT_VALUES_EQUAL(1, healthChecker->PendingChecks);

        NProto::TDiscoverInstancesRequest request;
        request.SetLimit(10);
        NProto::TDiscoverInstancesResponse response;
        discovery->ServeRequest(request, &response);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, response.InstancesSize());

        healthChecker->Promise.SetValue();
        response.Clear();
        discovery->ServeRequest(request, &response);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(2, response.InstancesSize());
        UNIT_ASSERT_VALUES_EQUAL("eds-best", response.GetInstances(0).GetHost());
        UNIT_ASSERT_VALUES_EQUAL(2, response.GetInstances(0).GetPort());
        UNIT_ASSERT_VALUES_EQUAL("eds-host", response.GetInstances(1).GetHost());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetInstances(1).GetPort());

        discovery->Stop();
    }

    Y_UNIT_TEST(ShouldMergeEdsWithOtherSourcesAndFilterUniqueEndpoints)
    {
        NProto::TDiscoveryServiceConfig config;
        config.SetBannedInstanceListFile(SetupBanFile({"banned\t4"}));
        TEnv env(std::move(config));
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto plainDual = Instance("dual", 11);
        plainDual.IsSecurePort = false;
        auto discovery = CreateDiscoveryService(
            env.Config,
            CreateWallClockTimer(),
            scheduler,
            logging,
            monitoring,
            CreateBanList(env.Config, logging, monitoring),
            CreateInstanceFetcherStub({
                Instance("overlap", 1),
                Instance("static", 2),
                plainDual,
                Instance("static-tie", 7),
            }),
            CreateInstanceFetcherStub({
                Instance("overlap", 1),
                Instance("conductor", 3),
                Instance("dual", 11),
                Instance("conductor-tie", 8),
                Instance("banned", 4),
            }),
            CreateInstanceFetcherStub({
                Instance("overlap", 1),
                Instance("eds-best", 6),
                Instance("eds-secure", 10),
                Instance("eds-tie", 9),
                Instance("banned", 4),
            }),
            CreateHealthCheckerStub(),
            std::make_shared<TTestBalancingPolicy>(TVector<TString>{
                "banned",
                "eds-best",
                "overlap",
                "eds-secure",
                "conductor",
                "static",
                "dual",
            })
        );

        discovery->Start();
        scheduler->RunAllScheduledTasks();

        auto check = [&] (
            NProto::EDiscoveryPortFilter filter,
            ui32 limit,
            std::initializer_list<std::pair<TString, ui16>> expected)
        {
            NProto::TDiscoverInstancesRequest request;
            request.SetInstanceFilter(filter);
            request.SetLimit(limit);
            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), response.InstancesSize());
            ui32 index = 0;
            for (const auto& [host, port]: expected) {
                UNIT_ASSERT_VALUES_EQUAL(host, response.GetInstances(index).GetHost());
                UNIT_ASSERT_VALUES_EQUAL(port, response.GetInstances(index).GetPort());
                ++index;
            }
        };

        check(NProto::DISCOVERY_ANY_PORT, 20, {
            {"eds-best", 6},
            {"overlap", 1},
            {"eds-secure", 10},
            {"conductor", 3},
            {"static", 2},
            // Plain and secure sources expose the same address in the response.
            {"dual", 11},
            {"conductor-tie", 8},
            {"static-tie", 7},
            {"eds-tie", 9},
        });
        check(NProto::DISCOVERY_INSECURE_PORT, 4, {
            {"eds-best", 6},
            {"overlap", 1},
            {"conductor", 3},
            {"static", 2},
        });
        check(NProto::DISCOVERY_INSECURE_PORT, 20, {
            {"eds-best", 6},
            {"overlap", 1},
            {"conductor", 3},
            {"static", 2},
            {"dual", 11},
            {"conductor-tie", 8},
            {"static-tie", 7},
            {"eds-tie", 9},
        });
        check(NProto::DISCOVERY_SECURE_PORT, 20, {
            {"eds-secure", 10},
            {"dual", 11},
        });

        discovery->Stop();
    }

    Y_UNIT_TEST(ShouldScheduleEdsIndependentlyAndStopItsFetcher)
    {
        NProto::TDiscoveryServiceConfig config;
        config.MutableEds()->SetRequestInterval(3000);
        config.SetConductorRequestInterval(7000);
        config.SetLocalFilesReloadInterval(11000);
        TEnv env(std::move(config));
        auto timer = std::make_shared<TTestTimer>();
        auto scheduler = std::make_shared<TTestScheduler>(timer->Now());
        auto staticFetcher = std::make_shared<TCountingInstanceFetcher>();
        auto conductorFetcher = std::make_shared<TCountingInstanceFetcher>();
        auto edsFetcher = std::make_shared<TCountingInstanceFetcher>();
        auto discovery = CreateDiscoveryService(
            env.Config,
            timer,
            scheduler,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateBanListStub(),
            staticFetcher,
            conductorFetcher,
            edsFetcher,
            CreateHealthCheckerStub(),
            std::make_shared<TTestBalancingPolicy>(TVector<TString>{})
        );

        auto advance = [&] (TDuration duration) {
            timer->AdvanceTime(duration);
            scheduler->AdvanceTime(duration);
            scheduler->RunAllScheduledTasksUntilNow();
        };

        discovery->Start();
        for (const auto& fetcher: {staticFetcher, conductorFetcher, edsFetcher}) {
            UNIT_ASSERT_VALUES_EQUAL(1, fetcher->StartCount);
            UNIT_ASSERT_VALUES_EQUAL(1, fetcher->FetchCount);
        }

        advance(TDuration::MilliSeconds(2999));
        UNIT_ASSERT_VALUES_EQUAL(1, edsFetcher->FetchCount);
        advance(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, edsFetcher->FetchCount);
        UNIT_ASSERT_VALUES_EQUAL(1, conductorFetcher->FetchCount);
        UNIT_ASSERT_VALUES_EQUAL(1, staticFetcher->FetchCount);

        advance(TDuration::MilliSeconds(2999));
        UNIT_ASSERT_VALUES_EQUAL(2, edsFetcher->FetchCount);
        advance(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(3, edsFetcher->FetchCount);
        advance(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(3, edsFetcher->FetchCount);
        UNIT_ASSERT_VALUES_EQUAL(2, conductorFetcher->FetchCount);
        UNIT_ASSERT_VALUES_EQUAL(1, staticFetcher->FetchCount);

        discovery->Stop();
        for (const auto& fetcher: {staticFetcher, conductorFetcher, edsFetcher}) {
            UNIT_ASSERT_VALUES_EQUAL(1, fetcher->StopCount);
        }

        advance(TDuration::Days(1));
        UNIT_ASSERT_VALUES_EQUAL(3, edsFetcher->FetchCount);
        UNIT_ASSERT_VALUES_EQUAL(2, conductorFetcher->FetchCount);
        UNIT_ASSERT_VALUES_EQUAL(1, staticFetcher->FetchCount);
    }

    void ShouldFilterInstancesImpl(NProto::EDiscoveryPortFilter filter)
    {
        NProto::TDiscoveryServiceConfig config;
        config.SetBannedInstanceListFile(SetupBanFile({
            "bannedhost\t1",
            "bannedhost\t10",
            "besthost\t555"
        }));

        TEnv env(std::move(config));

        auto timer = CreateWallClockTimer();
        auto scheduler = std::make_shared<TTestScheduler>();
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto discovery = CreateDiscoveryService(
            env.Config,
            timer,
            scheduler,
            logging,
            monitoring,
            CreateBanList(env.Config, logging, monitoring),
            CreateInstanceFetcherStub({
                Instance("host1", 1),
                Instance("host1", 10),
                Instance("bannedhost", 1),
                Instance("bannedhost", 10),
                Instance("besthost", 1),
                Instance("besthost", 10),
            }),
            CreateInstanceFetcherStub({
                Instance("secondbesthost", 2),
                Instance("secondbesthost", 20),
                Instance("host2", 2),
                Instance("host2", 20),
                Instance("host3", 3),
                Instance("host3", 30),
            }),
            CreateInstanceFetcherStub(),
            CreateHealthCheckerStub(),
            std::make_shared<TTestBalancingPolicy>(
                TVector<TString>{
                    "bannedhost",
                    "besthost",
                    "secondbesthost",
                    "host1",
                }
            )
        );

        discovery->Start();

        // instance fetch
        scheduler->RunAllScheduledTasks();
        // healthcheck & balancing
        scheduler->RunAllScheduledTasks();

        auto checkPort = [&] (int port, int insecurePort) {
            if (filter == NProto::EDiscoveryPortFilter::DISCOVERY_ANY_PORT) {
                return;
            }

            const int expectedPort = filter == NProto::EDiscoveryPortFilter::DISCOVERY_INSECURE_PORT
                ? insecurePort
                : 10*insecurePort;
            UNIT_ASSERT_VALUES_EQUAL(expectedPort, port);
        };

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);
            request.SetInstanceFilter(filter);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL("besthost", response.GetInstances(0).GetHost());

            checkPort(response.GetInstances(0).GetPort(), 1);
        }

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(2);
            request.SetInstanceFilter(filter);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(2, response.InstancesSize());

            if (filter == NProto::EDiscoveryPortFilter::DISCOVERY_ANY_PORT) {
                UNIT_ASSERT_VALUES_EQUAL("besthost", response.GetInstances(0).GetHost());
                UNIT_ASSERT_VALUES_EQUAL("besthost", response.GetInstances(1).GetHost());
            } else {
                UNIT_ASSERT_VALUES_EQUAL("besthost", response.GetInstances(0).GetHost());
                checkPort(response.GetInstances(0).GetPort(), 1);
                UNIT_ASSERT_VALUES_EQUAL("secondbesthost", response.GetInstances(1).GetHost());
                checkPort(response.GetInstances(1).GetPort(), 2);
            }
        }

        discovery->Stop();
    }

    Y_UNIT_TEST(ShouldNoFilterInstances)
    {
        ShouldFilterInstancesImpl(NProto::EDiscoveryPortFilter::DISCOVERY_ANY_PORT);
    }

    Y_UNIT_TEST(ShouldFilterInstancesSecure)
    {
        ShouldFilterInstancesImpl(NProto::EDiscoveryPortFilter::DISCOVERY_SECURE_PORT);
    }

    Y_UNIT_TEST(ShouldFilterInstancesInsecure)
    {
        ShouldFilterInstancesImpl(NProto::EDiscoveryPortFilter::DISCOVERY_INSECURE_PORT);
    }

    Y_UNIT_TEST(ShouldStubWorks)
    {
        const ui16 expectedInsecurePort = 9000;
        const ui16 expectedSecurePort = 9090;
        const TString expectedHost = "hostname";

        auto discovery = CreateDiscoveryServiceStub(
            expectedHost, expectedInsecurePort, expectedSecurePort);

        discovery->Start();

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL(expectedHost, response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(expectedInsecurePort, response.GetInstances(0).GetPort());
        }

        using NProto::EDiscoveryPortFilter;

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);
            request.SetInstanceFilter(EDiscoveryPortFilter::DISCOVERY_INSECURE_PORT);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL(expectedHost, response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(expectedInsecurePort, response.GetInstances(0).GetPort());
        }

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);
            request.SetInstanceFilter(EDiscoveryPortFilter::DISCOVERY_SECURE_PORT);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL(expectedHost, response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(expectedSecurePort, response.GetInstances(0).GetPort());
        }

        discovery->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
