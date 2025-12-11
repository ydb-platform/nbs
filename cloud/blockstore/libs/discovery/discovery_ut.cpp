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

struct TTestBalancingPolicy: IBalancingPolicy
{
    TTestBalancingPolicy(TVector<TString> bestHosts)
        : BestHosts(std::move(bestHosts))
    {}

    void Reorder(TInstanceList& instances) override
    {
        with_lock (instances.Lock) {
            for (auto& ii: instances.Instances) {
                auto it = Find(BestHosts.begin(), BestHosts.end(), ii.Host);
                ii.BalancingScore =
                    it == BestHosts.end()
                        ? 0
                        : 1. / (1 + std::distance(BestHosts.begin(), it));
            }

            StableSortBy(
                instances.Instances,
                [](const auto& ii) { return -ii.BalancingScore; });
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

struct TTestHealthChecker: IHealthChecker
{
    NThreading::TPromise<void> Promise = NThreading::NewPromise();

    NThreading::TFuture<void> UpdateStatus(TInstanceList& instances) override
    {
        Y_UNUSED(instances);

        return Promise;
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(DiscoveryTest)
{
    Y_UNIT_TEST(ShouldServeRequest)
    {
        NProto::TDiscoveryServiceConfig config;
        config.SetBannedInstanceListFile(
            SetupBanFile({"bannedhost\t1", "besthost\t555"}));

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
            CreateHealthCheckerStub(),
            std::make_shared<TTestBalancingPolicy>(TVector<TString>{
                "bannedhost",
                "besthost",
                "secondbesthost",
                "host1",
            }));

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

            UNIT_ASSERT_VALUES_EQUAL(
                "besthost",
                response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(1, response.GetInstances(0).GetPort());
        }

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(2);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(2, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "besthost",
                response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(1, response.GetInstances(0).GetPort());

            UNIT_ASSERT_VALUES_EQUAL(
                "secondbesthost",
                response.GetInstances(1).GetHost());
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
            hc,
            std::make_shared<TTestBalancingPolicy>(TVector<TString>{}));

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

    void ShouldFilterInstancesImpl(NProto::EDiscoveryPortFilter filter)
    {
        NProto::TDiscoveryServiceConfig config;
        config.SetBannedInstanceListFile(
            SetupBanFile({"bannedhost\t1", "bannedhost\t10", "besthost\t555"}));

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
            CreateHealthCheckerStub(),
            std::make_shared<TTestBalancingPolicy>(TVector<TString>{
                "bannedhost",
                "besthost",
                "secondbesthost",
                "host1",
            }));

        discovery->Start();

        // instance fetch
        scheduler->RunAllScheduledTasks();
        // healthcheck & balancing
        scheduler->RunAllScheduledTasks();

        auto checkPort = [&](int port, int insecurePort)
        {
            if (filter == NProto::EDiscoveryPortFilter::DISCOVERY_ANY_PORT) {
                return;
            }

            const int expectedPort =
                filter == NProto::EDiscoveryPortFilter::DISCOVERY_INSECURE_PORT
                    ? insecurePort
                    : 10 * insecurePort;
            UNIT_ASSERT_VALUES_EQUAL(expectedPort, port);
        };

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);
            request.SetInstanceFilter(filter);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "besthost",
                response.GetInstances(0).GetHost());

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
                UNIT_ASSERT_VALUES_EQUAL(
                    "besthost",
                    response.GetInstances(0).GetHost());
                UNIT_ASSERT_VALUES_EQUAL(
                    "besthost",
                    response.GetInstances(1).GetHost());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    "besthost",
                    response.GetInstances(0).GetHost());
                checkPort(response.GetInstances(0).GetPort(), 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    "secondbesthost",
                    response.GetInstances(1).GetHost());
                checkPort(response.GetInstances(1).GetPort(), 2);
            }
        }

        discovery->Stop();
    }

    Y_UNIT_TEST(ShouldNoFilterInstances)
    {
        ShouldFilterInstancesImpl(
            NProto::EDiscoveryPortFilter::DISCOVERY_ANY_PORT);
    }

    Y_UNIT_TEST(ShouldFilterInstancesSecure)
    {
        ShouldFilterInstancesImpl(
            NProto::EDiscoveryPortFilter::DISCOVERY_SECURE_PORT);
    }

    Y_UNIT_TEST(ShouldFilterInstancesInsecure)
    {
        ShouldFilterInstancesImpl(
            NProto::EDiscoveryPortFilter::DISCOVERY_INSECURE_PORT);
    }

    Y_UNIT_TEST(ShouldStubWorks)
    {
        const ui16 expectedInsecurePort = 9000;
        const ui16 expectedSecurePort = 9090;
        const TString expectedHost = "hostname";

        auto discovery = CreateDiscoveryServiceStub(
            expectedHost,
            expectedInsecurePort,
            expectedSecurePort);

        discovery->Start();

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                expectedHost,
                response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedInsecurePort,
                response.GetInstances(0).GetPort());
        }

        using NProto::EDiscoveryPortFilter;

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);
            request.SetInstanceFilter(
                EDiscoveryPortFilter::DISCOVERY_INSECURE_PORT);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                expectedHost,
                response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedInsecurePort,
                response.GetInstances(0).GetPort());
        }

        {
            NProto::TDiscoverInstancesRequest request;
            request.SetLimit(1);
            request.SetInstanceFilter(
                EDiscoveryPortFilter::DISCOVERY_SECURE_PORT);

            NProto::TDiscoverInstancesResponse response;
            discovery->ServeRequest(request, &response);

            UNIT_ASSERT_VALUES_EQUAL(1, response.InstancesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                expectedHost,
                response.GetInstances(0).GetHost());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedSecurePort,
                response.GetInstances(0).GetPort());
        }

        discovery->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
