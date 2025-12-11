#include "fetch.h"

#include "config.h"
#include "test_conductor.h"

#include <cloud/blockstore/config/discovery.pb.h>
#include <cloud/blockstore/public/api/protos/discovery.pb.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/string/printf.h>

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TTempDir TempDir;
    TDiscoveryConfigPtr Config;
    std::unique_ptr<TFakeConductor> Conductor;
    TString ConductorUrl;
    ITimerPtr Timer;
    std::shared_ptr<TTestScheduler> Scheduler;

    TEnv(bool withConductor, bool withSecurePorts = false)
        : Timer(CreateWallClockTimer())
        , Scheduler(std::make_shared<TTestScheduler>())
    {
        NProto::TDiscoveryServiceConfig c;
        c.SetInstanceListFile(TempDir.Path() / "instances.txt");
        *c.AddConductorGroups() = "group1";
        *c.AddConductorGroups() = "group2";
        *c.AddConductorGroups() = "group3";
        *c.AddConductorGroups() = "broken_group";

        if (withConductor) {
            int attempts = 10;
            while (true) {
                ui16 port = 1024 + RandomNumber<ui16>(65536 - 1024);
                try {
                    ConductorUrl = Sprintf("http://*:%u/somepath/", port);
                    Conductor.reset(new TFakeConductor(port));
                    Conductor->ForkStart();
                    c.SetConductorApiUrl(ConductorUrl);
                    c.SetConductorInstancePort(1);

                    if (withSecurePorts) {
                        c.SetConductorSecureInstancePort(10);
                    }

                    break;
                } catch (...) {
                    Cdbg << "failed to start conductor mock on port " << port
                         << ", error: " << CurrentExceptionMessage() << Endl;

                    if (--attempts == 0) {
                        throw;
                    }
                }
            }
        }

        Config = std::make_shared<TDiscoveryConfig>(std::move(c));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(FetchTest)
{
    struct TInstanceSpec
    {
        TString Host;
        ui16 Port;
    };

    using TInitInstanceSource =
        std::function<void(const TVector<TInstanceSpec>& instances)>;
    using TGetInstancePort = std::function<int(int)>;

    void DoTestFetch(
        IInstanceFetcher & fetcher,
        const TInitInstanceSource& initInstanceSource,
        bool withPorts)
    {
        const TVector<TInstanceSpec> stage1 = {
            {"host1", 1},
            {"host2", 2},
            {"host3", 3},
            {"host4", 4},
        };

        const TVector<TInstanceSpec> stage2 = {
            {"host1", 1},
            {"host3", 3},
            {"host4", 4},
            {"host5", 5},
            {"host6", 6},
        };

        auto port = [=](ui32 i)
        {
            return withPorts ? i : 1;
        };

        fetcher.Start();

        // initializing instance list
        initInstanceSource(stage1);

        // fetching instances for the first time
        TInstanceList instances;
        fetcher.FetchInstances(instances).Wait();

        // order of newly added instances is unspecified
        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [](const TInstanceInfo& ii) { return ii.Host; });

        // checking hosts:ports
        UNIT_ASSERT_VALUES_EQUAL(4, instances.Instances.size());

        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(1), instances.Instances[0].Port);

        UNIT_ASSERT_VALUES_EQUAL("host2", instances.Instances[1].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(2), instances.Instances[1].Port);

        UNIT_ASSERT_VALUES_EQUAL("host3", instances.Instances[2].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(3), instances.Instances[2].Port);

        UNIT_ASSERT_VALUES_EQUAL("host4", instances.Instances[3].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(4), instances.Instances[3].Port);

        // all new instances should be marked as unreachable
        for (const auto& ii: instances.Instances) {
            UNIT_ASSERT(ii.Status == TInstanceInfo::EStatus::Unreachable);
        }

        instances.Instances[0].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[0].BalancingScore = 0.2;
        instances.Instances[2].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[2].BalancingScore = 0.1;

        // deleting host2:2 and adding host5:5 and host6:6
        initInstanceSource(stage2);

        // refetching instances
        fetcher.FetchInstances(instances).Wait();

        // checking that existing instances are in the beginning of the
        // list and that their relative order is untouched
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(1), instances.Instances[0].Port);

        UNIT_ASSERT_VALUES_EQUAL("host3", instances.Instances[1].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(3), instances.Instances[1].Port);

        UNIT_ASSERT_VALUES_EQUAL("host4", instances.Instances[2].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(4), instances.Instances[2].Port);

        // order of newly added instances is unspecified
        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [](const TInstanceInfo& ii) { return ii.Host; });

        UNIT_ASSERT_VALUES_EQUAL(5, instances.Instances.size());

        // already existing instances should be kept intact
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(1), instances.Instances[0].Port);
        UNIT_ASSERT_VALUES_EQUAL(0.2, instances.Instances[0].BalancingScore);
        UNIT_ASSERT(
            instances.Instances[0].Status == TInstanceInfo::EStatus::Reachable);

        UNIT_ASSERT_VALUES_EQUAL("host3", instances.Instances[1].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(3), instances.Instances[1].Port);
        UNIT_ASSERT_VALUES_EQUAL(0.1, instances.Instances[1].BalancingScore);
        UNIT_ASSERT(
            instances.Instances[1].Status == TInstanceInfo::EStatus::Reachable);

        UNIT_ASSERT_VALUES_EQUAL("host4", instances.Instances[2].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(4), instances.Instances[2].Port);
        UNIT_ASSERT_VALUES_EQUAL(0, instances.Instances[2].BalancingScore);
        UNIT_ASSERT(
            instances.Instances[2].Status ==
            TInstanceInfo::EStatus::Unreachable);

        // newly added instances should be marked as unreachable
        UNIT_ASSERT_VALUES_EQUAL("host5", instances.Instances[3].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(5), instances.Instances[3].Port);
        UNIT_ASSERT_VALUES_EQUAL(0, instances.Instances[3].BalancingScore);
        UNIT_ASSERT(
            instances.Instances[3].Status ==
            TInstanceInfo::EStatus::Unreachable);

        UNIT_ASSERT_VALUES_EQUAL("host6", instances.Instances[4].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(6), instances.Instances[4].Port);
        UNIT_ASSERT_VALUES_EQUAL(0, instances.Instances[4].BalancingScore);
        UNIT_ASSERT(
            instances.Instances[4].Status ==
            TInstanceInfo::EStatus::Unreachable);

        fetcher.Stop();
    }

    void DoTestFetchSecure(
        IInstanceFetcher & fetcher,
        const TInitInstanceSource& initInstanceSource,
        const TGetInstancePort& port)
    {
        const TVector<TInstanceSpec> stage1 = {
            {"host1", 1},
            {"host2", 2},
            {"host3", 3},
            {"host4", 4},
        };

        const TVector<TInstanceSpec> stage2 = {
            {"host1", 1},
            {"host3", 3},
            {"host4", 4},
            {"host5", 5},
            {"host6", 6},
        };

        using TInstSpec =
            std::tuple<TStringBuf, int, double, TInstanceInfo::EStatus>;

        auto checkInstances =
            [&](auto& instances, std::initializer_list<TInstSpec> expected)
        {
            UNIT_ASSERT_VALUES_EQUAL(instances.size(), expected.size());

            SortBy(
                instances,
                [](const auto& ii) { return std::tie(ii.Host, ii.Port); });

            auto it = expected.begin();
            for (const auto& ii: instances) {
                UNIT_ASSERT_VALUES_EQUAL(std::get<0>(*it), ii.Host);
                UNIT_ASSERT_VALUES_EQUAL(std::get<1>(*it), ii.Port);

                UNIT_ASSERT(ii.IsSecurePort == ii.Port >= 10);

                UNIT_ASSERT_VALUES_EQUAL(std::get<2>(*it), ii.BalancingScore);
                UNIT_ASSERT(ii.Status == std::get<3>(*it));

                ++it;
            }
        };

        fetcher.Start();

        initInstanceSource(stage1);

        TInstanceList instances;
        fetcher.FetchInstances(instances).Wait();

        checkInstances(
            instances.Instances,
            {
                {"host1", port(1), 0.0, TInstanceInfo::EStatus::Unreachable},
                {"host1", port(10), 0.0, TInstanceInfo::EStatus::Unreachable},
                {"host2", port(2), 0.0, TInstanceInfo::EStatus::Unreachable},
                {"host2", port(20), 0.0, TInstanceInfo::EStatus::Unreachable},
                {"host3", port(3), 0.0, TInstanceInfo::EStatus::Unreachable},
                {"host3", port(30), 0.0, TInstanceInfo::EStatus::Unreachable},
                {"host4", port(4), 0.0, TInstanceInfo::EStatus::Unreachable},
                {"host4", port(40), 0.0, TInstanceInfo::EStatus::Unreachable},
            });

        instances.Instances[0].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[0].BalancingScore = 0.4;
        instances.Instances[1].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[1].BalancingScore = 0.3;

        instances.Instances[4].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[4].BalancingScore = 0.2;
        instances.Instances[5].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[5].BalancingScore = 0.1;

        initInstanceSource(stage2);

        fetcher.FetchInstances(instances).Wait();

        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(1), instances.Instances[0].Port);
        UNIT_ASSERT(!instances.Instances[0].IsSecurePort);

        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[1].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(10), instances.Instances[1].Port);
        UNIT_ASSERT(instances.Instances[1].IsSecurePort);

        UNIT_ASSERT_VALUES_EQUAL("host3", instances.Instances[2].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(3), instances.Instances[2].Port);
        UNIT_ASSERT(!instances.Instances[2].IsSecurePort);

        UNIT_ASSERT_VALUES_EQUAL("host3", instances.Instances[3].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(30), instances.Instances[3].Port);
        UNIT_ASSERT(instances.Instances[3].IsSecurePort);

        UNIT_ASSERT_VALUES_EQUAL("host4", instances.Instances[4].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(4), instances.Instances[4].Port);
        UNIT_ASSERT(!instances.Instances[4].IsSecurePort);

        UNIT_ASSERT_VALUES_EQUAL("host4", instances.Instances[5].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(40), instances.Instances[5].Port);
        UNIT_ASSERT(instances.Instances[5].IsSecurePort);

        checkInstances(
            instances.Instances,
            {{"host1", port(1), 0.4, TInstanceInfo::EStatus::Reachable},
             {"host1", port(10), 0.3, TInstanceInfo::EStatus::Reachable},
             {"host3", port(3), 0.2, TInstanceInfo::EStatus::Reachable},
             {"host3", port(30), 0.1, TInstanceInfo::EStatus::Reachable},

             {"host4", port(4), 0.0, TInstanceInfo::EStatus::Unreachable},
             {"host4", port(40), 0.0, TInstanceInfo::EStatus::Unreachable},
             {"host5", port(5), 0.0, TInstanceInfo::EStatus::Unreachable},
             {"host5", port(50), 0.0, TInstanceInfo::EStatus::Unreachable},
             {"host6", port(6), 0.0, TInstanceInfo::EStatus::Unreachable},
             {"host6", port(60), 0.0, TInstanceInfo::EStatus::Unreachable}});

        fetcher.Stop();
    }

    Y_UNIT_TEST(ShouldFetchFromFile)
    {
        TEnv env(false);

        auto fetcher = CreateStaticInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub());

        DoTestFetch(
            *fetcher,
            [&](const TVector<TInstanceSpec>& instanceSpecs)
            {
                TOFStream of(env.Config->GetInstanceListFile());
                for (const auto& x: instanceSpecs) {
                    of << x.Host << "\t" << x.Port << Endl;
                }
            },
            true);
    }

    Y_UNIT_TEST(ShouldFetchFromFileSecure)
    {
        TEnv env(false);

        auto fetcher = CreateStaticInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub());

        DoTestFetchSecure(
            *fetcher,
            [&](const TVector<TInstanceSpec>& instanceSpecs)
            {
                TOFStream of(env.Config->GetInstanceListFile());
                for (const auto& x: instanceSpecs) {
                    of << x.Host << "\t" << x.Port << "\t" << 10 * x.Port
                       << Endl;
                }
            },
            [](int p) { return p; });
    }

    Y_UNIT_TEST(ShouldFetchFromConductor)
    {
        TEnv env(true);

        auto fetcher = CreateConductorInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            env.Timer,
            env.Scheduler);

        auto group = [](const TString& host)
        {
            return host <= "host2" ? "group1" : "group2";
        };

        DoTestFetch(
            *fetcher,
            [&](const TVector<TInstanceSpec>& instanceSpecs)
            {
                TVector<THostInfo> hostInfo;
                for (const auto& x: instanceSpecs) {
                    hostInfo.push_back({group(x.Host), x.Host});
                }
                env.Conductor->SetHostInfo(std::move(hostInfo));
            },
            false);
    }

    Y_UNIT_TEST(ShouldFetchFromConductorSecure)
    {
        TEnv env(true, true);

        auto fetcher = CreateConductorInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            env.Timer,
            env.Scheduler);

        auto group = [](const TString& host)
        {
            return host <= "host2" ? "group1" : "group2";
        };

        DoTestFetchSecure(
            *fetcher,
            [&](const TVector<TInstanceSpec>& instanceSpecs)
            {
                TVector<THostInfo> hostInfo;
                for (const auto& x: instanceSpecs) {
                    hostInfo.push_back({group(x.Host), x.Host});
                }
                env.Conductor->SetHostInfo(std::move(hostInfo));
            },
            [](int p)
            {
                if (p >= 10) {
                    return 10;
                }

                return 1;
            });
    }

    Y_UNIT_TEST(ShouldProperlyProcessConductorTimeouts)
    {
        TEnv env(true);

        auto fetcher = CreateConductorInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            env.Timer,
            env.Scheduler);

        fetcher->Start();

        TVector<THostInfo> hostInfo = {
            {"group1", "host1"},
            {"group2", "host2"},
        };
        env.Conductor->SetHostInfo(hostInfo);

        TInstanceList instances;
        fetcher->FetchInstances(instances).Wait();

        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [](const TInstanceInfo& ii) { return ii.Host; });

        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("host2", instances.Instances[1].Host);

        env.Conductor->SetDrop("group2", true);
        hostInfo[1].Host = "host22";
        env.Conductor->SetHostInfo(hostInfo);

        auto fetch2 = fetcher->FetchInstances(instances);
        Sleep(TDuration::Seconds(1));
        env.Scheduler->RunAllScheduledTasks();
        fetch2.Wait();

        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [](const TInstanceInfo& ii) { return ii.Host; });

        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("host2", instances.Instances[1].Host);

        env.Conductor->SetDrop("group2", false);

        auto fetch3 = fetcher->FetchInstances(instances);
        fetch3.Wait();

        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [](const TInstanceInfo& ii) { return ii.Host; });

        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("host22", instances.Instances[1].Host);

        fetcher->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
