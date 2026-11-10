#include "fetch.h"

#include "config.h"
#include "test_conductor.h"

#include <cloud/blockstore/config/discovery.pb.h>
#include <cloud/blockstore/public/api/protos/discovery.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
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
        // Successful groups still update instances when this group fails.
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

void CheckFetchResult(
    const NThreading::TFuture<NProto::TError>& future,
    ui32 expectedCode)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    const auto& error = future.GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        expectedCode,
        error.GetCode(),
        error.GetMessage());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(FetchTest)
{
    Y_UNIT_TEST(ShouldReturnEmptyInstancesFromDefaultStub)
    {
        auto fetcher = CreateInstanceFetcherStub();
        fetcher->Start();

        TInstanceList instances;
        TInstanceInfo instance;
        instance.Host = "stale.example.net";
        instance.Port = 9766;
        instances.Instances.push_back(std::move(instance));

        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
        UNIT_ASSERT(instances.Instances.empty());

        fetcher->Stop();
    }

    struct TInstanceSpec
    {
        TString Host;
        ui16 Port;
    };

    using TInitInstanceSource =
        std::function<void(const TVector<TInstanceSpec>& instances)>;
    using TGetInstancePort = std::function<int(int)>;

    void DoTestFetch(
        IInstanceFetcher& fetcher,
        const TInitInstanceSource& initInstanceSource,
        bool withPorts,
        ui32 expectedCode)
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

        auto port = [=] (ui32 i) {
            return withPorts ? i : 1;
        };

        fetcher.Start();

        // initializing instance list
        initInstanceSource(stage1);

        // fetching instances for the first time
        TInstanceList instances;
        CheckFetchResult(fetcher.FetchInstances(instances), expectedCode);

        // order of newly added instances is unspecified
        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [] (const TInstanceInfo& ii) {
                return ii.Host;
            }
        );

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
        for (const auto& ii : instances.Instances) {
            UNIT_ASSERT(ii.Status == TInstanceInfo::EStatus::Unreachable);
        }

        instances.Instances[0].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[0].BalancingScore = 0.2;
        instances.Instances[2].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[2].BalancingScore = 0.1;

        // deleting host2:2 and adding host5:5 and host6:6
        initInstanceSource(stage2);

        // refetching instances
        CheckFetchResult(fetcher.FetchInstances(instances), expectedCode);

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
            [] (const TInstanceInfo& ii) {
                return ii.Host;
            }
        );

        UNIT_ASSERT_VALUES_EQUAL(5, instances.Instances.size());

        // already existing instances should be kept intact
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(1), instances.Instances[0].Port);
        UNIT_ASSERT_VALUES_EQUAL(0.2, instances.Instances[0].BalancingScore);
        UNIT_ASSERT(instances.Instances[0].Status
            == TInstanceInfo::EStatus::Reachable);

        UNIT_ASSERT_VALUES_EQUAL("host3", instances.Instances[1].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(3), instances.Instances[1].Port);
        UNIT_ASSERT_VALUES_EQUAL(0.1, instances.Instances[1].BalancingScore);
        UNIT_ASSERT(instances.Instances[1].Status
            == TInstanceInfo::EStatus::Reachable);

        UNIT_ASSERT_VALUES_EQUAL("host4", instances.Instances[2].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(4), instances.Instances[2].Port);
        UNIT_ASSERT_VALUES_EQUAL(0, instances.Instances[2].BalancingScore);
        UNIT_ASSERT(instances.Instances[2].Status
            == TInstanceInfo::EStatus::Unreachable);

        // newly added instances should be marked as unreachable
        UNIT_ASSERT_VALUES_EQUAL("host5", instances.Instances[3].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(5), instances.Instances[3].Port);
        UNIT_ASSERT_VALUES_EQUAL(0, instances.Instances[3].BalancingScore);
        UNIT_ASSERT(instances.Instances[3].Status
            == TInstanceInfo::EStatus::Unreachable);

        UNIT_ASSERT_VALUES_EQUAL("host6", instances.Instances[4].Host);
        UNIT_ASSERT_VALUES_EQUAL(port(6), instances.Instances[4].Port);
        UNIT_ASSERT_VALUES_EQUAL(0, instances.Instances[4].BalancingScore);
        UNIT_ASSERT(instances.Instances[4].Status
            == TInstanceInfo::EStatus::Unreachable);

        fetcher.Stop();
    }

    void DoTestFetchSecure(
        IInstanceFetcher& fetcher,
        const TInitInstanceSource& initInstanceSource,
        const TGetInstancePort& port,
        ui32 expectedCode)
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

        using TInstSpec = std::tuple<TStringBuf, int, double, TInstanceInfo::EStatus>;

        auto checkInstances = [&] (auto& instances, std::initializer_list<TInstSpec> expected) {
            UNIT_ASSERT_VALUES_EQUAL(instances.size(), expected.size());

            SortBy(instances, [] (const auto& ii) {
                return std::tie(ii.Host, ii.Port);
            });

            auto it = expected.begin();
            for (const auto& ii : instances) {
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
        CheckFetchResult(fetcher.FetchInstances(instances), expectedCode);

        checkInstances(instances.Instances, {
            {"host1", port(1) , 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host1", port(10), 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host2", port(2) , 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host2", port(20), 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host3", port(3) , 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host3", port(30), 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host4", port(4) , 0.0, TInstanceInfo::EStatus::Unreachable},
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

        CheckFetchResult(fetcher.FetchInstances(instances), expectedCode);

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

        checkInstances(instances.Instances, {
            {"host1", port(1) , 0.4, TInstanceInfo::EStatus::Reachable},
            {"host1", port(10), 0.3, TInstanceInfo::EStatus::Reachable},
            {"host3", port(3) , 0.2, TInstanceInfo::EStatus::Reachable},
            {"host3", port(30), 0.1, TInstanceInfo::EStatus::Reachable},

            {"host4", port(4) , 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host4", port(40), 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host5", port(5) , 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host5", port(50), 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host6", port(6) , 0.0, TInstanceInfo::EStatus::Unreachable},
            {"host6", port(60), 0.0, TInstanceInfo::EStatus::Unreachable}
        });

        fetcher.Stop();
    }

    Y_UNIT_TEST(ShouldFetchFromFile)
    {
        TEnv env(false);

        auto fetcher = CreateStaticInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub()
        );

        DoTestFetch(
            *fetcher,
            [&] (const TVector<TInstanceSpec>& instanceSpecs) {
                TOFStream of(env.Config->GetInstanceListFile());
                for (const auto& x : instanceSpecs) {
                    of << x.Host << "\t" << x.Port << Endl;
                }
            },
            true,
            S_OK
        );
    }

    Y_UNIT_TEST(ShouldKeepStaticInstancesOnFileReadErrors)
    {
        TEnv env(false);
        auto fetcher = CreateStaticInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub()
        );
        fetcher->Start();

        {
            TOFStream of(env.Config->GetInstanceListFile());
            of << "old-host\t1\n";
        }
        TInstanceList instances;
        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
        UNIT_ASSERT_VALUES_EQUAL(1, instances.Instances.size());
        instances.Instances[0].Status = TInstanceInfo::EStatus::Reachable;
        instances.Instances[0].BalancingScore = 0.5;
        instances.Instances[0].Tag = "old-state";

        auto checkOldInstance = [&] {
            UNIT_ASSERT_VALUES_EQUAL(1, instances.Instances.size());
            const auto& instance = instances.Instances[0];
            UNIT_ASSERT_VALUES_EQUAL("old-host", instance.Host);
            UNIT_ASSERT_VALUES_EQUAL(1, instance.Port);
            UNIT_ASSERT(instance.Status == TInstanceInfo::EStatus::Reachable);
            UNIT_ASSERT_VALUES_EQUAL(0.5, instance.BalancingScore);
            UNIT_ASSERT_VALUES_EQUAL("old-state", instance.Tag);
        };

        TFsPath(env.Config->GetInstanceListFile()).DeleteIfExists();
        CheckFetchResult(fetcher->FetchInstances(instances), E_IO);
        checkOldInstance();

        TFsPath(env.Config->GetInstanceListFile()).MkDir();
        CheckFetchResult(fetcher->FetchInstances(instances), E_IO);
        checkOldInstance();
        TFsPath(env.Config->GetInstanceListFile()).DeleteIfExists();

        {
            TOFStream of(env.Config->GetInstanceListFile());
            of << "new-host\t2\n";
        }
        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
        UNIT_ASSERT_VALUES_EQUAL(1, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("new-host", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances[0].Port);

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldApplyValidStaticInstancesAndSkipMalformedRows)
    {
        TEnv env(false);
        auto fetcher = CreateStaticInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub()
        );
        fetcher->Start();

        {
            TOFStream of(env.Config->GetInstanceListFile());
            of << "old-host\t1\nretained-host\t2\n";
        }
        TInstanceList instances;
        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
        for (auto& instance: instances.Instances) {
            instance.Status = TInstanceInfo::EStatus::Reachable;
            instance.BalancingScore = 0.5;
            instance.Tag = "old-state";
        }

        {
            TOFStream of(env.Config->GetInstanceListFile());
            of << "\nnew-host\t3\t13\n"
               << "missing-port\n"
               << "invalid-port\tnot-a-port\n"
               << "invalid-secure-port\t4\tbad\n"
               << "\t5\n"
               << "retained-host\t2\n\n";
        }
        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
        SortBy(instances.Instances, [] (const TInstanceInfo& instance) {
            return std::make_pair(instance.Host, instance.Port);
        });

        UNIT_ASSERT_VALUES_EQUAL(3, instances.Instances.size());
        const auto& newInstance = instances.Instances[0];
        UNIT_ASSERT_VALUES_EQUAL("new-host", newInstance.Host);
        UNIT_ASSERT_VALUES_EQUAL(3, newInstance.Port);
        UNIT_ASSERT(!newInstance.IsSecurePort);
        UNIT_ASSERT(newInstance.Status == TInstanceInfo::EStatus::Unreachable);
        UNIT_ASSERT_VALUES_EQUAL(0, newInstance.BalancingScore);
        UNIT_ASSERT(newInstance.Tag.empty());

        const auto& secureInstance = instances.Instances[1];
        UNIT_ASSERT_VALUES_EQUAL("new-host", secureInstance.Host);
        UNIT_ASSERT_VALUES_EQUAL(13, secureInstance.Port);
        UNIT_ASSERT(secureInstance.IsSecurePort);

        const auto& retained = instances.Instances[2];
        UNIT_ASSERT_VALUES_EQUAL("retained-host", retained.Host);
        UNIT_ASSERT_VALUES_EQUAL(2, retained.Port);
        UNIT_ASSERT(retained.Status == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT_VALUES_EQUAL(0.5, retained.BalancingScore);
        UNIT_ASSERT_VALUES_EQUAL("old-state", retained.Tag);

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldClearStaticInstancesWhenFileHasNoValidRows)
    {
        TEnv env(false);
        auto fetcher = CreateStaticInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub()
        );
        fetcher->Start();

        TInstanceList instances;
        for (const TStringBuf contents: {
                 TStringBuf("broken-host\tnot-a-port\n"),
                 TStringBuf("\n\n"),
                 TStringBuf("")})
        {
            {
                TOFStream of(env.Config->GetInstanceListFile());
                of << "old-host\t1\n";
            }
            CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
            UNIT_ASSERT_VALUES_EQUAL(1, instances.Instances.size());

            {
                TOFStream of(env.Config->GetInstanceListFile());
                of << contents;
            }
            CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
            UNIT_ASSERT(instances.Instances.empty());
        }

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldResetStateForNewStaticInstanceAfterRemovingHosts)
    {
        TEnv env(false);
        auto fetcher = CreateStaticInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub()
        );
        fetcher->Start();

        {
            TOFStream of(env.Config->GetInstanceListFile());
            of << "A\t1\nB\t2\nC\t3\n";
        }

        TInstanceList instances;
        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
        UNIT_ASSERT_VALUES_EQUAL(3, instances.Instances.size());

        SortBy(instances.Instances, [] (const TInstanceInfo& ii) {
            return ii.Host;
        });
        UNIT_ASSERT_VALUES_EQUAL("A", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("B", instances.Instances[1].Host);
        UNIT_ASSERT_VALUES_EQUAL("C", instances.Instances[2].Host);

        for (ui32 i = 0; i < instances.Instances.size(); ++i) {
            auto& ii = instances.Instances[i];
            ii.Status = TInstanceInfo::EStatus::Reachable;
            ii.BalancingScore = 3 - i;
            ii.Tag = ii.Host;
            ii.PrevStat = {TInstant::Seconds(10 + i), 100 + i, 10 + i};
            ii.LastStat = {TInstant::Seconds(20 + i), 200 + i, 20 + i};
        }
        const auto survivor = instances.Instances[2];

        // Move C to the front and reuse B's old slot for the new host D.
        {
            TOFStream of(env.Config->GetInstanceListFile());
            of << "C\t3\nD\t4\n";
        }
        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);

        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances.size());
        const auto& retained = instances.Instances[0];
        UNIT_ASSERT_VALUES_EQUAL(survivor.Host, retained.Host);
        UNIT_ASSERT_VALUES_EQUAL(survivor.Port, retained.Port);
        UNIT_ASSERT_VALUES_EQUAL(survivor.IsSecurePort, retained.IsSecurePort);
        UNIT_ASSERT_VALUES_EQUAL(survivor.Tag, retained.Tag);
        UNIT_ASSERT(retained.Status == survivor.Status);
        UNIT_ASSERT_VALUES_EQUAL(survivor.BalancingScore, retained.BalancingScore);
        UNIT_ASSERT_VALUES_EQUAL(survivor.PrevStat.Ts, retained.PrevStat.Ts);
        UNIT_ASSERT_VALUES_EQUAL(survivor.PrevStat.Bytes, retained.PrevStat.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(
            survivor.PrevStat.Requests,
            retained.PrevStat.Requests);
        UNIT_ASSERT_VALUES_EQUAL(survivor.LastStat.Ts, retained.LastStat.Ts);
        UNIT_ASSERT_VALUES_EQUAL(survivor.LastStat.Bytes, retained.LastStat.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(
            survivor.LastStat.Requests,
            retained.LastStat.Requests);

        const auto& added = instances.Instances[1];
        UNIT_ASSERT_VALUES_EQUAL("D", added.Host);
        UNIT_ASSERT_VALUES_EQUAL(4, added.Port);
        UNIT_ASSERT(!added.IsSecurePort);
        UNIT_ASSERT_VALUES_EQUAL(0, added.BalancingScore);
        UNIT_ASSERT(added.Status == TInstanceInfo::EStatus::Unreachable);
        UNIT_ASSERT(added.Tag.empty());
        UNIT_ASSERT(!added.PrevStat);
        UNIT_ASSERT_VALUES_EQUAL(0, added.PrevStat.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(0, added.PrevStat.Requests);
        UNIT_ASSERT(!added.LastStat);
        UNIT_ASSERT_VALUES_EQUAL(0, added.LastStat.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(0, added.LastStat.Requests);

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldFetchFromFileSecure)
    {
        TEnv env(false);

        auto fetcher = CreateStaticInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub()
        );

        DoTestFetchSecure(
            *fetcher,
            [&] (const TVector<TInstanceSpec>& instanceSpecs) {
                TOFStream of(env.Config->GetInstanceListFile());
                for (const auto& x : instanceSpecs) {
                    of << x.Host << "\t" << x.Port << "\t" << 10*x.Port << Endl;
                }
            },
            [](int p) {
                return p;
            },
            S_OK
        );
    }

    Y_UNIT_TEST(ShouldFetchFromConductor)
    {
        TEnv env(true);

        auto fetcher = CreateConductorInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            env.Timer,
            env.Scheduler
        );

        auto group = [] (const TString& host) {
            return host <= "host2" ? "group1" : "group2";
        };

        DoTestFetch(
            *fetcher,
            [&] (const TVector<TInstanceSpec>& instanceSpecs) {
                TVector<THostInfo> hostInfo;
                for (const auto& x : instanceSpecs) {
                    hostInfo.push_back({group(x.Host), x.Host});
                }
                env.Conductor->SetHostInfo(std::move(hostInfo));
            },
            false,
            S_OK
        );
    }

    Y_UNIT_TEST(ShouldFetchFromConductorSecure) {
        TEnv env(true, true);

        auto fetcher = CreateConductorInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            env.Timer,
            env.Scheduler
        );

        auto group = [] (const TString& host) {
            return host <= "host2" ? "group1" : "group2";
        };

        DoTestFetchSecure(
            *fetcher,
            [&] (const TVector<TInstanceSpec>& instanceSpecs) {
                TVector<THostInfo> hostInfo;
                for (const auto& x : instanceSpecs) {
                    hostInfo.push_back({group(x.Host), x.Host});
                }
                env.Conductor->SetHostInfo(std::move(hostInfo));
            },
            [](int p) {
                if (p >= 10) {
                    return 10;
                }

                return 1;
            },
            S_OK
        );
    }

    Y_UNIT_TEST(ShouldAcceptEmptyConductorGroupWhenAnotherGroupFails)
    {
        TEnv env(true);
        auto fetcher = CreateConductorInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            env.Timer,
            env.Scheduler
        );
        fetcher->Start();

        // group1, group2 and group3 are empty; broken_group returns HTTP 503.
        TInstanceList instances;
        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);
        UNIT_ASSERT(instances.Instances.empty());

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldReturnErrorWhenAllConductorGroupsFail)
    {
        TEnv env(true);
        auto config = *env.Config->GetConfig();
        config.ClearConductorGroups();
        *config.AddConductorGroups() = "broken_group";
        env.Config = std::make_shared<TDiscoveryConfig>(std::move(config));
        auto fetcher = CreateConductorInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            env.Timer,
            env.Scheduler
        );
        fetcher->Start();

        TInstanceList instances;
        TInstanceInfo instance;
        instance.Host = "old-host";
        instance.Port = 1;
        instance.Tag = "broken_group";
        instances.Instances.push_back(std::move(instance));

        CheckFetchResult(fetcher->FetchInstances(instances), E_FAIL);
        UNIT_ASSERT_VALUES_EQUAL(1, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("old-host", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(1, instances.Instances[0].Port);

        fetcher->Stop();
    }

    Y_UNIT_TEST(ShouldProperlyProcessConductorTimeouts)
    {
        TEnv env(true);
        auto config = *env.Config->GetConfig();
        config.ClearConductorGroups();
        *config.AddConductorGroups() = "group1";
        *config.AddConductorGroups() = "group2";
        env.Config = std::make_shared<TDiscoveryConfig>(std::move(config));

        auto fetcher = CreateConductorInstanceFetcher(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            env.Timer,
            env.Scheduler
        );

        fetcher->Start();

        TVector<THostInfo> hostInfo = {
            {"group1", "host1"},
            {"group2", "host2"},
        };
        env.Conductor->SetHostInfo(hostInfo);

        TInstanceList instances;
        CheckFetchResult(fetcher->FetchInstances(instances), S_OK);

        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [] (const TInstanceInfo& ii) {
                return ii.Host;
            }
        );

        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("host2", instances.Instances[1].Host);

        env.Conductor->SetDrop("group2", true);
        hostInfo[1].Host = "host22";
        env.Conductor->SetHostInfo(hostInfo);

        auto fetch2 = fetcher->FetchInstances(instances);
        Sleep(TDuration::Seconds(1));
        env.Scheduler->RunAllScheduledTasks();
        CheckFetchResult(fetch2, S_OK);

        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [] (const TInstanceInfo& ii) {
                return ii.Host;
            }
        );

        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("host2", instances.Instances[1].Host);

        env.Conductor->SetDrop("group2", false);

        auto fetch3 = fetcher->FetchInstances(instances);
        CheckFetchResult(fetch3, S_OK);

        SortBy(
            instances.Instances.begin(),
            instances.Instances.end(),
            [] (const TInstanceInfo& ii) {
                return ii.Host;
            }
        );

        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("host22", instances.Instances[1].Host);

        env.Conductor->SetDrop("group1", true);
        env.Conductor->SetDrop("group2", true);
        auto fetch4 = fetcher->FetchInstances(instances);
        env.Scheduler->RunAllScheduledTasks();
        CheckFetchResult(fetch4, E_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(2, instances.Instances.size());
        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("host22", instances.Instances[1].Host);

        fetcher->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
