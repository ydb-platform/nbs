#include "healthcheck.h"
#include "config.h"
#include "ping.h"
#include "test_server.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/grpc/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/file.h>
#include <util/system/sanitizers.h>

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString FilePath(TStringBuf fileName)
{
    return JoinFsPaths(ArcadiaSourceRoot(), "cloud/blockstore/tests", fileName);
}

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TDiscoveryConfigPtr Config;
    TVector<std::shared_ptr<TFakeBlockStoreServer>> Instances;
    TPortManager PortManager;

    TString RootCertsPath;
    TString CertPath;
    TString PrivateKeyPath;

    TEnv(
        ui32 instanceCount,
        TDuration timeout = TDuration::Zero(),
        ui32 maxPingRequestsPerHealthCheck = 0)
    {
        RootCertsPath = CertPath = FilePath("certs/server.crt");
        PrivateKeyPath = FilePath("certs/server.key");

        for (ui32 i = 0; i < instanceCount; ++i) {
            int attempts = 10;
            while (true) {
                Instances.push_back(std::make_shared<TFakeBlockStoreServer>(
                    PortManager.GetTcpPort(),
                    PortManager.GetTcpPort(),
                    RootCertsPath,
                    PrivateKeyPath,
                    CertPath
                ));

                try {
                    Instances.back()->ForkStart();

                    break;
                } catch (...) {
                    Instances.pop_back();

                    if (--attempts == 0) {
                        throw;
                    }
                }
            }
        }

        NProto::TDiscoveryServiceConfig c;
        c.SetPingRequestTimeout(timeout.MilliSeconds());
        c.SetMaxPingRequestsPerHealthCheck(maxPingRequestsPerHealthCheck);
        Config = std::make_shared<TDiscoveryConfig>(std::move(c));
    }
};

TInstanceInfo Instance(TString host, ui16 port, bool insecure = true)
{
    TInstanceInfo ii;
    ii.Host = std::move(host);
    ii.Port = port;
    ii.IsSecurePort = !insecure;
    return ii;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(HealthcheckTest)
{
    void ShouldCheckHealthImpl(bool insecure)
    {
        TEnv env(3, TDuration::Zero(), 0);

        auto logging = CreateLoggingService("console");
        auto grpcLog = logging->CreateLog("GRPC");
        GrpcLoggerInit(grpcLog, true /* enableTracing */);

        auto monitoring = CreateMonitoringServiceStub();
        auto healthChecker = CreateHealthChecker(
            env.Config,
            std::move(logging),
            monitoring,
            CreateInsecurePingClient(),
            CreateSecurePingClient(env.RootCertsPath)
        );

        healthChecker->Start();

        TInstanceList instances;
        for (const auto& instance : env.Instances) {
            instances.Instances.push_back(Instance("localhost", insecure
                ? instance->Port()
                : instance->SecurePort(), insecure));
        }
        instances.Instances.push_back(Instance("0.0.0.0", 1));
        instances.Instances.push_back(Instance("nonexistenthost", 80));

        env.Instances[0]->SetLastByteCount(100);
        env.Instances[1]->SetLastByteCount(200);
        env.Instances[2]->SetErrorMessage("xxx");

        healthChecker->UpdateStatus(instances).Wait();

        UNIT_ASSERT(instances.Instances[0].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            instances.Instances[0].LastStat.Bytes
        );

        UNIT_ASSERT(instances.Instances[1].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            instances.Instances[1].LastStat.Bytes
        );

        UNIT_ASSERT(instances.Instances[2].Status
            == TInstanceInfo::EStatus::Unreachable);
        UNIT_ASSERT(instances.Instances[3].Status
            == TInstanceInfo::EStatus::Unreachable);
        UNIT_ASSERT(instances.Instances[4].Status
            == TInstanceInfo::EStatus::Unreachable);

        // hdr histogram does not work under tsan
        if (!NSan::TSanIsOn()) {
            const auto pingTimeMedian = monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "discovery")
                ->GetSubgroup("subcomponent", "healthcheck")
                ->GetSubgroup("percentiles", "PingTime")
                ->GetCounter("50")->GetAtomic();

            UNIT_ASSERT(pingTimeMedian);
        }

        healthChecker->Stop();
    }

    void ShouldProperlyProcessPingTimeoutsImpl(bool insecure)
    {
        TEnv env(2, TDuration::Seconds(1), 0);

        auto healthChecker = CreateHealthChecker(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateInsecurePingClient(),
            CreateSecurePingClient(env.RootCertsPath)
        );

        healthChecker->Start();

        TInstanceList instances;
        for (const auto& instance : env.Instances) {
            instances.Instances.push_back(Instance("localhost", insecure
                ? instance->Port()
                : instance->SecurePort(), insecure));
        }

        env.Instances[1]->SetDropPingRequests(true);

        healthChecker->UpdateStatus(instances).Wait();

        UNIT_ASSERT(instances.Instances[0].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[1].Status
            == TInstanceInfo::EStatus::Unreachable);

        env.Instances[1]->SetDropPingRequests(false);

        healthChecker->UpdateStatus(instances).Wait();

        UNIT_ASSERT(instances.Instances[0].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[1].Status
            == TInstanceInfo::EStatus::Reachable);

        healthChecker->Stop();
    }

    void ShouldNotSendTooManyRequestsAtOnceImpl(bool insecure)
    {
        TEnv env(4, TDuration::Seconds(15), 2);

        auto healthChecker = CreateHealthChecker(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateInsecurePingClient(),
            CreateSecurePingClient(env.RootCertsPath)
        );

        healthChecker->Start();

        TInstanceList instances;
        for (const auto& instance : env.Instances) {
            instances.Instances.push_back(Instance("localhost", insecure
                ? instance->Port()
                : instance->SecurePort(), insecure));
        }

        healthChecker->UpdateStatus(instances).Wait();

        UNIT_ASSERT(instances.Instances[0].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[1].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[2].Status
            == TInstanceInfo::EStatus::Unreachable);
        UNIT_ASSERT(instances.Instances[3].Status
            == TInstanceInfo::EStatus::Unreachable);

        const auto ts0 = instances.Instances[0].LastStat.Ts;
        const auto ts1 = instances.Instances[1].LastStat.Ts;
        UNIT_ASSERT(ts0.GetValue());
        UNIT_ASSERT(ts1.GetValue());
        UNIT_ASSERT(!instances.Instances[2].LastStat.Ts.GetValue());
        UNIT_ASSERT(!instances.Instances[3].LastStat.Ts.GetValue());

        healthChecker->UpdateStatus(instances).Wait();

        UNIT_ASSERT(instances.Instances[0].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[1].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[2].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[3].Status
            == TInstanceInfo::EStatus::Reachable);

        UNIT_ASSERT_VALUES_EQUAL(ts0, instances.Instances[0].LastStat.Ts);
        UNIT_ASSERT_VALUES_EQUAL(ts1, instances.Instances[1].LastStat.Ts);

        const auto ts2 = instances.Instances[2].LastStat.Ts;
        const auto ts3 = instances.Instances[3].LastStat.Ts;
        UNIT_ASSERT(ts2.GetValue());
        UNIT_ASSERT(ts3.GetValue());

        healthChecker->UpdateStatus(instances).Wait();

        UNIT_ASSERT(instances.Instances[0].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[1].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[2].Status
            == TInstanceInfo::EStatus::Reachable);
        UNIT_ASSERT(instances.Instances[3].Status
            == TInstanceInfo::EStatus::Reachable);

        UNIT_ASSERT_VALUES_UNEQUAL(ts0, instances.Instances[0].LastStat.Ts);
        UNIT_ASSERT_VALUES_UNEQUAL(ts1, instances.Instances[1].LastStat.Ts);
        UNIT_ASSERT_VALUES_EQUAL(ts2, instances.Instances[2].LastStat.Ts);
        UNIT_ASSERT_VALUES_EQUAL(ts3, instances.Instances[3].LastStat.Ts);

        healthChecker->Stop();
    }

    Y_UNIT_TEST(ShouldCheckHealth)
    {
        ShouldCheckHealthImpl(true);
    }

    Y_UNIT_TEST(ShouldCheckHealthSecure)
    {
        ShouldCheckHealthImpl(false);
    }

    Y_UNIT_TEST(ShouldProperlyProcessPingTimeouts)
    {
        ShouldProperlyProcessPingTimeoutsImpl(true);
    }

    Y_UNIT_TEST(ShouldProperlyProcessPingTimeoutsSecure)
    {
        ShouldProperlyProcessPingTimeoutsImpl(false);
    }

    Y_UNIT_TEST(ShouldSucceedUponEmptyInstanceList)
    {
        TEnv env(2, TDuration::Seconds(1), 0);

        auto healthChecker = CreateHealthChecker(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateInsecurePingClient(),
            CreateSecurePingClient(env.RootCertsPath)
        );

        healthChecker->Start();

        TInstanceList instances;
        UNIT_ASSERT(healthChecker->UpdateStatus(instances).HasValue());

        healthChecker->Stop();
    }

    Y_UNIT_TEST(ShouldNotSendTooManyRequestsAtOnce)
    {
        ShouldNotSendTooManyRequestsAtOnceImpl(true);
    }

    Y_UNIT_TEST(ShouldNotSendTooManyRequestsAtOnceSecure)
    {
        ShouldNotSendTooManyRequestsAtOnceImpl(false);
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
