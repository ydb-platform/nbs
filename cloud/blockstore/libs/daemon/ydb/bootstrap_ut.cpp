#include "bootstrap.h"

#include <cloud/blockstore/libs/daemon/testlib/bootstrap_metrics.h>
#include <cloud/blockstore/libs/storage/init/common/actorsystem.h>

#include <cloud/storage/core/libs/kikimr/monitoring/monitoring.h>

#include <contrib/ydb/library/actors/interconnect/poller/poller_actor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/scope.h>
#include <util/network/socket.h>

namespace NCloud::NBlockStore::NServer {

namespace NTests {

// Exercise the bootstrap's production handover without requiring registration,
// CMS or storage backends. The injected actor system still uses the real shared
// blockstore runner and the normal actor-based monitoring service.
struct TBootstrapYdbTestAccess
{
    static void StartActorSystem(
        TBootstrapYdb& bootstrap,
        NCloud::NStorage::TKikimrMonitoringPtr monitoring,
        IActorSystemPtr actorSystem)
    {
        bootstrap.KikimrMonitoring = std::move(monitoring);
        bootstrap.ActorSystem = std::move(actorSystem);
        bootstrap.StartActorSystem();
    }
};

}   // namespace NTests

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMonitoringPollerInitializer final: public NKikimr::IServiceInitializer
{
public:
    void InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) override
    {
        // Normally installed with interconnect; this fixture only needs the
        // poller for the real TMon HTTP transport, without opening an IC port.
        setup->LocalServices.emplace_back(
            NActors::MakePollerActorId(),
            NActors::TActorSetupCmd(
                NActors::CreatePollerActor(),
                NActors::TMailboxType::ReadAsFilled,
                appData->SystemPoolId));
    }
};

NKikimrConfig::TMonitoringConfig MakeMonitoringConfig(ui16 port)
{
    NKikimrConfig::TMonitoringConfig config;
    config.SetMonitoringPort(port);
    config.SetMonitoringAddress("127.0.0.1");
    config.SetMonitoringThreads(1);
    return config;
}

TString WaitForPage(ui16 port, const TString& path)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    for (;;) {
        try {
            TKeepAliveHttpClient client(
                "127.0.0.1",
                port,
                TDuration::Seconds(1),
                TDuration::Seconds(1));
            TStringStream body;
            if (client.DoGet(path, &body) == 200) {
                return body.Str();
            }
        } catch (const yexception&) {
            // KikimrStart initiates normal monitoring asynchronously.
        }
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Monitoring did not start");
        Sleep(TDuration::MilliSeconds(10));
    }
}

i64 GetSensorValue(const TString& body, TStringBuf name)
{
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(body, &json));
    for (const auto& sensor: json["sensors"].GetArray()) {
        if (sensor["labels"]["sensor"].GetString() == name) {
            return sensor["value"].GetIntegerRobust();
        }
    }
    UNIT_FAIL("Missing sensor " << name);
    return 0;
}

void CheckPortReleased(ui16 port)
{
    TSocketHolder listener(socket(AF_INET, SOCK_STREAM, 0));
    UNIT_ASSERT(!listener.Closed());
    // Permit reuse after completed HTTP connections, but never SO_REUSEPORT:
    // an exporter that is still listening must make this bind fail.
    const int reuseAddress = 1;
    UNIT_ASSERT_VALUES_EQUAL(
        0,
        setsockopt(
            listener,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuseAddress,
            sizeof(reuseAddress)));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    UNIT_ASSERT_VALUES_EQUAL(
        0,
        bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)));
    UNIT_ASSERT_VALUES_EQUAL(0, listen(listener, 1));
}

void CheckActorSystemHandover(bool changeMonitoringPort)
{
    TPortManager ports;
    const ui16 earlyPort = ports.GetPort();
    const ui16 mainPort = changeMonitoringPort ? ports.GetPort() : earlyPort;
    {
        auto factories = std::make_shared<NKikimr::TModuleFactories>();
        TBootstrapYdb bootstrap(
            factories,
            std::make_shared<TServerModuleFactories>(),
            {});
        Y_DEFER
        {
            bootstrap.Stop();
        };

        auto monitoring = NCloud::NStorage::CreateKikimrMonitoring(
            MakeMonitoringConfig(earlyPort),
            "blockstore");
        const auto counters = monitoring->GetCounters();
        auto initialization = counters->GetSubgroup("counters", "blockstore")
                                  ->GetSubgroup("component", "bootstrap");
        auto savedCounter = initialization->GetCounter("SavedCounter", true);
        *savedCounter = 42;
        monitoring->Start();
        const TString path =
            "/counters/counters=blockstore/component=bootstrap/json";
        const auto before = WaitForPage(earlyPort, path);
        UNIT_ASSERT_VALUES_EQUAL(1, GetSensorValue(before, "Initializing"));
        UNIT_ASSERT_VALUES_EQUAL(42, GetSensorValue(before, "SavedCounter"));

        auto config = std::make_shared<NKikimrConfig::TAppConfig>();
        *config->MutableMonitoringConfig() = MakeMonitoringConfig(mainPort);
        config->MutableLogConfig();
        auto* systemConfig = config->MutableActorSystemConfig();
        auto* executor = systemConfig->AddExecutor();
        executor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
        executor->SetThreads(1);
        executor->SetName("System");
        systemConfig->MutableScheduler();

        NCloud::NBlockStore::NStorage::TActorSystemArgs args;
        args.ModuleFactories = factories;
        args.NodeId = 1;
        args.AppConfig = config;
        args.Counters = counters;
        args.ServicesMask.DisableAll();
        args.ServicesMask.EnableBasicServices = true;
        args.ServicesMask.EnableLogger = true;
        args.OnInitialize = [](auto&, auto& services)
        {
            services.AddServiceInitializer(new TMonitoringPollerInitializer());
        };
        args.PrepareKikimrRunConfig = [](auto&) {
        };
        args.OnStart = [](IActorSystem&) {
        };
        auto actorSystem =
            MakeIntrusive<NCloud::NBlockStore::NStorage::TActorSystem>(args);
        actorSystem->Init();
        actorSystem->RegisterIndexPage("normal-only", "Normal monitoring");
        UNIT_ASSERT(actorSystem->GetCounters() == counters);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            initialization->GetCounter("Initializing", false)->GetAtomic());

        NTests::TBootstrapYdbTestAccess::StartActorSystem(
            bootstrap,
            monitoring,
            actorSystem);
        // This page only exists in the real TMon, so serving counters from the
        // temporary runtime cannot accidentally satisfy the handover check.
        WaitForPage(mainPort, "/normal-only");
        ++*savedCounter;
        const auto after = WaitForPage(mainPort, path);
        UNIT_ASSERT_VALUES_EQUAL(0, GetSensorValue(after, "Initializing"));
        UNIT_ASSERT_VALUES_EQUAL(43, GetSensorValue(after, "SavedCounter"));
        UNIT_ASSERT(actorSystem->GetCounters() == counters);
        UNIT_ASSERT(monitoring->GetCounters() == counters);
        if (changeMonitoringPort) {
            // CMS may move normal monitoring to another port. The early port
            // must still close rather than leaving a second exporter behind.
            CheckPortReleased(earlyPort);
        }
    }
    // Exercise the bootstrap's normal cleanup too, including its repeated
    // Stop on the already-stopped temporary runtime.
    CheckPortReleased(earlyPort);
    if (changeMonitoringPort) {
        CheckPortReleased(mainPort);
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(TBootstrapYdbMetricsTest)
{
    Y_UNIT_TEST(ShouldExposeMetricsBeforeNodeRegistrationCompletes)
    {
        NTests::TBootstrapMetricsEnv env;
        TBootstrapYdb bootstrap(
            std::make_shared<NKikimr::TModuleFactories>(),
            std::make_shared<TServerModuleFactories>(),
            {});
        env.CheckRegistration(bootstrap, {"--service", "kikimr"});
    }

    Y_UNIT_TEST(ShouldHandOverMonitoringToSharedActorSystem)
    {
        CheckActorSystemHandover(false);
    }

    Y_UNIT_TEST(ShouldCloseEarlyMonitoringWhenCmsChangesItsPort)
    {
        CheckActorSystemHandover(true);
    }
}

}   // namespace NCloud::NBlockStore::NServer
