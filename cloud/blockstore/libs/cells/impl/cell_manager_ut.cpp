#include "cell_manager.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/server/server.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;
using namespace NCloud::NBlockStore::NClient;
using namespace NCloud::NBlockStore::NServer;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TTestService> CreateLocalService()
{
    auto service = std::make_shared<TTestService>();

    service->SetHandler([] (auto request){
        Y_UNUSED(request);
        NProto::TDescribeVolumeResponse response;
        *response.MutableError() = MakeError(E_NOT_FOUND, "");
        return MakeFuture<NProto::TDescribeVolumeResponse>(std::move(response));
    });
    return service;
}

void CheckDescribe(
    const ICellManagerPtr& cellManager,
    const NProto::TClientConfig& config,
    ui32 errorCode)
{
    NProto::THeaders headers;
    headers.SetClientId(FQDNHostName());

    auto future = cellManager->DescribeVolume(
        MakeIntrusive<TCallContext>(),
        "disk",
        std::move(headers),
        CreateLocalService(),
        config);

    const auto& response = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(errorCode, response.GetError().GetCode());
}

TString GetTestFilePath(const TString& fileName)
{
    return JoinFsPaths(
        ArcadiaSourceRoot(),
        "cloud/blockstore/tests",
        fileName);
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>();
}

////////////////////////////////////////////////////////////////////////////////

struct TTestContext
{
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    IProfileLogPtr ProfileLog;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    ITraceSerializerPtr TraceSerializer;
    IServerStatsPtr ServerStats;
    TString CellId;

    TTestContext()
        : Timer(CreateWallClockTimer())
        , Scheduler(CreateSchedulerStub())
        , Logging(CreateLoggingService("console"))
        , Monitoring(CreateMonitoringServiceStub())
        , ProfileLog(CreateProfileLogStub())
        , RequestStats(CreateRequestStatsStub())
        , VolumeStats(CreateVolumeStatsStub())
        , TraceSerializer(CreateTraceSerializerStub())
        , ServerStats(CreateServerStatsStub())
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TTestServerBuilder final
{
private:
    TTestContext TestContext;
    NProto::TServerAppConfig ServerAppConfig;

public:
    explicit TTestServerBuilder(TTestContext testContext)
        : TestContext(std::move(testContext))
    {}

    TTestServerBuilder& SetPort(ui16 port)
    {
        ServerAppConfig.MutableServerConfig()->SetPort(port);
        return *this;
    }

    TTestServerBuilder& SetSecureEndpoint(
        ui16 port,
        const TString& rootCertsFileName,
        const TString& certFileName,
        const TString& certPrivateKeyFileName)
    {
        auto& serverConfig = *ServerAppConfig.MutableServerConfig();
        serverConfig.SetSecurePort(port);
        serverConfig.SetRootCertsFile(GetTestFilePath(rootCertsFileName));
        serverConfig.SetCertFile(GetTestFilePath(certFileName));
        serverConfig.SetCertPrivateKeyFile(GetTestFilePath(certPrivateKeyFileName));
        return *this;
    }

    TTestServerBuilder& AddCert(
        const TString& certFileName,
        const TString& certPrivateKeyFileName)
    {
        auto& cert = *ServerAppConfig.MutableServerConfig()->AddCerts();
        cert.SetCertFile(GetTestFilePath(certFileName));
        cert.SetCertPrivateKeyFile(GetTestFilePath(certPrivateKeyFileName));
        return *this;
    }

    TTestServerBuilder& SetCellId(TString cellId)
    {
        TestContext.CellId = std::move(cellId);
        return *this;
    }

    IServerPtr BuildServer(
        IBlockStorePtr service,
        IBlockStorePtr udsService = nullptr)
    {
        auto serverConfig = std::make_shared<TServerAppConfig>(ServerAppConfig);

        auto serverStats = CreateServerStats(
            serverConfig,
            CreateTestDiagnosticsConfig(),
            TestContext.Monitoring,
            TestContext.ProfileLog,
            TestContext.RequestStats,
            TestContext.VolumeStats);

        auto server = CreateServer(
            std::move(serverConfig),
            TestContext.Logging,
            std::move(serverStats),
            std::move(service),
            std::move(udsService),
            TServerOptions {
                .CellId = TestContext.CellId
            });
        return server;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCellConfigBuilder
{
private:
    NProto::TCellsConfig Config;

public:
    TCellConfigBuilder(TString cellId, bool isEnabled)
    {
        Config.SetCellId(std::move(cellId));
        Config.SetCellsEnabled(isEnabled);
    }

    TCellConfigBuilder& SetRootCert(TString rootCertFile)
    {
        Config.MutableGrpcClientConfig()->SetRootCertsFile(
            GetTestFilePath(rootCertFile));
        return *this;
    }

    TCellConfigBuilder& AddCell(
        TString cellId,
        ui16 grpcPort,
        ui16 secureGrpcPort,
        ui32 describeVolumeHostCnt,
        ui32 minCellConnections,
        TVector<TString> hosts)
    {
        auto* cell = Config.AddCells();
        cell->SetCellId(std::move(cellId));
        cell->SetGrpcPort(grpcPort);
        cell->SetSecureGrpcPort(secureGrpcPort);
        cell->SetDescribeVolumeHostCount(describeVolumeHostCnt);
        cell->SetMinCellConnections(minCellConnections);
        for (auto host: hosts) {
            cell->AddHosts()->SetFqdn(std::move(host));
        }
        return *this;
    }

    NProto::TCellsConfig Build() const
    {
        return Config;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellManagerTest)
{
    Y_UNIT_TEST(ShouldHandleCellDescribeRequestsOverInsecureChannel)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);

        auto service = std::make_shared<TTestService>();
        service->DescribeVolumeHandler =
            [&] (auto request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TDescribeVolumeResponse>();
            };

        TTestContext testContext;

        auto server = TTestServerBuilder(testContext)
            .SetPort(port)
            .SetCellId("xyz")
            .BuildServer(service);


        auto cfg = TCellConfigBuilder("abc", true)
            .AddCell(
                "xyz",  // cellid
                port,   // port
                0,      // secure port
                1,      // describe volume host count
                1,      // min cell connections
                {"localhost"})
            .Build();

        auto config = std::make_shared<TCellsConfig>(std::move(cfg));

        auto cellManager = CreateCellManager(
            std::move(config),
            testContext.Timer,
            testContext.Scheduler,
            testContext.Logging,
            testContext.Monitoring,
            testContext.TraceSerializer,
            testContext.ServerStats,
            nullptr);

        server->Start();
        cellManager->Start();
        Y_DEFER {
            cellManager->Stop();
            server->Stop();
        };

        NProto::TClientConfig clientConfig;
        clientConfig.SetPort(port);

        CheckDescribe(cellManager, std::move(clientConfig), S_OK);
    }

    Y_UNIT_TEST(ShouldHandleCellDescribeRequestsOverSecureChannel)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);
        ui16 securePort = portManager.GetPort(9002);

        auto service = std::make_shared<TTestService>();
        service->DescribeVolumeHandler =
            [&] (auto request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TDescribeVolumeResponse>();
            };

        TTestContext testContext;

        auto server = TTestServerBuilder(testContext)
            .SetPort(port)
            .SetSecureEndpoint(
                securePort,
                "certs/server.crt",
                "certs/server.crt",
                "certs/server.key")
            .SetCellId("xyz")
            .BuildServer(service);


        auto cfg = TCellConfigBuilder("abc", true)
            .AddCell(
                "xyz",        // cellid
                port,         // port
                securePort,   // secure port
                1,            // describe volume host count
                1,            // min cell connections
                {"localhost"})
            .SetRootCert("certs/server.crt")
            .Build();

        auto config = std::make_shared<TCellsConfig>(std::move(cfg));

        auto cellManager = CreateCellManager(
            std::move(config),
            testContext.Timer,
            testContext.Scheduler,
            testContext.Logging,
            testContext.Monitoring,
            testContext.TraceSerializer,
            testContext.ServerStats,
            nullptr);

        server->Start();
        cellManager->Start();
        Y_DEFER {
            cellManager->Stop();
            server->Stop();
        };

        NProto::TClientConfig clientConfig;
        clientConfig.SetPort(port);
        clientConfig.SetSecurePort(securePort);

        CheckDescribe(cellManager, std::move(clientConfig), S_OK);
    }
}

}   // namespace NCloud::NBlockStore::NCells