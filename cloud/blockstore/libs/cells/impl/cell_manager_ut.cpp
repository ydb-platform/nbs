#include "cell_manager.h"
#include "describe_volume.h"
#include "mon.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/inbound_activity.h>
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
#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/grpc/tls_certificate_provider.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/str.h>

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

ICertificateProviderPtr CreateClientCertificateProvider(
    const TCellsConfigPtr& config)
{
    auto appConfig = std::make_shared<TClientAppConfig>(
        config->GetGrpcClientConfig());

    TVector<NCloud::TCertificateFiles> certPathList {
        {
            .PrivateKeyPath = appConfig->GetCertPrivateKeyFile(),
            .CertChainPath = appConfig->GetCertFile()
        }
    };

    return CreateStaticCertificateProvider(
        appConfig->GetRootCertsFile(),
        std::move(certPathList));
}

ICertificateProviderPtr CreateServerCertificateProvider(
    const TServerAppConfigPtr& config)
{
    TVector<NCloud::TCertificateFiles> certPathList;
    for (const auto& cert: config->GetCerts()) {
        certPathList.push_back({
            cert.CertPrivateKeyFile,
            cert.CertFile
        });
    }

    if (certPathList.empty()) {
        certPathList.push_back({
            config->GetCertPrivateKeyFile(),
            config->GetCertFile()
        });
    }

    return CreateStaticCertificateProvider(
        config->GetRootCertsFile(),
        std::move(certPathList));
}

////////////////////////////////////////////////////////////////////////////////

struct TTestContext
{
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    TDiagnosticsConfigPtr DiagnosticsConfig;
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
        , DiagnosticsConfig(std::make_shared<TDiagnosticsConfig>())
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
        auto certificateProvider =
            CreateServerCertificateProvider(serverConfig);

        auto server = CreateServer(
            std::move(serverConfig),
            TestContext.Logging,
            std::move(serverStats),
            std::move(service),
            std::move(udsService),
            TServerOptions {
                .CellId = TestContext.CellId
            },
            std::move(certificateProvider));
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
            config,
            testContext.Timer,
            testContext.Scheduler,
            testContext.Logging,
            testContext.Monitoring,
            testContext.TraceSerializer,
            testContext.ServerStats,
            CreateClientCertificateProvider(config),
            nullptr,
            CreateLocalService());

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
            config,
            testContext.Timer,
            testContext.Scheduler,
            testContext.Logging,
            testContext.Monitoring,
            testContext.TraceSerializer,
            testContext.ServerStats,
            CreateClientCertificateProvider(config),
            nullptr,
            CreateLocalService());

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

    Y_UNIT_TEST(ShouldRenderCellsPage)
    {
        TTestContext testContext;

        auto cfg = TCellConfigBuilder("abc", true)
            .AddCell("xyz", 9001, 0, 1, 1, {"host-alpha"})
            .AddCell("uvw", 9001, 0, 1, 1, {"host-gamma"})
            .Build();
        // pinged, so its hosts' liveness means something; uvw is not
        cfg.MutableCells(0)->SetHostMigrationEnabled(true);
        // the host overrides the cell's transport
        cfg.MutableCells(0)->SetTransport(NProto::CELL_DATA_TRANSPORT_RDMA);
        cfg.MutableCells(0)->MutableHosts(0)->SetTransport(
            NProto::CELL_DATA_TRANSPORT_GRPC);
        auto config = std::make_shared<TCellsConfig>(std::move(cfg));
        Y_UNUSED(testContext);

        TCellsSnapshot snapshot;
        snapshot.HostStatuses["xyz"].push_back(
            {.Fqdn = "host-alpha", .Alive = true, .Warm = false,
             .Connections = 0});
        snapshot.HostStatuses["uvw"].push_back(
            {.Fqdn = "host-gamma", .Alive = true, .Warm = false,
             .Connections = 0});
        snapshot.Mounts.push_back(
            {.DiskId = "disk-1",
             .ClientId = "client-1",
             .CellId = "xyz",
             .Host = "host-alpha",
             .DataTransport = "grpc fallback",
             .TabletHost = "host-beta"});

        TStringStream out;
        RenderCellsPage(out, *config, snapshot, TDiagnosticsConfig());
        const auto html = out.Str();

        UNIT_ASSERT_STRING_CONTAINS(html, "this node: abc");
        UNIT_ASSERT_STRING_CONTAINS(html, "name='Volume'");
        UNIT_ASSERT_STRING_CONTAINS(html, "value='search'");

        // a healthy cell folds away, its heading already says it is fine
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<details class='panel panel-success'>"
            "<summary class='panel-heading'><strong>xyz</strong>");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 / 1 alive");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<span class='label label-default'>default: rdma</span>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td>host-alpha</td><td><span class='label label-success'>"
            "alive</span></td><td><span class='badge'>0</span></td>"
            "<td>grpc</td><td>9001</td>");

        // a host nobody pings is not vouched for: its cell is neither green
        // nor folded
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<details class='panel panel-default' open>"
            "<summary class='panel-heading'><strong>uvw</strong>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td>host-gamma</td><td><span class='label label-default'>"
            "not probed</span></td>");

        // the summary counts what has no one-line heading of its own
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<div class='stat'>1</div>"
            "<small class='text-muted'>intercell mounts</small>");

        // a remote mount names the host it goes through, linked to the disk
        // there, what carries its data now and where its tablet is
        UNIT_ASSERT_STRING_CONTAINS(html, "Intercell mounts");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<a href='http://host-alpha:8766/blockstore/service?action=search"
            "&amp;Volume=disk-1' target='_blank' rel='noopener'>host-alpha</a>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<span class='label label-warning'>grpc fallback</span>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<a href='http://host-beta:8766/blockstore/service?action=search"
            "&amp;Volume=disk-1' target='_blank' rel='noopener'>host-beta</a> "
            "<span class='label label-warning'>elsewhere</span>");

        UNIT_ASSERT_STRING_CONTAINS(html, "Inbound");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<summary class='panel-heading'><strong>Cells config</strong>");
    }

    Y_UNIT_TEST(ShouldRenderSearchResultLinks)
    {
        TVector<TCellDescribeResult> results;
        results.push_back({
            .CellId = "xyz",
            .Status = ECellDescribeStatus::Found,
            .Fqdn = "host-a"});
        results.push_back({   // the local row: CellId left empty
            .Status = ECellDescribeStatus::Found,
            .Fqdn = "localhost"});
        results.push_back({
            .CellId = "abc",
            .Status = ECellDescribeStatus::NotFound});
        results.push_back({
            .CellId = "def",
            .Status = ECellDescribeStatus::Unavailable});
        results.push_back({
            .CellId = "ghi",
            .Status = ECellDescribeStatus::MigrationDestination,
            .Fqdn = "host-m"});

        TStringStream out;
        RenderCellsSearchResult(
            out, results, TDiagnosticsConfig(), "own", "disk-x");
        const auto html = out.Str();

        // a remote hit links the disk to the responding host's mon port, with
        // the action that triggers the search on the target service page, in
        // a new tab
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td><a href='http://host-a:8766/blockstore/service?action=search"
            "&amp;Volume=disk-x' target='_blank' rel='noopener'>disk-x</a>"
            "</td><td>host-a</td>"
            "<td><span class='label label-success'>found</span></td>");
        // the local hit links relative to /blockstore/cells so the Viewer node
        // prefix survives; no leading slash, no http://host:port
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td>local (own)</td><td><a href='service?action=search"
            "&amp;Volume=disk-x' target='_blank' rel='noopener'>disk-x</a>"
            "</td><td>localhost</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<span class='label label-default'>not found</span>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<span class='label label-warning'>unavailable</span>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td></td><td>host-m</td>"
            "<td><span class='label label-info'>migration copy</span></td>");
        // the form keeps what was searched for
        UNIT_ASSERT_STRING_CONTAINS(html, "value='disk-x'");
    }

    Y_UNIT_TEST(ShouldEncodeSpecialCharsInSearchLink)
    {
        TVector<TCellDescribeResult> results;
        results.push_back({   // the local row: CellId left empty
            .Status = ECellDescribeStatus::Found,
            .Fqdn = "localhost"});

        TStringStream out;
        RenderCellsSearchResult(
            out, results, TDiagnosticsConfig(), "own", "disk#a&b");
        const auto html = out.Str();

        // the id is url-encoded before html-escaping, so '#'/'&' cannot
        // truncate or split the Volume query parameter
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "service?action=search&amp;Volume=disk%23a%26b");
    }

    Y_UNIT_TEST(ShouldRejectConnectionToUnconfiguredCell)
    {
        TTestContext testContext;

        auto cfg = TCellConfigBuilder("abc", true)
            .AddCell(
                "xyz",  // cellid
                9001,   // port
                0,      // secure port
                1,      // describe volume host count
                1,      // min cell connections
                {"localhost"})
            .Build();

        auto config = std::make_shared<TCellsConfig>(std::move(cfg));

        auto cellManager = CreateCellManager(
            config,
            testContext.Timer,
            testContext.Scheduler,
            testContext.Logging,
            testContext.Monitoring,
            testContext.TraceSerializer,
            testContext.ServerStats,
            CreateClientCertificateProvider(config),
            nullptr,
            CreateLocalService());

        // A cell id we never configured can only come from broken internal
        // state, so it is reported as such rather than as a lookup miss.
        auto future = cellManager->CreateConnection(
            "no-such-cell",
            {},
            std::make_shared<TClientAppConfig>(),
            nullptr);

        UNIT_ASSERT(future.HasValue());

        const auto& result = future.GetValue();
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            result.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldServeGrpcDataThroughControlPort)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);

        auto service = std::make_shared<TTestService>();
        ui32 zeroBlocksCount = 0;
        service->ZeroBlocksHandler =
            [&] (auto request) {
                Y_UNUSED(request);
                ++zeroBlocksCount;
                return MakeFuture<NProto::TZeroBlocksResponse>();
            };
        TString written;
        service->WriteBlocksHandler =
            [&] (auto request) {
                for (const auto& block: request->GetBlocks().GetBuffers()) {
                    written += block;
                }
                return MakeFuture<NProto::TWriteBlocksResponse>();
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
        // the same gRPC data endpoint the rdma transport falls back to
        cfg.MutableCells(0)->SetTransport(NProto::CELL_DATA_TRANSPORT_GRPC);

        auto config = std::make_shared<TCellsConfig>(std::move(cfg));

        auto cellManager = CreateCellManager(
            config,
            testContext.Timer,
            testContext.Scheduler,
            testContext.Logging,
            testContext.Monitoring,
            testContext.TraceSerializer,
            testContext.ServerStats,
            CreateClientCertificateProvider(config),
            nullptr,
            CreateLocalService());

        server->Start();
        cellManager->Start();
        Y_DEFER {
            cellManager->Stop();
            server->Stop();
        };

        auto connectionOrError = cellManager
            ->CreateConnection(
                "xyz",
                {},
                std::make_shared<TClientAppConfig>(),
                nullptr)
            .GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(
            !HasError(connectionOrError),
            connectionOrError.GetError());

        // as a session sends it
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        request->MutableHeaders()->SetClientId("client");

        auto storage = connectionOrError.GetResult()->GetStorage();
        auto response =
            storage->ZeroBlocks(MakeIntrusive<TCallContext>(), request)
                .GetValue(TDuration::Seconds(5));

        // the cell's control port only takes the control service
        UNIT_ASSERT_C(!HasError(response), response.GetError());
        UNIT_ASSERT_VALUES_EQUAL(1, zeroBlocksCount);

        // as a gRPC-IPC session sends it: with what its own server filled in
        request->MutableHeaders()->MutableInternal()->SetRequestSource(
            NProto::SOURCE_FD_DATA_CHANNEL);
        response = storage->ZeroBlocks(MakeIntrusive<TCallContext>(), request)
                       .GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
        UNIT_ASSERT_VALUES_EQUAL(2, zeroBlocksCount);

        const ui32 blockSize = 4096;
        TString data(blockSize, 'x');
        auto writeRequest =
            std::make_shared<NProto::TWriteBlocksLocalRequest>();
        writeRequest->MutableHeaders()->SetClientId("client");
        writeRequest->MutableHeaders()->MutableInternal()->SetRequestSource(
            NProto::SOURCE_FD_DATA_CHANNEL);
        writeRequest->BlocksCount = 1;
        writeRequest->SetBlockSize(blockSize);
        writeRequest->Sglist =
            TGuardedSgList({TBlockDataRef(data.data(), data.size())});
        auto writeResponse =
            storage
                ->WriteBlocksLocal(MakeIntrusive<TCallContext>(), writeRequest)
                .GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(writeResponse), writeResponse.GetError());
        UNIT_ASSERT_VALUES_EQUAL(data, written);
    }

    Y_UNIT_TEST(ShouldListRemoteMounts)
    {
        TPortManager portManager;
        ui16 port = portManager.GetPort(9001);

        auto service = std::make_shared<TTestService>();
        service->MountVolumeHandler =
            [&] (auto request) {
                NProto::TMountVolumeResponse response;
                if (request->GetDiskId() == "bad-disk") {
                    *response.MutableError() = MakeError(E_NOT_FOUND);
                }
                response.SetTabletHost("localhost");
                response.MutableVolume()->SetDiskId(request->GetDiskId());
                return MakeFuture(std::move(response));
            };
        service->UnmountVolumeHandler =
            [&] (auto request) {
                Y_UNUSED(request);
                return MakeFuture(NProto::TUnmountVolumeResponse());
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
        cfg.MutableCells(0)->SetTransport(NProto::CELL_DATA_TRANSPORT_GRPC);

        auto config = std::make_shared<TCellsConfig>(std::move(cfg));

        auto cellManager = CreateCellManager(
            config,
            testContext.Timer,
            testContext.Scheduler,
            testContext.Logging,
            testContext.Monitoring,
            testContext.TraceSerializer,
            testContext.ServerStats,
            CreateClientCertificateProvider(config),
            nullptr,
            CreateLocalService());

        server->Start();
        cellManager->Start();
        Y_DEFER {
            cellManager->Stop();
            server->Stop();
        };

        auto connectionOrError = cellManager
            ->CreateConnection(
                "xyz",
                {},
                std::make_shared<TClientAppConfig>(),
                nullptr)
            .GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(
            !HasError(connectionOrError),
            connectionOrError.GetError());
        auto connection = connectionOrError.ExtractResult();

        // nothing mounted through it yet
        UNIT_ASSERT_VALUES_EQUAL(0, cellManager->GetSnapshot().Mounts.size());

        auto mount = [&] (const TString& diskId)
        {
            auto request = std::make_shared<NProto::TMountVolumeRequest>();
            request->SetDiskId(diskId);
            request->MutableHeaders()->SetClientId("client-1");
            return connection->GetService()
                ->MountVolume(MakeIntrusive<TCallContext>(), request)
                .GetValue(TDuration::Seconds(5));
        };

        // a failed mount is not listed
        UNIT_ASSERT(HasError(mount("bad-disk")));
        UNIT_ASSERT_VALUES_EQUAL(0, cellManager->GetSnapshot().Mounts.size());

        auto response = mount("disk-1");
        UNIT_ASSERT_C(!HasError(response), response.GetError());

        auto mounts = cellManager->GetSnapshot().Mounts;
        UNIT_ASSERT_VALUES_EQUAL(1, mounts.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", mounts[0].DiskId);
        UNIT_ASSERT_VALUES_EQUAL("client-1", mounts[0].ClientId);
        UNIT_ASSERT_VALUES_EQUAL("xyz", mounts[0].CellId);
        UNIT_ASSERT_VALUES_EQUAL("localhost", mounts[0].Host);
        UNIT_ASSERT_VALUES_EQUAL("grpc", mounts[0].DataTransport);
        UNIT_ASSERT_VALUES_EQUAL("localhost", mounts[0].TabletHost);

        auto request = std::make_shared<NProto::TUnmountVolumeRequest>();
        request->SetDiskId("disk-1");
        request->MutableHeaders()->SetClientId("client-1");
        auto unmountResponse =
            connection->GetService()
                ->UnmountVolume(MakeIntrusive<TCallContext>(), request)
                .GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(
            !HasError(unmountResponse),
            unmountResponse.GetError());
        UNIT_ASSERT_VALUES_EQUAL(0, cellManager->GetSnapshot().Mounts.size());

        UNIT_ASSERT(!HasError(mount("disk-1")));
        UNIT_ASSERT_VALUES_EQUAL(1, cellManager->GetSnapshot().Mounts.size());

        // the mount is gone with its connection
        connection.reset();
        UNIT_ASSERT_VALUES_EQUAL(0, cellManager->GetSnapshot().Mounts.size());
    }

    Y_UNIT_TEST(ShouldPublishCellSensors)
    {
        TTestContext testContext;

        auto cfg = TCellConfigBuilder("abc", true)
            .AddCell("xyz", 9001, 0, 1, 1, {"host-1", "host-2"})
            .Build();
        auto config = std::make_shared<TCellsConfig>(std::move(cfg));

        auto cellManager = CreateCellManager(
            config,
            testContext.Timer,
            testContext.Scheduler,
            testContext.Logging,
            testContext.Monitoring,
            testContext.TraceSerializer,
            testContext.ServerStats,
            CreateClientCertificateProvider(config),
            nullptr,
            CreateLocalService());

        auto cell = testContext.Monitoring->GetCounters()
            ->FindSubgroup("counters", "blockstore");
        cell = cell ? cell->FindSubgroup("component", "cells") : nullptr;
        cell = cell ? cell->FindSubgroup("cell", "xyz") : nullptr;
        UNIT_ASSERT(cell);
        UNIT_ASSERT_VALUES_EQUAL(2, cell->GetCounter("HostsConfigured")->Val());
        UNIT_ASSERT(cell->FindCounter("HostsUnavailable"));
        UNIT_ASSERT(cell->FindCounter("Migrations"));
    }
}

}   // namespace NCloud::NBlockStore::NCells
