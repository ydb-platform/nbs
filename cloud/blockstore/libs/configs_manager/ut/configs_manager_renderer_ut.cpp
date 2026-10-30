/*******************************************************************************

Renderer output and monitoring metadata tests. Direct rendering checks source
columns and field presentation. A minimal actor fixture checks metadata from
ConfigsDispatcher deliveries without subscribers or an HTTP server.

*******************************************************************************/

#include "configs_manager_test_helpers.h"

#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/configs_manager/configs_manager.h>
#include <cloud/blockstore/libs/configs_manager/configs_manager_renderer.h>
#include <cloud/blockstore/libs/kikimr/components.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/control/immediate_control_board_impl.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/logger/null.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NActors;
using namespace NKikimr::NConsole;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 PrivateDatabaseConfigKind =
    NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem;

NProto::TBlockstoreConfig MakeConfig(ui32 writeBlobThreshold)
{
    NProto::TBlockstoreConfig config;
    config.MutableStorageService()->SetWriteBlobThreshold(writeBlobThreshold);
    return config;
}

// A minimal ConfigsManager runtime for renderer metadata from real deliveries.
// Each test starts with one static and one dynamic storage value. The runtime
// owns the manager; the fixture retains its holder only for failure injection.
class TRendererActorFixture: public NUnitTest::TBaseFixture
{
protected:
    // Actor runtime owning the manager and edge actors for this test.
    TTestActorRuntime Runtime;

    // Dispatcher edge actor supplying private configuration updates.
    TActorId Dispatcher;

    // Manager actor serving the rendered monitoring page.
    TActorId Manager;

    // Published startup configuration retained for preparation-failure tests.
    TBlockstoreConfigHolderPtr ConfigHolder;

    // Start a manager with the minimal sources and isolated critical logging.
    void SetUp(NUnitTest::TTestContext&) override;

    // Reset the critical logger installed for this test.
    void TearDown(NUnitTest::TTestContext&) override;

    // Deliver a payload, or remove the private source when the pointer is null.
    void SendNotification(
        std::shared_ptr<const google::protobuf::Message> config,
        ui64 cookie);

    // Synchronize with an accepted update before checking its rendering.
    void WaitForAck(ui64 cookie);

    // Read the renderer output after the manager processes queued deliveries.
    TString GetMonitoringPage();
};

// Start the manager with one overridden value, without consumer subscriptions.
void TRendererActorFixture::SetUp(NUnitTest::TTestContext&)
{
    // Isolate rejected-delivery logging from tests that inspect critical logs.
    SetCriticalEventsLog(TLog(MakeHolder<TNullLogBackend>()));
    Runtime.AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);
    NKikimr::SetupTabletServices(Runtime);
    Dispatcher = Runtime.AllocateEdgeActor();

    // Build only the sources and controls required by the manager and renderer.
    auto controls = std::make_shared<NStorage::TStorageConfigControls>();
    auto staticConfig = MakeConfig(100);
    auto dynamicConfig = MakeConfig(200);
    auto startupConfig =
        MergeBlockstoreConfigSources(staticConfig, dynamicConfig);
    ConfigHolder = std::make_shared<TBlockstoreConfigHolder>(
        MakeBlockstoreConfig(startupConfig, controls));
    controls->UpdateDefaults(startupConfig.GetStorageService());
    Manager = Runtime.Register(CreateConfigsManager({
        .ConfigHolder = ConfigHolder,
        .StaticConfig = std::move(staticConfig),
        .StartupConfig = std::move(startupConfig),
        .InitialDynamicConfig = std::move(dynamicConfig),
        .StorageConfigControls = std::move(controls),
        .ConfigsDispatcherId = Dispatcher,
    }));

    // Complete bootstrap before tests deliver a configuration or request HTML.
    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(Runtime.GrabEdgeEventRethrow<
                TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest>(handle));
}

// Reset the critical logger so its state cannot affect subsequent fixtures.
void TRendererActorFixture::TearDown(NUnitTest::TTestContext&)
{
    SetCriticalEventsLog(TLog());
}

// Deliver the private section or its removal through the dispatcher protocol.
void TRendererActorFixture::SendNotification(
    std::shared_ptr<const google::protobuf::Message> config,
    ui64 cookie)
{
    auto event = std::make_unique<TEvConsole::TEvConfigNotificationRequest>();
    if (config) {
        event->OpaqueConfigs.emplace(
            PrivateDatabaseConfigKind,
            std::move(config));
    }
    Runtime.Send(
        new IEventHandle(Manager, Dispatcher, event.release(), 0, cookie));
}

// Wait until an accepted configuration delivery has been processed.
void TRendererActorFixture::WaitForAck(ui64 cookie)
{
    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(
        Runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationResponse>(
            handle));
    UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
}

// Read the page through the actor without creating an HTTP server.
TString TRendererActorFixture::GetMonitoringPage()
{
    TStringStream output;
    NMonitoring::TMonService2HttpRequest
        request(&output, nullptr, nullptr, nullptr, {}, nullptr);
    const auto edge = Runtime.AllocateEdgeActor();
    Runtime.Send(
        new IEventHandle(Manager, edge, new NMon::TEvHttpInfo(request)));
    const auto response = Runtime.GrabEdgeEventRethrow<NMon::TEvHttpInfoRes>(
        edge,
        TDuration::Seconds(1));
    UNIT_ASSERT(response);
    response->Get()->Output(output);
    return output.Str();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigsManagerRendererTest)
{
    // Check that startup delivery is shown separately from absent runtime data.
    Y_UNIT_TEST_F(
        ShouldRenderStartupSourceBeforeRuntimeUpdate,
        TRendererActorFixture)
    {
        const auto html = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<th>Dynamic<br>(startup)</th><th>Dynamic<br>(runtime)</th>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.WriteBlobThreshold</td>"
            "<td>100</td><td>200</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>200</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "class='config-dynamic-total'>1</span>");
    }

    // Keep startup values and count only the latest accepted runtime source.
    Y_UNIT_TEST_F(
        ShouldRenderStartupAndLatestRuntimeSources,
        TRendererActorFixture)
    {
        ui64 cookie = 1;
        for (const ui32 value: {300, 400, 200}) {
            // Replace the whole runtime source, including a return to startup.
            SendNotification(
                std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(value)),
                cookie);
            WaitForAck(cookie++);

            // Retain startup history without counting it as a second override.
            const auto html = GetMonitoringPage();
            UNIT_ASSERT_STRING_CONTAINS(
                html,
                "StorageService.WriteBlobThreshold</td>"
                "<td>100</td><td>200</td><td>" +
                    ToString(value) + "</td><td>&mdash;</td><td>" +
                    ToString(value) + "</td>");
            UNIT_ASSERT_STRING_CONTAINS(
                html,
                "class='config-dynamic-total'>1</span>");
        }
    }

    // Show identical values in runtime after a new accepted delivery cookie.
    Y_UNIT_TEST_F(
        ShouldRenderUnchangedStartupValuesInRuntimeSource,
        TRendererActorFixture)
    {
        // Establish the initial delivery, then accept its values as an update.
        auto config =
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200));
        SendNotification(config, 1);
        WaitForAck(1);
        SendNotification(config, 2);
        WaitForAck(2);

        // Preserve startup and show the runtime copy without double counting.
        const auto html = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.WriteBlobThreshold</td>"
            "<td>100</td><td>200</td><td>200</td>"
            "<td>&mdash;</td><td>200</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "class='config-dynamic-total'>1</span>");
    }

    // Retain startup history when an accepted runtime source is empty.
    Y_UNIT_TEST_F(
        ShouldRenderStartupSourceAfterEmptyRuntimeConfig,
        TRendererActorFixture)
    {
        // Accept an explicitly present empty private configuration.
        SendNotification(std::make_shared<NProto::TBlockstoreConfig>(), 1);
        WaitForAck(1);

        // Clear current overrides while retaining their startup source.
        const auto html = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.WriteBlobThreshold</td>"
            "<td>100</td><td>200</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>100</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ">StorageService</a></td><td>"
            "<span class='config-zero'>&mdash;</span></td>");
    }

    // Show enum names and numbers in every source and ICB column.
    Y_UNIT_TEST(ShouldRenderEnumNamesAndNumbers)
    {
        // Give each source an explicit enum value and apply an ICB override.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetSSDCompactionType(
            NProto::CT_DEFAULT);
        auto* checksumFlags = staticConfig.MutableServer()
                                  ->MutableServerConfig()
                                  ->MutableChecksumFlags();
        checksumFlags->AddMediaKindsToValidateDataIntegrity(
            NCloud::NProto::STORAGE_MEDIA_SSD);
        checksumFlags->AddMediaKindsToValidateDataIntegrity(
            NCloud::NProto::STORAGE_MEDIA_HDD);
        auto startupDynamicConfig = MakeConfig(200);
        startupDynamicConfig.MutableStorageService()->SetSSDCompactionType(
            NProto::CT_LOAD);
        auto dynamicConfig = MakeConfig(300);
        dynamicConfig.MutableStorageService()->SetSSDCompactionType(
            NProto::CT_DEFAULT);
        const auto startupConfig =
            MergeBlockstoreConfigSources(staticConfig, startupDynamicConfig);
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        NConfig::TRuntimeConfigDiagnostics diagnostics;
        const auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            dynamicConfig,
            controls,
            diagnostics);
        controls->UpdateDefaults(config->GetStorageConfig()->GetConfigProto());
        NKikimr::TControlBoard board;
        controls->Register(board);
        TAtomic previous = 0;
        UNIT_ASSERT(
            !board.SetValue("BlockStore_SSDCompactionType", 1, previous));

        // Render startup and runtime sources alongside the effective override.
        const TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = startupDynamicConfig,
                    .UpdateStatus = EConfigUpdateStatus::Runtime,
                    .DynamicConfigPresent = true,
                },
        };
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Use the same format for raw values, ICB and Effective.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.SSDCompactionType</td>"
            "<td>CT_DEFAULT (0)</td><td>CT_LOAD (1)</td>"
            "<td>CT_DEFAULT (0)</td><td>CT_LOAD (1)</td><td>CT_LOAD (1)</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Server.ServerConfig.ChecksumFlags."
            "MediaKindsToValidateDataIntegrity[0]</td>"
            "<td>STORAGE_MEDIA_SSD (1)</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>STORAGE_MEDIA_SSD (1)</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Server.ServerConfig.ChecksumFlags."
            "MediaKindsToValidateDataIntegrity[1]</td>"
            "<td>STORAGE_MEDIA_HDD (3)</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>STORAGE_MEDIA_HDD (3)</td>");
    }

    // Check source columns, live ICB values, secret redaction and HTML
    // escaping.
    Y_UNIT_TEST(ShouldRenderConfigSourcesWithLiveIcbOverridesAndRedactedSecrets)
    {
        // Give the sources distinct values, secrets and HTML-sensitive text.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetSchemeShardDir("<static&>");
        staticConfig.MutableStorageService()
            ->SetFreshBlobByteCountFlushThreshold(4096);
        staticConfig.MutableServer()
            ->MutableServerConfig()
            ->SetNodeRegistrationToken("static-secret-value");

        auto dynamicConfig = MakeConfig(200);
        dynamicConfig.MutableServer()
            ->MutableServerConfig()
            ->SetNodeRegistrationToken("dynamic-secret-value");

        // Build the startup base before applying an operator override.
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        auto config =
            MakeStartupBlockstoreConfig(staticConfig, dynamicConfig, controls);
        controls->UpdateDefaults(config->GetStorageConfig()->GetConfigProto());
        NKikimr::TControlBoard controlBoard;
        controls->Register(controlBoard);
        TAtomic previous = 0;
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            300,
            previous));

        // Render all sources while preserving the live override.
        const TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = dynamicConfig,
                    .DynamicConfigPresent = true,
                },
        };
        const TString html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Check the source values and hide credentials and unescaped HTML.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<th>Parameter</th><th>Static</th>"
            "<th>Dynamic<br>(startup)</th><th>Dynamic<br>(runtime)</th>"
            "<th>ICB</th><th>Effective</th>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<th>Parameter</th><th>Static</th>"
            "<th>Dynamic<br>(startup)</th><th>Dynamic<br>(runtime)</th>"
            "<th>Effective</th>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td>Dynamic config</td><td>"
            "<span class='config-present'>present</span></td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "StorageService.WriteBlobThreshold");
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>100</td><td>200</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>300</td><td>300</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.FreshBlobByteCountFlushThreshold</td>"
            "<td>4096</td><td>&mdash;</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>4096</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "[redacted]");
        UNIT_ASSERT(!html.Contains("static-secret-value"));
        UNIT_ASSERT(!html.Contains("dynamic-secret-value"));
        UNIT_ASSERT(!html.Contains("<static&>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "&lt;static&amp;&gt;");
    }

    // Label unknown proto3 enum numbers in scalar and repeated fields.
    Y_UNIT_TEST(ShouldRenderUnknownEnumValues)
    {
        // Keep undeclared numbers in ordinary configuration fields.
        auto staticConfig = MakeConfig(100);
        auto* server = staticConfig.MutableServer()->MutableServerConfig();
        server->SetEncryptZeroPolicy(
            static_cast<NProto::EEncryptZeroPolicy>(2));
        server->MutableChecksumFlags()->AddMediaKindsToValidateDataIntegrity(
            static_cast<NCloud::NProto::EStorageMediaKind>(123));
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        const auto config =
            MakeStartupBlockstoreConfig(staticConfig, {}, controls);

        // Render the source and its unchanged effective protobuf values.
        const TBlockstoreConfigRenderer renderer;
        const auto html =
            renderer.RenderHtml(*config, staticConfig, {}, *controls);

        // Mark unknown values without claiming they were ignored.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Server.ServerConfig.EncryptZeroPolicy</td>"
            "<td>2 (unknown enum value)</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>2 (unknown enum value)</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Server.ServerConfig.ChecksumFlags."
            "MediaKindsToValidateDataIntegrity[0]</td>"
            "<td>123 (unknown enum value)</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>123 (unknown enum value)</td>");
    }

    // Display invalid enum overrides without applying them to Effective.
    Y_UNIT_TEST(ShouldRenderInvalidEnumOverrides)
    {
        // Keep an explicit valid enum as the fallback for an invalid override.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetSSDCompactionType(
            NProto::CT_LOAD);
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        const auto config =
            MakeStartupBlockstoreConfig(staticConfig, {}, controls);
        controls->UpdateDefaults(config->GetStorageConfig()->GetConfigProto());
        NKikimr::TControlBoard board;
        controls->Register(board);
        TAtomic previous = 0;
        const TBlockstoreConfigRenderer renderer;

        // Cover an unknown number and one that would narrow to a known enum.
        for (const i64 value: {i64{2}, (i64{1} << 32) + 1}) {
            board.SetValue("BlockStore_SSDCompactionType", value, previous);
            const auto html =
                renderer.RenderHtml(*config, staticConfig, {}, *controls);

            // Keep the entered integer visible and retain the valid fallback.
            const TString expected = TStringBuilder()
                << "StorageService.SSDCompactionType</td>"
                   "<td>CT_LOAD (1)</td><td>&mdash;</td><td>&mdash;</td><td>"
                << value
                << " (unknown enum value; ignored)</td><td>CT_LOAD (1)</td>";
            UNIT_ASSERT_STRING_CONTAINS(html, expected);
            UNIT_ASSERT_STRING_CONTAINS(html, "class='config-icb'>1</span>");
        }
    }

    // Show ordinary security settings and file names while hiding token values.
    Y_UNIT_TEST(ShouldRenderPublicSecuritySettingsAndRedactTokenValues)
    {
        // Configure public metadata, certificate paths and actual secrets.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetAuthorizationMode(
            NCloud::NProto::AUTHORIZATION_REQUIRE);
        staticConfig.MutableStorageService()->SetNodeRegistrationToken(
            "storage-token-secret");
        staticConfig.MutableLogbroker()->SetCaCertFilename(
            "/certs/logbroker.pem");
        staticConfig.MutableRootKms()->SetRootCertsFile("/certs/root.pem");
        staticConfig.MutableRootKms()->SetPrivateKeyFile("/keys/root.key");
        staticConfig.MutableRootKms()->SetKeyId("root-key-id");
        staticConfig.MutableEndpoint()->MutableClientConfig()->SetAuthToken(
            "endpoint-token-secret");
        staticConfig.MutableIamClient()->SetTokenAgentUnixSocket(
            "/run/token-agent.sock");
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        auto config = MakeStartupBlockstoreConfig(staticConfig, {}, controls);

        // Render the initial state without a dynamic source.
        const TBlockstoreConfigRenderer renderer;
        const auto html =
            renderer.RenderHtml(*config, staticConfig, {}, *controls);

        // Preserve ordinary values and omit raw secrets.
        for (const TStringBuf value:
             {
                 "AUTHORIZATION_REQUIRE",
                 "/certs/logbroker.pem",
                 "/certs/root.pem",
                 "/keys/root.key",
                 "root-key-id",
                 "/run/token-agent.sock",
             })
        {
            UNIT_ASSERT_STRING_CONTAINS(html, value);
        }
        UNIT_ASSERT(!html.Contains("storage-token-secret"));
        UNIT_ASSERT(!html.Contains("endpoint-token-secret"));
    }

    // Show an empty message and retain its section after a deferred removal.
    Y_UNIT_TEST(ShouldRenderEmptyMessageAfterDeferredRemoval)
    {
        // Enable lockdown through an explicitly empty startup message.
        const auto staticConfig = MakeConfig(100);
        auto startupDynamicConfig = MakeConfig(200);
        startupDynamicConfig.MutableLocalNVMe()->MutableLockdownConfig();
        startupDynamicConfig.MutableSpdkEnv();
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        const auto startupConfig =
            MergeBlockstoreConfigSources(staticConfig, startupDynamicConfig);
        auto config = MakeStartupBlockstoreConfig(
            staticConfig,
            startupDynamicConfig,
            controls);
        TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = startupDynamicConfig,
                    .DynamicConfigPresent = true,
                },
        };

        // Show explicit empty messages and omit automatically created sections.
        const auto startupHtml = renderer.RenderHtml(
            *config,
            staticConfig,
            startupDynamicConfig,
            *controls);
        UNIT_ASSERT_STRING_CONTAINS(
            startupHtml,
            "LocalNVMe.LockdownConfig</td>"
            "<td>&mdash;</td><td>{}</td><td>&mdash;</td><td>{}</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            startupHtml,
            ">LocalNVMe</a></td><td>"
            "<span class='config-dynamic-total'>1</span>");
        UNIT_ASSERT(!startupHtml.Contains(">RootKms</a>"));
        UNIT_ASSERT_STRING_CONTAINS(
            startupHtml,
            "<td>SpdkEnv</td>"
            "<td>&mdash;</td><td>{}</td><td>&mdash;</td><td>{}</td>");

        // Request removal at runtime while the frozen startup value remains.
        const auto dynamicConfig = MakeConfig(300);
        config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            dynamicConfig,
            controls,
            renderer.Data.RuntimeDiagnostics);
        renderer.Data.UpdateStatus = EConfigUpdateStatus::Runtime;
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Preserve the empty effective message and its deferred section count.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>"
            "LocalNVMe.LockdownConfig</td>"
            "<td>&mdash;</td><td>{}</td><td>&mdash;</td><td>{}</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ">LocalNVMe</a></td><td><span class='config-zero'>&mdash;</span>"
            " / <span class='config-dynamic-deferred'>deferred: 1</span>");
    }

    // Distinguish an empty selected oneof branch from absent and filled
    // sources.
    Y_UNIT_TEST(ShouldRenderEmptyOneofMessageAlongsidePopulatedSource)
    {
        // Select an empty metadata branch instead of the static JWT branch.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableLogbroker()->SetAddress("broker");
        staticConfig.MutableLogbroker()->MutableIamJwtFile()->SetJwtFilename(
            "/auth/jwt");
        auto startupDynamicConfig = MakeConfig(200);
        startupDynamicConfig.MutableLogbroker()->MutableIamMetadataServer();
        auto dynamicConfig = MakeConfig(300);
        dynamicConfig.MutableLogbroker()
            ->MutableIamMetadataServer()
            ->SetEndpoint("runtime-endpoint");
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        const auto startupConfig =
            MergeBlockstoreConfigSources(staticConfig, startupDynamicConfig);
        TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = startupDynamicConfig,
                    .UpdateStatus = EConfigUpdateStatus::Runtime,
                    .DynamicConfigPresent = true,
                },
        };

        // Keep the frozen empty branch while displaying the requested fields.
        const auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            dynamicConfig,
            controls,
            renderer.Data.RuntimeDiagnostics);
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Mark only empty messages; populated values remain in child rows.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>"
            "Logbroker.IamMetadataServer</td>"
            "<td>&mdash;</td><td>{}</td><td>&mdash;</td><td>{}</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>"
            "Logbroker.IamMetadataServer.Endpoint</td>"
            "<td>&mdash;</td><td>&mdash;</td>"
            "<td>&quot;runtime-endpoint&quot;</td><td>&mdash;</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>Logbroker.Address</td>");
    }

    // Highlight both empty branches for one rejected switch, counting it once.
    Y_UNIT_TEST(ShouldHighlightEmptyOneofBranchesForDeferredSwitch)
    {
        // Retain the startup branch while the runtime source requests another.
        const NProto::TBlockstoreConfig staticConfig;
        auto startupDynamicConfig = MakeConfig(200);
        startupDynamicConfig.MutableLogbroker()->MutableIamMetadataServer();
        auto dynamicConfig = MakeConfig(300);
        dynamicConfig.MutableLogbroker()->MutableIamJwtFile();
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        const auto config = MakeStartupBlockstoreConfig(
            staticConfig,
            startupDynamicConfig,
            controls);
        TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = startupDynamicConfig,
                    .UpdateStatus = EConfigUpdateStatus::Runtime,
                    .DynamicConfigPresent = true,
                },
        };

        // Supply a fixed-selection diagnostic directly to the renderer.
        renderer.Data.RuntimeDiagnostics.IgnoredPaths.emplace(
            "Logbroker.AuthConfig",
            NConfig::ERuntimeConfigIgnoreReason::OneofSwitchForbidden);
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Mark the retained and rejected branches as one deferred group change.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td "
            "class='config-dynamic-deferred'>Logbroker.IamMetadataServer</td>"
            "<td>&mdash;</td><td>{}</td><td>&mdash;</td><td>{}</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>Logbroker.IamJwtFile</td>"
            "<td>&mdash;</td><td>&mdash;</td><td>{}</td><td>&mdash;</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ">Logbroker</a></td><td>"
            "<span class='config-dynamic-total'>1</span>"
            " / <span class='config-dynamic-deferred'>deferred: 1</span>");
    }

    // Count explicit parameters, deferred changes and ICB overrides separately.
    Y_UNIT_TEST(ShouldRenderDynamicDeferredAndIcbCounters)
    {
        // Fix two startup values and request one scalar and one collection
        // change.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetListVolumesConcurrency(10);
        staticConfig.MutableStorageService()->SetMaxSSDGroupReadIops(12000);
        const auto startupConfig =
            MergeBlockstoreConfigSources(staticConfig, MakeConfig(200));
        auto dynamicConfig = MakeConfig(300);
        auto* storage = dynamicConfig.MutableStorageService();
        storage->SetListVolumesConcurrency(30);
        storage->SetMaxSSDGroupReadIops(12000);
        storage->AddKnownSpareNodes("node-a");
        storage->AddKnownSpareNodes("node-b");
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        NConfig::TRuntimeConfigDiagnostics diagnostics;
        auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            dynamicConfig,
            controls,
            diagnostics);

        // Override a permitted field after publishing its configured default.
        controls->UpdateDefaults(config->GetStorageConfig()->GetConfigProto());
        NKikimr::TControlBoard board;
        controls->Register(board);
        TAtomic previous = 0;
        UNIT_ASSERT(
            !board.SetValue("BlockStore_WriteBlobThreshold", 600, previous));
        const TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = MakeConfig(200),
                    .UpdateStatus = EConfigUpdateStatus::Runtime,
                    .DynamicConfigPresent = true,
                    .RuntimeDiagnostics = std::move(diagnostics),
                },
        };
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Count the collection once and exclude both ICB and unchanged RO
        // values from the two changes that require a restart.
        const auto summaryStart = html.find(">StorageService</a>");
        UNIT_ASSERT(summaryStart != TString::npos);
        const auto summary = html.substr(
            summaryStart,
            html.find("</tr>", summaryStart) - summaryStart);
        UNIT_ASSERT_STRING_CONTAINS(
            summary,
            "class='config-dynamic-total'>4</span>");
        UNIT_ASSERT_STRING_CONTAINS(summary, "deferred: 2</span>");
        UNIT_ASSERT_STRING_CONTAINS(summary, "class='config-icb'>1</span>");

        // Mark deferred scalar and collection rows, retaining ordinary names
        // for an ICB override and an unchanged startup-only value.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>"
            "StorageService.ListVolumesConcurrency</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>"
            "StorageService.KnownSpareNodes[0]</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>"
            "StorageService.KnownSpareNodes[1]</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td>StorageService.WriteBlobThreshold</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td>StorageService.MaxSSDGroupReadIops</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<p class='config-deferred-hint'>"
            "<span class='config-dynamic-deferred'>Deferred</span> "
            "parameters cannot be updated at runtime and take effect "
            "after restart.</p>");
    }

    // Align repeated and map values and mark elements of deferred collections.
    Y_UNIT_TEST(ShouldRenderRepeatedAndMapElementsWithDeferredHighlighting)
    {
        // Give lists different lengths and maps overlapping and distinct keys.
        auto staticConfig = MakeConfig(100);
        auto* first =
            staticConfig.MutableStorageService()->AddLinkedDiskFillBandwidth();
        first->SetReadBandwidth(10);
        first->SetWriteBandwidth(11);
        staticConfig.MutableStorageService()
            ->AddLinkedDiskFillBandwidth()
            ->SetReadBandwidth(20);
        auto* staticThreshold =
            staticConfig.MutableDiagnostics()->AddRequestThresholds();
        (*staticThreshold->MutableByRequestType())["shared"] = 100;
        (*staticThreshold->MutableByRequestType())["static-only"] = 0;
        auto dynamicConfig = MakeConfig(200);
        dynamicConfig.MutableStorageService()
            ->AddLinkedDiskFillBandwidth()
            ->SetReadBandwidth(30);
        for (int i = 0; i != 17; ++i) {
            dynamicConfig.MutableStorageService()->AddKnownSpareNodes(
                "node-" + ToString(i));
        }
        auto* dynamicThreshold =
            dynamicConfig.MutableDiagnostics()->AddRequestThresholds();
        (*dynamicThreshold->MutableByRequestType())["dynamic-only"] = 200;
        (*dynamicThreshold->MutableByRequestType())["shared"] = 300;
        auto startupDynamicConfig = MakeConfig(150);
        startupDynamicConfig.MutableStorageService()
            ->AddLinkedDiskFillBandwidth()
            ->SetReadBandwidth(15);
        auto* startupThreshold =
            startupDynamicConfig.MutableDiagnostics()->AddRequestThresholds();
        (*startupThreshold->MutableByRequestType())["shared"] = 150;
        (*startupThreshold->MutableByRequestType())["startup-only"] = 400;
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        auto config =
            MakeStartupBlockstoreConfig(staticConfig, dynamicConfig, controls);

        // Apply collection-level diagnostics to every displayed child row.
        NConfig::TRuntimeConfigDiagnostics diagnostics;
        for (const auto path:
             {
                 "StorageService.LinkedDiskFillBandwidth[]",
                 "Diagnostics.RequestThresholds[]",
             })
        {
            diagnostics.IgnoredPaths.emplace(
                path,
                NConfig::ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        }

        // Render every element, including a zero map value and the seventeenth
        // item.
        const TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = std::move(startupDynamicConfig),
                    .UpdateStatus = EConfigUpdateStatus::Runtime,
                    .DynamicConfigPresent = true,
                    .RuntimeDiagnostics = std::move(diagnostics),
                },
        };
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Compare matching indices and keys while retaining missing source
        // cells.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.LinkedDiskFillBandwidth[0].ReadBandwidth</td>"
            "<td>10</td><td>15</td><td>30</td>"
            "<td class='config-icb-unavailable'></td><td>10</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.LinkedDiskFillBandwidth[1].ReadBandwidth</td>"
            "<td>20</td><td>&mdash;</td><td>&mdash;</td>"
            "<td class='config-icb-unavailable'></td><td>20</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.KnownSpareNodes[16]</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Diagnostics.RequestThresholds[0].ByRequestType[&quot;shared&quot;]"
            "</td>"
            "<td>100</td><td>150</td><td>300</td><td>100</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Diagnostics.RequestThresholds[0].ByRequestType[&quot;static-only&"
            "quot;]</td>"
            "<td>0</td><td>&mdash;</td><td>&mdash;</td><td>0</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Diagnostics.RequestThresholds[0].ByRequestType[&quot;dynamic-only&"
            "quot;]</td>"
            "<td>&mdash;</td><td>&mdash;</td><td>200</td><td>&mdash;</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Diagnostics.RequestThresholds[0].ByRequestType[&quot;startup-only&"
            "quot;]</td>"
            "<td>&mdash;</td><td>400</td><td>&mdash;</td><td>&mdash;</td>");

        // Highlight nested messages and map values from rejected collections.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>"
            "StorageService.LinkedDiskFillBandwidth[0].ReadBandwidth</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td class='config-dynamic-deferred'>"
            "Diagnostics.RequestThresholds[0].ByRequestType[&quot;shared&quot;]"
            "</td>");
    }

    // Preserve the renderer startup time through identical initial deliveries.
    Y_UNIT_TEST_F(
        ShouldKeepRendererStartupTimeForIdenticalStartupDeliveries,
        TRendererActorFixture)
    {
        // Observe the startup page before receiving dispatcher updates.
        const auto startupTime = Runtime.GetCurrentTime().ToString();
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            startupTime + ", startup");

        // Keep the same startup label and time for repeated initial delivery.
        for (int i = 0; i != 2; ++i) {
            Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            SendNotification(
                std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200)),
                1);
            WaitForAck(1);
            const auto html = GetMonitoringPage();
            UNIT_ASSERT_STRING_CONTAINS(html, startupTime + ", startup</td>");
            UNIT_ASSERT_STRING_CONTAINS(
                html,
                "StorageService.WriteBlobThreshold</td>"
                "<td>100</td><td>200</td><td>&mdash;</td>"
                "<td>&mdash;</td><td>200</td>");
        }
    }

    // Render a changed first dispatcher delivery with its runtime receipt time.
    Y_UNIT_TEST_F(
        ShouldRenderReceiptTimeForFirstChangedDispatcherUpdate,
        TRendererActorFixture)
    {
        // Deliver a new source immediately after startup.
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        const auto receiptTime = Runtime.GetCurrentTime().ToString();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            1);
        WaitForAck(1);

        // Attribute the changed values to runtime rather than startup.
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            receiptTime + ", runtime</td>");
    }

    // Render the new receipt time for a delivery matching the accepted runtime
    // source.
    Y_UNIT_TEST_F(
        ShouldRenderReceiptTimeForUnchangedRuntimeDelivery,
        TRendererActorFixture)
    {
        // Establish a runtime source, then deliver it again with a new cookie.
        auto config =
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
        SendNotification(config, 1);
        WaitForAck(1);
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        const auto receiptTime = Runtime.GetCurrentTime().ToString();
        SendNotification(config, 2);
        WaitForAck(2);

        // Keep the runtime origin and explicitly show unchanged values.
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            receiptTime + ", runtime (unchanged)</td>");
    }

    // Render a return to the original startup values as a runtime replacement.
    Y_UNIT_TEST_F(
        ShouldRenderRuntimeForReturnToStartupValues,
        TRendererActorFixture)
    {
        // Replace the startup source and then restore its original values.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            1);
        WaitForAck(1);
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        const auto receiptTime = Runtime.GetCurrentTime().ToString();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200)),
            2);
        WaitForAck(2);

        // Show the actual delivery time and runtime origin for the restoration.
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            receiptTime + ", runtime</td>");
    }

    // Render source removal as a runtime update with an absent dynamic source.
    Y_UNIT_TEST_F(
        ShouldRenderAbsentDynamicSourceAfterRemoval,
        TRendererActorFixture)
    {
        // Remove the startup dynamic source after advancing the receipt clock.
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        const auto receiptTime = Runtime.GetCurrentTime().ToString();
        SendNotification(nullptr, 1);
        WaitForAck(1);

        // Render both the runtime update and the absent source state.
        const auto html = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(html, receiptTime + ", runtime</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<td>Dynamic config</td><td>"
            "<span class='config-absent'>absent</span></td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.WriteBlobThreshold</td>"
            "<td>100</td><td>200</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>100</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ">StorageService</a></td><td>"
            "<span class='config-zero'>&mdash;</span></td>");
    }

    // Render startup rejection without replacing the accepted startup metadata.
    Y_UNIT_TEST_F(
        ShouldRenderStartupRejectionWithAcceptedTime,
        TRendererActorFixture)
    {
        // Reject a delivery after startup without accepting another source.
        const auto startupTime = Runtime.GetCurrentTime().ToString();
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        SendNotification(
            std::make_shared<NCloud::NProto::TError>(
                MakeError(E_ARGUMENT, "invalid startup delivery")),
            1);

        // Append the reason to the retained startup timestamp and origin.
        const auto html = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            startupTime +
                ", startup <span class='config-update-rejected'>"
                "- rejected: Failed to parse PrivateDatabaseConfig "
                "from CMS: E_ARGUMENT invalid startup delivery</span>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.WriteBlobThreshold</td>"
            "<td>100</td><td>200</td><td>&mdash;</td>"
            "<td>&mdash;</td><td>200</td>");
    }

    // Render an escaped rejection reason alongside the last accepted runtime
    // metadata.
    Y_UNIT_TEST_F(
        ShouldRenderEscapedRuntimeRejectionWithAcceptedTime,
        TRendererActorFixture)
    {
        // Accept one source before receiving an HTML-sensitive error reason.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            1);
        WaitForAck(1);
        const auto acceptedTime = Runtime.GetCurrentTime().ToString();
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        SendNotification(
            std::make_shared<NCloud::NProto::TError>(
                MakeError(E_ARGUMENT, "invalid private config <&>")),
            2);

        // Keep accepted time and origin while escaping the rendered error.
        const auto html = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            acceptedTime +
                ", runtime <span class='config-update-rejected'>"
                "- rejected: Failed to parse PrivateDatabaseConfig "
                "from CMS: E_ARGUMENT invalid private config "
                "&lt;&amp;&gt;</span>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "StorageService.WriteBlobThreshold</td>"
            "<td>100</td><td>200</td><td>300</td>"
            "<td>&mdash;</td><td>300</td>");
    }

    // Clear the renderer rejection when a changed source is accepted.
    Y_UNIT_TEST_F(
        ShouldClearRendererRejectionOnAcceptedChangedConfig,
        TRendererActorFixture)
    {
        // Display a rejection before receiving a valid changed source.
        SendNotification(
            std::make_shared<NCloud::NProto::TError>(
                MakeError(E_ARGUMENT, "invalid delivery")),
            1);
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            "invalid delivery</span>");
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        const auto receiptTime = Runtime.GetCurrentTime().ToString();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            2);
        WaitForAck(2);

        // Render the accepted update with no remaining rejection suffix.
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            receiptTime + ", runtime</td>");
    }

    // Clear the renderer rejection when accepted values match the runtime
    // source.
    Y_UNIT_TEST_F(
        ShouldClearRendererRejectionOnAcceptedUnchangedConfig,
        TRendererActorFixture)
    {
        // Establish a runtime source and observe a subsequent rejected
        // delivery.
        auto config =
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
        SendNotification(config, 1);
        WaitForAck(1);
        SendNotification(
            std::make_shared<NCloud::NProto::TError>(
                MakeError(E_ARGUMENT, "invalid delivery")),
            2);
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            "invalid delivery</span>");

        // Accept identical values and remove the error from the new receipt
        // time.
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        const auto receiptTime = Runtime.GetCurrentTime().ToString();
        SendNotification(config, 3);
        WaitForAck(3);
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            receiptTime + ", runtime (unchanged)</td>");
    }

    // Clear the renderer rejection when a new delivery matches startup values.
    Y_UNIT_TEST_F(
        ShouldClearRendererRejectionOnAcceptedStartupConfig,
        TRendererActorFixture)
    {
        // Reject the initial delivery while retaining the startup
        // configuration.
        SendNotification(
            std::make_shared<NCloud::NProto::TError>(
                MakeError(E_ARGUMENT, "invalid delivery")),
            1);
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            "invalid delivery</span>");

        // Accept the startup values with a new cookie and show their receipt
        // time.
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        const auto receiptTime = Runtime.GetCurrentTime().ToString();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200)),
            2);
        WaitForAck(2);
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            receiptTime + ", runtime (unchanged)</td>");
    }

    // Render an actual preparation failure alongside the accepted startup
    // state.
    Y_UNIT_TEST_F(
        ShouldRenderPreparationFailureAlongsideStartupConfig,
        TRendererActorFixture)
    {
        // Fail one adapter read during preparation without subscribing
        // consumers.
        ConfigHolder->Set(
            MakeIntrusive<TFailingBlockstoreConfig>(ConfigHolder->Get()));
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            1);

        // Display the preparation reason while keeping the startup origin.
        const auto html = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ", startup <span class='config-update-rejected'>- rejected: "
            "Failed to apply PrivateDatabaseConfig from CMS: ");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "test configuration preparation failure");
    }

    // Clear the renderer preparation error when the same source succeeds on
    // retry.
    Y_UNIT_TEST_F(
        ShouldClearRendererPreparationFailureAfterSuccessfulRetry,
        TRendererActorFixture)
    {
        // Display the failure before retrying the source with another cookie.
        ConfigHolder->Set(
            MakeIntrusive<TFailingBlockstoreConfig>(ConfigHolder->Get()));
        auto config =
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
        SendNotification(config, 1);
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            "test configuration preparation failure");

        // Render the accepted retry without retaining the preparation error.
        SendNotification(config, 2);
        WaitForAck(2);
        UNIT_ASSERT_STRING_CONTAINS(GetMonitoringPage(), ", runtime</td>");
    }

    // Render the unexpected payload type without exposing payload values.
    Y_UNIT_TEST_F(
        ShouldRenderUnexpectedPayloadErrorWithoutPayloadValues,
        TRendererActorFixture)
    {
        // Supply only the unexpected type and one value that must remain
        // private.
        auto payload = std::make_shared<NProto::TStorageServiceConfig>();
        payload->SetNodeType("private-secret-value");
        SendNotification(payload, 1);

        // Display the protocol reason while retaining payload confidentiality.
        const auto html = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<span class='config-update-rejected'>- rejected: "
            "Internal error: received an unexpected "
            "PrivateDatabaseConfig payload type ");
        UNIT_ASSERT(!html.Contains("private-secret-value"));
    }

    // Render an explicit null payload as a rejection rather than source
    // removal.
    Y_UNIT_TEST_F(ShouldRenderNullPayloadError, TRendererActorFixture)
    {
        // Deliver a present private section whose message pointer is null.
        auto event =
            std::make_unique<TEvConsole::TEvConfigNotificationRequest>();
        event->OpaqueConfigs.emplace(PrivateDatabaseConfigKind, nullptr);
        Runtime.Send(
            new IEventHandle(Manager, Dispatcher, event.release(), 0, 1));

        // Display the exact protocol reason from the rejected delivery.
        UNIT_ASSERT_STRING_CONTAINS(
            GetMonitoringPage(),
            "<span class='config-update-rejected'>- rejected: "
            "Internal error: received a null PrivateDatabaseConfig "
            "payload from ConfigsDispatcher</span>");
    }
}

}   // namespace NCloud::NBlockStore
