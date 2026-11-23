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

#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/logger/null.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/strbuf.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/split.h>
#include <util/string/subst.h>

#include <google/protobuf/descriptor.h>

#include <optional>
#include <regex>
#include <string>
#include <utility>

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

// Presentation flags extracted by GetParameterRow from cell classes and labels.
// Default flags describe a row without highlights or ignored-value labels.
struct TParameterRowStyle
{
    // Restart-required highlight on the parameter-name cell.
    bool Deferred = false;

    // Ignored static-only label after a line break in startup Dynamic.
    bool StartupDynamicIgnored = false;

    // Ignored static-only label after a line break in runtime Dynamic.
    bool RuntimeDynamicIgnored = false;

    // Unavailable ICB cell style; false when the table has no ICB column.
    bool IcbUnavailable = false;
};

// Configuration values and presentation flags owned by one rendered row.
struct TParameterRow
{
    // Full parameter path extracted from the first cell.
    TString ParameterName;

    // Value from the static configuration.
    TString StaticValue;

    // Value from the startup dynamic configuration.
    TString StartupDynamicValue;

    // Value from the accepted runtime dynamic configuration.
    TString RuntimeDynamicValue;

    // ICB cell text, or nullopt when the table has no ICB column.
    std::optional<TString> IcbValue;

    // Effective configuration value.
    TString EffectiveValue;

    // Presentation flags extracted from this row's cell classes and labels.
    TParameterRowStyle Style;
};

// Extract a unique renderer row by trying layouts with and without ICB.
// Support the renderer's cell attributes, inline labels and escaping entities.
TParameterRow GetParameterRow(const TString& html, TStringBuf parameterName)
{
    // Locate the parameter name and take its enclosing row.
    const TString marker = TStringBuilder()
                           << '>' << EncodeHtmlPcdata(parameterName) << "</td>";
    const auto namePos = html.find(marker);
    UNIT_ASSERT_C(
        namePos != TString::npos,
        "Missing row for " << parameterName);
    UNIT_ASSERT_C(
        html.find(marker, namePos + marker.size()) == TString::npos,
        "Duplicate row for " << parameterName);
    const auto begin = html.rfind("<tr", namePos);
    const auto end = html.find("</tr>", namePos);
    UNIT_ASSERT_C(
        begin != TString::npos && end != TString::npos,
        "Unclosed row for " << parameterName);
    const TStringBuf row(html.data() + begin, end + 5 - begin);

    // Capture three groups per cell: attributes, value and ignored label.
    const auto makePattern = [](size_t cellCount)
    {
        const char* cell =
            "<td([^>]*)>([^<]*)"
            "(<br><span class=['\"]config-dynamic-ignored['\"]>"
            "static-only, ignored</span>)?</td>\\s*";
        std::string pattern = R"(<tr[^>]*>\s*)";
        for (size_t i = 0; i != cellCount; ++i) {
            pattern += cell;
        }
        return std::regex(pattern + "</tr>");
    };
    std::cmatch cells;
    const bool hasIcb =
        std::regex_match(row.begin(), row.end(), cells, makePattern(6));

    // Preserve the captured six-cell row when it matches.
    const bool hasNoIcb =
        !hasIcb &&
        std::regex_match(row.begin(), row.end(), cells, makePattern(5));
    UNIT_ASSERT_C(
        hasIcb || hasNoIcb,
        "Unexpected row layout for " << parameterName);

    // Decode values separately from their ignored-value labels.
    const auto getText = [&cells](size_t index)
    {
        const auto& value = cells[index * 3 - 1];
        TString text(value.first, value.length());
        SubstGlobal(text, "&mdash;", "—");
        return DecodeHtmlPcdata(text);
    };
    const auto isIgnored = [&cells](size_t index)
    {
        return cells[index * 3].matched;
    };

    // Read CSS class tokens from the captured cell attributes.
    const std::regex classAttribute(
        R"re((?:^|\s)class\s*=\s*['"]([^'"]*)['"])re");
    const auto hasClass =
        [&cells, &classAttribute](size_t index, TStringBuf expectedClass)
    {
        std::cmatch attribute;
        const auto& attributes = cells[index * 3 - 2];
        if (!std::regex_search(
                attributes.first,
                attributes.second,
                attribute,
                classAttribute))
        {
            return false;
        }
        const TStringBuf classes(attribute[1].first, attribute[1].second);
        for (const auto token: StringSplitter(classes).SplitBySet(" \t\r\n\f"))
        {
            if (token.Token() == expectedClass) {
                return true;
            }
        }
        return false;
    };

    // Keep ignored labels confined to the two Dynamic source cells.
    UNIT_ASSERT_C(
        !isIgnored(1) && !isIgnored(2) && !isIgnored(hasIcb ? 6 : 5) &&
            (!hasIcb || !isIgnored(5)),
        "Ignored label outside Dynamic cells for " << parameterName);
    UNIT_ASSERT_VALUES_EQUAL_C(getText(1), parameterName, parameterName);
    return {
        .ParameterName = getText(1),
        .StaticValue = getText(2),
        .StartupDynamicValue = getText(3),
        .RuntimeDynamicValue = getText(4),
        .IcbValue = hasIcb ? std::optional<TString>(getText(5)) : std::nullopt,
        .EffectiveValue = getText(hasIcb ? 6 : 5),
        .Style =
            {
                .Deferred = hasClass(1, "config-dynamic-deferred"),
                .StartupDynamicIgnored = isIgnored(3),
                .RuntimeDynamicIgnored = isIgnored(4),
                .IcbUnavailable =
                    hasIcb && hasClass(5, "config-icb-unavailable"),
            },
    };
}

// Compare all source cells of a parameter row with their expected text.
void AssertParameterRow(
    const TParameterRow& actual,
    const TParameterRow& expected)
{
    UNIT_ASSERT_VALUES_EQUAL_C(
        actual.ParameterName,
        expected.ParameterName,
        expected.ParameterName);
    UNIT_ASSERT_VALUES_EQUAL_C(
        actual.StaticValue,
        expected.StaticValue,
        expected.ParameterName << ": Static");
    UNIT_ASSERT_VALUES_EQUAL_C(
        actual.StartupDynamicValue,
        expected.StartupDynamicValue,
        expected.ParameterName << ": Dynamic (startup)");
    UNIT_ASSERT_VALUES_EQUAL_C(
        actual.RuntimeDynamicValue,
        expected.RuntimeDynamicValue,
        expected.ParameterName << ": Dynamic (runtime)");
    UNIT_ASSERT_VALUES_EQUAL_C(
        actual.IcbValue.has_value(),
        expected.IcbValue.has_value(),
        expected.ParameterName << ": ICB column presence");
    if (expected.IcbValue) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            *actual.IcbValue,
            *expected.IcbValue,
            expected.ParameterName << ": ICB");
    }
    UNIT_ASSERT_VALUES_EQUAL_C(
        actual.EffectiveValue,
        expected.EffectiveValue,
        expected.ParameterName << ": Effective");
}

// Extract a parameter row and compare all source values with the expected text.
void AssertParameterRow(const TString& html, const TParameterRow& expected)
{
    AssertParameterRow(GetParameterRow(html, expected.ParameterName), expected);
}

// Compare the presentation flags of an already parsed parameter row.
void AssertParameterRowStyle(
    const TParameterRow& row,
    const TParameterRowStyle& expected)
{
    UNIT_ASSERT_VALUES_EQUAL_C(
        row.Style.Deferred,
        expected.Deferred,
        row.ParameterName << ": parameter-name deferred style");
    UNIT_ASSERT_VALUES_EQUAL_C(
        row.Style.StartupDynamicIgnored,
        expected.StartupDynamicIgnored,
        row.ParameterName << ": Dynamic (startup) ignored label");
    UNIT_ASSERT_VALUES_EQUAL_C(
        row.Style.RuntimeDynamicIgnored,
        expected.RuntimeDynamicIgnored,
        row.ParameterName << ": Dynamic (runtime) ignored label");
    UNIT_ASSERT_VALUES_EQUAL_C(
        row.Style.IcbUnavailable,
        expected.IcbUnavailable,
        row.ParameterName << ": unavailable ICB style");
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
    // Verify that every top-level field is a singular message: the renderer
    // traverses all configuration sections using singular message accessors.
    Y_UNIT_TEST(ShouldHaveOnlySingularMessageTopLevelSections)
    {
        const auto* descriptor = NProto::TBlockstoreConfig::descriptor();
        for (int i = 0; i != descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            const TString errorMessage =
                TStringBuilder()
                << "Renderer invariant violated: " << field->full_name()
                << " must be a singular message; got "
                << (field->is_repeated() ? "repeated " : "")
                << field->cpp_type_name();
            UNIT_ASSERT_C(!field->is_repeated(), errorMessage);
            UNIT_ASSERT_C(
                field->cpp_type() ==
                    google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE,
                errorMessage);
        }
    }

    // Verify that ignored static-only values are labeled only in their startup
    // or runtime Dynamic cells, without labels in Static or Effective cells.
    Y_UNIT_TEST(ShouldMarkIgnoredStaticOnlyValuesInDynamicCells)
    {
        // Build startup sources with ignored scalar and empty-message values.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetNodeType("static-node");
        staticConfig.MutableStorageService()->SetSchemeShardDir("/static");
        staticConfig.MutableDiskAgent()->SetDedicatedDiskAgent(true);
        staticConfig.MutableDiskAgent()->SetEnabled(false);
        auto startupDynamicConfig = MakeConfig(200);
        startupDynamicConfig.MutableStorageService()->SetNodeType(
            "startup-node");
        startupDynamicConfig.MutableStorageService()
            ->MutableConfigDispatcherSettings();
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        const auto startup = MakeStartupBlockstoreConfig(
            staticConfig,
            startupDynamicConfig,
            controls,
            /*extraParameters=*/{});
        TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = startupDynamicConfig,
                    .DynamicConfigPresent = true,
                    .CurrentConfigPreparationDiagnostics =
                        startup.NormalizationDiagnostics,
                    .StartupDynamicConfigNormalizationDiagnostics =
                        startup.NormalizationDiagnostics,
                },
        };

        // Check startup labels while the runtime Dynamic column is absent.
        auto html = renderer.RenderHtml(
            *startup.Config,
            staticConfig,
            startupDynamicConfig,
            *controls);

        const auto startupNodeRow =
            GetParameterRow(html, "StorageService.NodeType");
        AssertParameterRow(
            startupNodeRow,
            {
                .ParameterName = "StorageService.NodeType",
                .StaticValue = "\"static-node\"",
                .StartupDynamicValue = "\"startup-node\"",
                .RuntimeDynamicValue = "—",
                .IcbValue = "",
                .EffectiveValue = "\"static-node\"",
            });
        AssertParameterRowStyle(
            startupNodeRow,
            {.StartupDynamicIgnored = true, .IcbUnavailable = true});

        const auto dispatcherRow =
            GetParameterRow(html, "StorageService.ConfigDispatcherSettings");
        AssertParameterRow(
            dispatcherRow,
            {
                .ParameterName = "StorageService.ConfigDispatcherSettings",
                .StaticValue = "—",
                .StartupDynamicValue = "{}",
                .RuntimeDynamicValue = "—",
                .IcbValue = "",
                .EffectiveValue = "—",
            });
        AssertParameterRowStyle(
            dispatcherRow,
            {.StartupDynamicIgnored = true, .IcbUnavailable = true});

        // Replace Dynamic with other ignored fields and a populated message.
        auto dynamicConfig = MakeConfig(300);
        dynamicConfig.MutableStorageService()->SetSchemeShardDir("/runtime");
        dynamicConfig.MutableStorageService()
            ->MutableConfigDispatcherSettings()
            ->MutableAllowList()
            ->AddNames("LogConfigItem");
        dynamicConfig.MutableDiskAgent()->SetEnabled(true);
        const auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startup.Proto,
            dynamicConfig,
            controls,
            /*extraParameters=*/{},
            renderer.Data.CurrentConfigPreparationDiagnostics);
        renderer.Data.UpdateStatus = EConfigUpdateStatus::Runtime;
        html = renderer
                   .RenderHtml(*config, staticConfig, dynamicConfig, *controls);

        // Keep each label in its source column, including message children.
        const auto retainedNodeRow =
            GetParameterRow(html, "StorageService.NodeType");
        AssertParameterRow(
            retainedNodeRow,
            {
                .ParameterName = "StorageService.NodeType",
                .StaticValue = "\"static-node\"",
                .StartupDynamicValue = "\"startup-node\"",
                .RuntimeDynamicValue = "—",
                .IcbValue = "",
                .EffectiveValue = "\"static-node\"",
            });
        AssertParameterRowStyle(
            retainedNodeRow,
            {.StartupDynamicIgnored = true, .IcbUnavailable = true});

        const auto schemeShardRow =
            GetParameterRow(html, "StorageService.SchemeShardDir");
        AssertParameterRow(
            schemeShardRow,
            {
                .ParameterName = "StorageService.SchemeShardDir",
                .StaticValue = "\"/static\"",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "\"/runtime\"",
                .IcbValue = "",
                .EffectiveValue = "\"/static\"",
            });
        AssertParameterRowStyle(
            schemeShardRow,
            {.RuntimeDynamicIgnored = true, .IcbUnavailable = true});

        const auto nameRow = GetParameterRow(
            html,
            "StorageService.ConfigDispatcherSettings.AllowList.Names[0]");
        AssertParameterRow(
            nameRow,
            {
                .ParameterName = "StorageService.ConfigDispatcherSettings."
                                 "AllowList.Names[0]",
                .StaticValue = "—",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "\"LogConfigItem\"",
                .IcbValue = "",
                .EffectiveValue = "—",
            });
        AssertParameterRowStyle(
            nameRow,
            {.RuntimeDynamicIgnored = true, .IcbUnavailable = true});

        const auto enabledRow = GetParameterRow(html, "DiskAgent.Enabled");
        AssertParameterRow(
            enabledRow,
            {
                .ParameterName = "DiskAgent.Enabled",
                .StaticValue = "false",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "true",
                .IcbValue = std::nullopt,
                .EffectiveValue = "false",
            });
        AssertParameterRowStyle(enabledRow, {.RuntimeDynamicIgnored = true});
    }

    // Verify that ignored static-only diagnostics produce a red explanation and
    // per-section ignored counts, without deferred indicators.
    Y_UNIT_TEST(ShouldRenderSummaryForIgnoredStaticOnlyValues)
    {
        // Supply ignored values in two sections, including an empty message.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableDiskAgent()->SetDedicatedDiskAgent(true);
        staticConfig.MutableDiskAgent()->SetEnabled(false);
        auto dynamicConfig = MakeConfig(200);
        dynamicConfig.MutableStorageService()->SetNodeType("ignored-node");
        dynamicConfig.MutableStorageService()
            ->MutableConfigDispatcherSettings();
        dynamicConfig.MutableDiskAgent()->SetEnabled(true);
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        const auto startup =
            MakeStartupBlockstoreConfig(
                staticConfig,
                {},
                controls,
                /*extraParameters=*/{});
        TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .UpdateStatus = EConfigUpdateStatus::Runtime,
                    .DynamicConfigPresent = true,
                },
        };

        // Render a runtime source whose ignored values remain static-only.
        const auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startup.Proto,
            dynamicConfig,
            controls,
            /*extraParameters=*/{},
            renderer.Data.CurrentConfigPreparationDiagnostics);
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Count ignored parameters by section and explain the policy.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ">StorageService</a></td><td>"
            "<span class='config-dynamic-total'>3</span>"
            " / <span class='config-dynamic-ignored'>ignored</span>: 2");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ">DiskAgent</a></td><td>"
            "<span class='config-dynamic-total'>1</span>"
            " / <span class='config-dynamic-ignored'>ignored</span>: 1");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<p class='config-dynamic-ignored'>"
            "Static-only parameters detected in the dynamic config are "
            "ignored, even after a restart. Set them in the static "
            "configuration.</p>");
        UNIT_ASSERT(!html.Contains("<td class='config-dynamic-deferred'>"));
        UNIT_ASSERT(!html.Contains("take effect after restart"));
    }

    // Verify that removing the startup dynamic configuration at runtime
    // keeps the general explanation and the ignored static-only value and its
    // "static-only, ignored" label in the startup Dynamic column. The Effective
    // value must still match Static. Hide the ignored counter because it counts
    // only the current dynamic configuration, excluding values preserved in the
    // startup Dynamic column.
    Y_UNIT_TEST(
        ShouldRetainStartupLabelsForIgnoredStaticOnlyValuesAfterDynamicRemoval)
    {
        // Accept a startup value whose ignored source must remain visible.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetNodeType("static-node");
        auto startupDynamicConfig = MakeConfig(200);
        startupDynamicConfig.MutableStorageService()->SetNodeType(
            "startup-node");
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        const auto startup = MakeStartupBlockstoreConfig(
            staticConfig,
            startupDynamicConfig,
            controls,
            /*extraParameters=*/{});
        TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = startupDynamicConfig,
                    .DynamicConfigPresent = true,
                    .CurrentConfigPreparationDiagnostics =
                        startup.NormalizationDiagnostics,
                    .StartupDynamicConfigNormalizationDiagnostics =
                        startup.NormalizationDiagnostics,
                },
        };
        const TString ignoredHint =
            "<p class='config-dynamic-ignored'>"
            "Static-only parameters detected in the dynamic config are "
            "ignored, even after a restart. Set them in the static "
            "configuration.</p>";

        // Confirm the initial label, explanation and ignored count.
        auto html = renderer.RenderHtml(
            *startup.Config,
            staticConfig,
            startupDynamicConfig,
            *controls);

        const auto startupNodeRow =
            GetParameterRow(html, "StorageService.NodeType");
        AssertParameterRow(
            startupNodeRow,
            {
                .ParameterName = "StorageService.NodeType",
                .StaticValue = "\"static-node\"",
                .StartupDynamicValue = "\"startup-node\"",
                .RuntimeDynamicValue = "—",
                .IcbValue = "",
                .EffectiveValue = "\"static-node\"",
            });
        AssertParameterRowStyle(
            startupNodeRow,
            {.StartupDynamicIgnored = true, .IcbUnavailable = true});
        UNIT_ASSERT_STRING_CONTAINS(html, ignoredHint);
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ">StorageService</a></td><td>"
            "<span class='config-dynamic-total'>2</span>"
            " / <span class='config-dynamic-ignored'>ignored</span>: 1");

        // Remove the current source while retaining the saved startup source.
        const auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startup.Proto,
            {},
            controls,
            /*extraParameters=*/{},
            renderer.Data.CurrentConfigPreparationDiagnostics);
        renderer.Data.UpdateStatus = EConfigUpdateStatus::Runtime;
        renderer.Data.DynamicConfigPresent = false;
        html = renderer.RenderHtml(*config, staticConfig, {}, *controls);

        // Keep the historical labels and hint, but no current ignored counter.
        const auto retainedNodeRow =
            GetParameterRow(html, "StorageService.NodeType");
        AssertParameterRow(
            retainedNodeRow,
            {
                .ParameterName = "StorageService.NodeType",
                .StaticValue = "\"static-node\"",
                .StartupDynamicValue = "\"startup-node\"",
                .RuntimeDynamicValue = "—",
                .IcbValue = "",
                .EffectiveValue = "\"static-node\"",
            });
        AssertParameterRowStyle(
            retainedNodeRow,
            {.StartupDynamicIgnored = true, .IcbUnavailable = true});
        UNIT_ASSERT_STRING_CONTAINS(html, ignoredHint);
        UNIT_ASSERT(!html.Contains(">ignored</span>:"));
    }

    // Verify that a configuration delivery error is shown on the page while
    // the last accepted runtime Dynamic value, its "static-only, ignored"
    // label and the general static-only explanation remain visible.
    // Then remove the runtime dynamic configuration and verify that both
    // the label and explanation disappear, because the startup Dynamic
    // source has no ignored static-only values.
    Y_UNIT_TEST_F(
        ShouldRetainLabelsForIgnoredStaticOnlyValuesAfterRejectedUpdate,
        TRendererActorFixture)
    {
        // Accept a dynamic source whose static-only value is ignored.
        auto dynamicConfig = MakeConfig(300);
        dynamicConfig.MutableStorageService()->SetNodeType("ignored-node");
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(dynamicConfig),
            1);
        WaitForAck(1);
        const auto acceptedHtml = GetMonitoringPage();

        const auto acceptedHtmlNodeRow =
            GetParameterRow(acceptedHtml, "StorageService.NodeType");
        AssertParameterRow(
            acceptedHtmlNodeRow,
            {
                .ParameterName = "StorageService.NodeType",
                .StaticValue = "—",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "\"ignored-node\"",
                .IcbValue = "",
                .EffectiveValue = "—",
            });
        AssertParameterRowStyle(
            acceptedHtmlNodeRow,
            {.RuntimeDynamicIgnored = true, .IcbUnavailable = true});
        UNIT_ASSERT_STRING_CONTAINS(
            acceptedHtml,
            "<p class='config-dynamic-ignored'>");

        // Reject another delivery without replacing the accepted source report.
        SendNotification(
            std::make_shared<NCloud::NProto::TError>(
                MakeError(E_ARGUMENT, "invalid replacement")),
            2);
        const auto rejectedHtml = GetMonitoringPage();
        UNIT_ASSERT_STRING_CONTAINS(rejectedHtml, "invalid replacement");

        const auto rejectedHtmlNodeRow =
            GetParameterRow(rejectedHtml, "StorageService.NodeType");
        AssertParameterRow(
            rejectedHtmlNodeRow,
            {
                .ParameterName = "StorageService.NodeType",
                .StaticValue = "—",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "\"ignored-node\"",
                .IcbValue = "",
                .EffectiveValue = "—",
            });
        AssertParameterRowStyle(
            rejectedHtmlNodeRow,
            {.RuntimeDynamicIgnored = true, .IcbUnavailable = true});
        UNIT_ASSERT_STRING_CONTAINS(
            rejectedHtml,
            "<p class='config-dynamic-ignored'>");

        // Clear the accepted runtime source and its normalization diagnostics.
        SendNotification(nullptr, 3);
        WaitForAck(3);
        const auto clearedHtml = GetMonitoringPage();
        UNIT_ASSERT(!clearedHtml.Contains("static-only, ignored"));
        UNIT_ASSERT(
            !clearedHtml.Contains("<p class='config-dynamic-ignored'>"));
    }

    // Check that startup delivery is shown separately from absent runtime data.
    Y_UNIT_TEST_F(
        ShouldRenderStartupSourceBeforeRuntimeUpdate,
        TRendererActorFixture)
    {
        const auto html = GetMonitoringPage();
        UNIT_ASSERT(!html.Contains("<p class='config-dynamic-ignored'>"));
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<th>Dynamic<br>(startup)</th><th>Dynamic<br>(runtime)</th>");

        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.WriteBlobThreshold",
                .StaticValue = "100",
                .StartupDynamicValue = "200",
                .RuntimeDynamicValue = "—",
                .IcbValue = "—",
                .EffectiveValue = "200",
            });
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

            AssertParameterRow(
                html,
                {
                    .ParameterName = "StorageService.WriteBlobThreshold",
                    .StaticValue = "100",
                    .StartupDynamicValue = "200",
                    .RuntimeDynamicValue = ToString(value),
                    .IcbValue = "—",
                    .EffectiveValue = ToString(value),
                });
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

        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.WriteBlobThreshold",
                .StaticValue = "100",
                .StartupDynamicValue = "200",
                .RuntimeDynamicValue = "200",
                .IcbValue = "—",
                .EffectiveValue = "200",
            });
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

        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.WriteBlobThreshold",
                .StaticValue = "100",
                .StartupDynamicValue = "200",
                .RuntimeDynamicValue = "—",
                .IcbValue = "—",
                .EffectiveValue = "100",
            });
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
        NConfig::TConfigPreparationDiagnostics diagnostics;
        const auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            dynamicConfig,
            controls,
            /*extraParameters=*/{},
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
        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.SSDCompactionType",
                .StaticValue = "CT_DEFAULT (0)",
                .StartupDynamicValue = "CT_LOAD (1)",
                .RuntimeDynamicValue = "CT_DEFAULT (0)",
                .IcbValue = "CT_LOAD (1)",
                .EffectiveValue = "CT_LOAD (1)",
            });

        AssertParameterRow(
            html,
            {
                .ParameterName = "Server.ServerConfig.ChecksumFlags."
                                 "MediaKindsToValidateDataIntegrity[0]",
                .StaticValue = "STORAGE_MEDIA_SSD (1)",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "STORAGE_MEDIA_SSD (1)",
            });

        AssertParameterRow(
            html,
            {
                .ParameterName = "Server.ServerConfig.ChecksumFlags."
                                 "MediaKindsToValidateDataIntegrity[1]",
                .StaticValue = "STORAGE_MEDIA_HDD (3)",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "STORAGE_MEDIA_HDD (3)",
            });
    }

    // Check named source rows, live ICB values, secret redaction and HTML
    // escaping.
    Y_UNIT_TEST(ShouldRenderConfigSourcesWithLiveIcbOverridesAndRedactedSecrets)
    {
        // Give the sources distinct values, secrets and HTML-sensitive text.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetSchemeShardDir("<static&>");
        staticConfig.MutableStorageService()
            ->SetFreshBlobByteCountFlushThresholdHDD(4096);
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
            MakeStartupBlockstoreConfig(
                staticConfig,
                dynamicConfig,
                controls,
                /*extraParameters=*/{})
                .Config;
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

        // Check both table layouts and the presence of the dynamic source.
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

        // Bind each storage value to its parameter and source column.
        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.WriteBlobThreshold",
                .StaticValue = "100",
                .StartupDynamicValue = "200",
                .RuntimeDynamicValue = "—",
                .IcbValue = "300",
                .EffectiveValue = "300",
            });

        AssertParameterRow(
            html,
            {
                .ParameterName =
                    "StorageService.FreshBlobByteCountFlushThresholdHDD",
                .StaticValue = "4096",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = "—",
                .EffectiveValue = "4096",
            });

        // Keep the token row visible and redact every source containing it.
        AssertParameterRow(
            html,
            {
                .ParameterName = "Server.ServerConfig.NodeRegistrationToken",
                .StaticValue = "[redacted]",
                .StartupDynamicValue = "[redacted]",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "[redacted]",
            });

        // Reject raw secrets anywhere on the page and verify HTML escaping.
        UNIT_ASSERT(!html.Contains("static-secret-value"));
        UNIT_ASSERT(!html.Contains("dynamic-secret-value"));
        UNIT_ASSERT(!html.Contains("<static&>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "&lt;static&amp;&gt;");
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
        auto config =
            MakeStartupBlockstoreConfig(
                staticConfig,
                {},
                controls,
                /*extraParameters=*/{}).Config;

        // Render the initial state without a dynamic source.
        const TBlockstoreConfigRenderer renderer;
        const auto html =
            renderer.RenderHtml(*config, staticConfig, {}, *controls);

        // Match public settings to their own rows while omitting raw secrets.
        const TParameterRow publicSettings[] = {
            {
                .ParameterName = "StorageService.AuthorizationMode",
                .StaticValue = "AUTHORIZATION_REQUIRE (2)",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = "—",
                .EffectiveValue = "AUTHORIZATION_REQUIRE (2)",
            },

            {
                .ParameterName = "Logbroker.CaCertFilename",
                .StaticValue = "\"/certs/logbroker.pem\"",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "\"/certs/logbroker.pem\"",
            },

            {
                .ParameterName = "RootKms.RootCertsFile",
                .StaticValue = "\"/certs/root.pem\"",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "\"/certs/root.pem\"",
            },

            {
                .ParameterName = "RootKms.PrivateKeyFile",
                .StaticValue = "\"/keys/root.key\"",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "\"/keys/root.key\"",
            },

            {
                .ParameterName = "RootKms.KeyId",
                .StaticValue = "\"root-key-id\"",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "\"root-key-id\"",
            },

            {
                .ParameterName = "IamClient.TokenAgentUnixSocket",
                .StaticValue = "\"/run/token-agent.sock\"",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "\"/run/token-agent.sock\"",
            },
        };
        for (const auto& expected: publicSettings) {
            AssertParameterRow(html, expected);
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
                          controls,
                          /*extraParameters=*/{})
                          .Config;
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

        AssertParameterRow(
            startupHtml,
            {
                .ParameterName = "LocalNVMe.LockdownConfig",
                .StaticValue = "—",
                .StartupDynamicValue = "{}",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "{}",
            });
        UNIT_ASSERT_STRING_CONTAINS(
            startupHtml,
            ">LocalNVMe</a></td><td>"
            "<span class='config-dynamic-total'>1</span>");
        UNIT_ASSERT(!startupHtml.Contains(">RootKms</a>"));

        AssertParameterRow(
            startupHtml,
            {
                .ParameterName = "SpdkEnv",
                .StaticValue = "—",
                .StartupDynamicValue = "{}",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "{}",
            });

        // Request removal at runtime while the frozen startup value remains.
        const auto dynamicConfig = MakeConfig(300);
        config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            dynamicConfig,
            controls,
            /*extraParameters=*/{},
            renderer.Data.CurrentConfigPreparationDiagnostics);
        renderer.Data.UpdateStatus = EConfigUpdateStatus::Runtime;
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Preserve the empty effective message and its deferred section count.
        const auto lockdownRow =
            GetParameterRow(html, "LocalNVMe.LockdownConfig");
        AssertParameterRow(
            lockdownRow,
            {
                .ParameterName = "LocalNVMe.LockdownConfig",
                .StaticValue = "—",
                .StartupDynamicValue = "{}",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "{}",
            });
        AssertParameterRowStyle(lockdownRow, {.Deferred = true});
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
            /*extraParameters=*/{},
            renderer.Data.CurrentConfigPreparationDiagnostics);
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Mark only empty messages; populated values remain in child rows.
        const auto metadataRow =
            GetParameterRow(html, "Logbroker.IamMetadataServer");
        AssertParameterRow(
            metadataRow,
            {
                .ParameterName = "Logbroker.IamMetadataServer",
                .StaticValue = "—",
                .StartupDynamicValue = "{}",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "{}",
            });
        AssertParameterRowStyle(metadataRow, {.Deferred = true});

        const auto endpointRow =
            GetParameterRow(html, "Logbroker.IamMetadataServer.Endpoint");
        AssertParameterRow(
            endpointRow,
            {
                .ParameterName = "Logbroker.IamMetadataServer.Endpoint",
                .StaticValue = "—",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "\"runtime-endpoint\"",
                .IcbValue = std::nullopt,
                .EffectiveValue = "—",
            });
        AssertParameterRowStyle(endpointRow, {.Deferred = true});

        const auto addressRow = GetParameterRow(html, "Logbroker.Address");
        AssertParameterRowStyle(addressRow, {});
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
                                controls,
                                /*extraParameters=*/{})
                                .Config;
        TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = startupDynamicConfig,
                    .UpdateStatus = EConfigUpdateStatus::Runtime,
                    .DynamicConfigPresent = true,
                },
        };

        // Supply a fixed-selection diagnostic directly to the renderer.
        renderer.Data.CurrentConfigPreparationDiagnostics.IgnoredPaths.emplace(
            "Logbroker.AuthConfig",
            NConfig::EConfigIgnoreReason::OneofSwitchForbidden);
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Mark the retained and rejected branches as one deferred group change.
        const auto metadataRow =
            GetParameterRow(html, "Logbroker.IamMetadataServer");
        AssertParameterRow(
            metadataRow,
            {
                .ParameterName = "Logbroker.IamMetadataServer",
                .StaticValue = "—",
                .StartupDynamicValue = "{}",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "{}",
            });
        AssertParameterRowStyle(metadataRow, {.Deferred = true});

        const auto jwtRow = GetParameterRow(html, "Logbroker.IamJwtFile");
        AssertParameterRow(
            jwtRow,
            {
                .ParameterName = "Logbroker.IamJwtFile",
                .StaticValue = "—",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "{}",
                .IcbValue = std::nullopt,
                .EffectiveValue = "—",
            });
        AssertParameterRowStyle(jwtRow, {.Deferred = true});
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            ">Logbroker</a></td><td>"
            "<span class='config-dynamic-total'>1</span>"
            " / <span class='config-dynamic-deferred'>deferred: 1</span>");
    }

    // Count explicit parameters, deferred changes, ignored static-only values
    // and ICB overrides separately.
    Y_UNIT_TEST(ShouldRenderDynamicDeferredAndIcbCounters)
    {
        // Request a scalar and a collection change alongside a static-only
        // value that must not contribute to pending-restart diagnostics.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetListVolumesConcurrency(10);
        staticConfig.MutableStorageService()->SetMaxSSDGroupReadIops(12000);
        const auto startupConfig =
            MergeBlockstoreConfigSources(staticConfig, MakeConfig(200));
        auto dynamicConfig = MakeConfig(300);
        auto* storage = dynamicConfig.MutableStorageService();
        storage->SetListVolumesConcurrency(30);
        storage->SetMaxSSDGroupReadIops(12000);
        storage->SetNodeType("ignored-node");
        storage->AddKnownSpareNodes("node-a");
        storage->AddKnownSpareNodes("node-b");
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        NConfig::TConfigPreparationDiagnostics diagnostics;
        auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            dynamicConfig,
            controls,
            /*extraParameters=*/{},
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
                    .CurrentConfigPreparationDiagnostics =
                        std::move(diagnostics),
                },
        };
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Count the collection once and exclude ICB, unchanged RO values and
        // static-only values from the two changes that require a restart.
        const auto summaryStart = html.find(">StorageService</a>");
        UNIT_ASSERT(summaryStart != TString::npos);
        const auto summary = html.substr(
            summaryStart,
            html.find("</tr>", summaryStart) - summaryStart);
        UNIT_ASSERT_STRING_CONTAINS(
            summary,
            "class='config-dynamic-total'>5</span>");
        UNIT_ASSERT_STRING_CONTAINS(summary, "deferred: 2</span>");
        UNIT_ASSERT_STRING_CONTAINS(
            summary,
            "deferred: 2</span>"
            " / <span class='config-dynamic-ignored'>ignored</span>: 1");
        UNIT_ASSERT_STRING_CONTAINS(summary, "class='config-icb'>1</span>");

        // Mark deferred scalar and collection rows, retaining ordinary names
        // for an ICB override and an unchanged startup-only value.
        const auto listVolumesConcurrencyRow =
            GetParameterRow(html, "StorageService.ListVolumesConcurrency");
        AssertParameterRowStyle(
            listVolumesConcurrencyRow,
            {.Deferred = true, .IcbUnavailable = true});

        const auto firstSpareNodeRow =
            GetParameterRow(html, "StorageService.KnownSpareNodes[0]");
        AssertParameterRowStyle(
            firstSpareNodeRow,
            {.Deferred = true, .IcbUnavailable = true});

        const auto secondSpareNodeRow =
            GetParameterRow(html, "StorageService.KnownSpareNodes[1]");
        AssertParameterRowStyle(
            secondSpareNodeRow,
            {.Deferred = true, .IcbUnavailable = true});

        const auto writeBlobThresholdRow =
            GetParameterRow(html, "StorageService.WriteBlobThreshold");
        AssertParameterRowStyle(writeBlobThresholdRow, {});

        const auto maxSsdGroupReadIopsRow =
            GetParameterRow(html, "StorageService.MaxSSDGroupReadIops");
        AssertParameterRowStyle(
            maxSsdGroupReadIopsRow,
            {.IcbUnavailable = true});

        const auto nodeRow = GetParameterRow(html, "StorageService.NodeType");
        AssertParameterRow(
            nodeRow,
            {
                .ParameterName = "StorageService.NodeType",
                .StaticValue = "—",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "\"ignored-node\"",
                .IcbValue = "",
                .EffectiveValue = "—",
            });
        AssertParameterRowStyle(
            nodeRow,
            {.RuntimeDynamicIgnored = true, .IcbUnavailable = true});
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
            MakeStartupBlockstoreConfig(
                staticConfig,
                dynamicConfig,
                controls,
                /*extraParameters=*/{})
                .Config;

        // Apply collection-level diagnostics to every displayed child row.
        NConfig::TConfigPreparationDiagnostics diagnostics;
        for (const auto path:
             {
                 "StorageService.LinkedDiskFillBandwidth[]",
                 "Diagnostics.RequestThresholds[]",
             })
        {
            diagnostics.IgnoredPaths.emplace(
                path,
                NConfig::EConfigIgnoreReason::RuntimeUpdateForbidden);
        }

        // Render every element, including a zero map value and the seventeenth
        // item.
        const TBlockstoreConfigRenderer renderer{
            .Data =
                {
                    .StartupDynamicConfig = std::move(startupDynamicConfig),
                    .UpdateStatus = EConfigUpdateStatus::Runtime,
                    .DynamicConfigPresent = true,
                    .CurrentConfigPreparationDiagnostics =
                        std::move(diagnostics),
                },
        };
        const auto html = renderer.RenderHtml(
            *config,
            staticConfig,
            dynamicConfig,
            *controls);

        // Compare matching indices and keys while retaining missing source
        // cells.
        const auto firstBandwidthRow = GetParameterRow(
            html,
            "StorageService.LinkedDiskFillBandwidth[0].ReadBandwidth");
        AssertParameterRow(
            firstBandwidthRow,
            {
                .ParameterName =
                    "StorageService.LinkedDiskFillBandwidth[0].ReadBandwidth",
                .StaticValue = "10",
                .StartupDynamicValue = "15",
                .RuntimeDynamicValue = "30",
                .IcbValue = "",
                .EffectiveValue = "10",
            });

        AssertParameterRowStyle(
            firstBandwidthRow,
            {.Deferred = true, .IcbUnavailable = true});

        const auto secondBandwidthRow = GetParameterRow(
            html,
            "StorageService.LinkedDiskFillBandwidth[1].ReadBandwidth");
        AssertParameterRow(
            secondBandwidthRow,
            {
                .ParameterName =
                    "StorageService.LinkedDiskFillBandwidth[1].ReadBandwidth",
                .StaticValue = "20",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = "",
                .EffectiveValue = "20",
            });

        AssertParameterRowStyle(
            secondBandwidthRow,
            {.Deferred = true, .IcbUnavailable = true});

        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.KnownSpareNodes[16]",
                .StaticValue = "—",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "\"node-16\"",
                .IcbValue = "",
                .EffectiveValue = "\"node-16\"",
            });

        const auto sharedThresholdRow = GetParameterRow(
            html,
            "Diagnostics.RequestThresholds[0].ByRequestType[\"shared\"]");
        AssertParameterRow(
            sharedThresholdRow,
            {
                .ParameterName = "Diagnostics.RequestThresholds[0]."
                                 "ByRequestType[\"shared\"]",
                .StaticValue = "100",
                .StartupDynamicValue = "150",
                .RuntimeDynamicValue = "300",
                .IcbValue = std::nullopt,
                .EffectiveValue = "100",
            });

        AssertParameterRow(
            html,
            {
                .ParameterName = "Diagnostics.RequestThresholds[0]."
                                 "ByRequestType[\"static-only\"]",
                .StaticValue = "0",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "0",
            });

        AssertParameterRow(
            html,
            {
                .ParameterName = "Diagnostics.RequestThresholds[0]."
                                 "ByRequestType[\"dynamic-only\"]",
                .StaticValue = "—",
                .StartupDynamicValue = "—",
                .RuntimeDynamicValue = "200",
                .IcbValue = std::nullopt,
                .EffectiveValue = "—",
            });

        AssertParameterRow(
            html,
            {
                .ParameterName = "Diagnostics.RequestThresholds[0]."
                                 "ByRequestType[\"startup-only\"]",
                .StaticValue = "—",
                .StartupDynamicValue = "400",
                .RuntimeDynamicValue = "—",
                .IcbValue = std::nullopt,
                .EffectiveValue = "—",
            });

        // Highlight the map value from the rejected collection.
        AssertParameterRowStyle(sharedThresholdRow, {.Deferred = true});
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

            AssertParameterRow(
                html,
                {
                    .ParameterName = "StorageService.WriteBlobThreshold",
                    .StaticValue = "100",
                    .StartupDynamicValue = "200",
                    .RuntimeDynamicValue = "—",
                    .IcbValue = "—",
                    .EffectiveValue = "200",
                });
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

        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.WriteBlobThreshold",
                .StaticValue = "100",
                .StartupDynamicValue = "200",
                .RuntimeDynamicValue = "—",
                .IcbValue = "—",
                .EffectiveValue = "100",
            });
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

        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.WriteBlobThreshold",
                .StaticValue = "100",
                .StartupDynamicValue = "200",
                .RuntimeDynamicValue = "—",
                .IcbValue = "—",
                .EffectiveValue = "200",
            });
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

        AssertParameterRow(
            html,
            {
                .ParameterName = "StorageService.WriteBlobThreshold",
                .StaticValue = "100",
                .StartupDynamicValue = "200",
                .RuntimeDynamicValue = "300",
                .IcbValue = "—",
                .EffectiveValue = "300",
            });
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
