#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TDiagnosticsConfig CreateConfig(const TString& protoText)
{
    NProto::TDiagnosticsConfig config;
    NCloud::ParseProtoTextFromStringRobust(protoText, config);
    return config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiagnosticsConfigTest)
{
    Y_UNIT_TEST(ShouldDumpDefaultLatencySliSettings)
    {
        const TDiagnosticsConfig config;
        UNIT_ASSERT(!config.GetEnableLatencySli());
        UNIT_ASSERT(config.GetLatencySliThresholds().empty());
        TStringStream text;
        TStringStream html;
        config.Dump(text);
        config.DumpHtml(html);
        UNIT_ASSERT_STRING_CONTAINS(
            text.Str(), TStringBuilder() << "EnableLatencySli: " << false);
        UNIT_ASSERT_STRING_CONTAINS(text.Str(), "LatencySliThresholds: \n");
        UNIT_ASSERT_STRING_CONTAINS(html.Str(), "EnableLatencySli</td><td>");
        UNIT_ASSERT_STRING_CONTAINS(html.Str(), "LatencySliThresholds</td><td></td>");
    }

    Y_UNIT_TEST(ShouldDumpConfiguredLatencySliSettings)
    {
        const TDiagnosticsConfig config(CreateConfig(R"(
EnableLatencySli: true
LatencySliThresholds {
    MediaKind: STORAGE_MEDIA_SSD_LOCAL
    Write: false
    StartBytes: 1
    EndBytes: 8193
    ThresholdUs: 1000
}
LatencySliThresholds {
    MediaKind: STORAGE_MEDIA_HDD
    Write: true
    StartBytes: 8193
    EndBytes: 1048577
    ThresholdUs: 25000
}
)"));
        UNIT_ASSERT(config.GetEnableLatencySli());
        const auto thresholds = config.GetLatencySliThresholds();
        UNIT_ASSERT_VALUES_EQUAL(2, thresholds.size());
        TStringStream text;
        TStringStream html;
        config.Dump(text);
        config.DumpHtml(html);
        UNIT_ASSERT_STRING_CONTAINS(
            text.Str(), TStringBuilder() << "EnableLatencySli: " << true);
        UNIT_ASSERT_STRING_CONTAINS(
            html.Str(), TStringBuilder() << "EnableLatencySli</td><td>" << true);
        UNIT_ASSERT_STRING_CONTAINS(text.Str(), "LatencySliThresholds: ");
        UNIT_ASSERT_STRING_CONTAINS(html.Str(), "LatencySliThresholds</td><td>");
        for (const auto& row: thresholds) {
            UNIT_ASSERT_STRING_CONTAINS(text.Str(), row.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(html.Str(), row.ShortDebugString());
        }
    }

    Y_UNIT_TEST(ShouldConfigureBlobCompressionChunkSizes)
    {
        TDiagnosticsConfig defaults;
        UNIT_ASSERT(defaults.GetBlobCompressionChunkSizes().empty());

        auto proto =
            CreateConfig("BlobCompressionChunkSizes: [0, 1, 16, 128]");
        const TDiagnosticsConfig config(proto);
        proto.AddBlobCompressionChunkSizes(256);
        // The diagnostics config owns an immutable snapshot of the settings.
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{0, 1, 16, 128}),
            config.GetBlobCompressionChunkSizes());
    }

    Y_UNIT_TEST(TestBooleanFieldExplicitTrue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
ReportHistogramAsSingleCounter: true
ReportHistogramAsMultipleCounters: true
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "ReportHistogramAsSingleCounter"));
        UNIT_ASSERT(HasField(protoConfig, "ReportHistogramAsMultipleCounters"));

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsSingleCounter(),
            true);
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsMultipleCounters(),
            true);
    }

    Y_UNIT_TEST(TestBooleanFieldExplicitFalse)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
ReportHistogramAsSingleCounter: false
ReportHistogramAsMultipleCounters: false
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "ReportHistogramAsSingleCounter"));
        UNIT_ASSERT(HasField(protoConfig, "ReportHistogramAsMultipleCounters"));

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsSingleCounter(),
            false);
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsMultipleCounters(),
            false);
    }

    Y_UNIT_TEST(TestBooleanFieldDelaultValues)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig("");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(!HasField(protoConfig, "ReportHistogramAsSingleCounter"));
        UNIT_ASSERT(!HasField(protoConfig, "ReportHistogramAsMultipleCounters"));

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsSingleCounter(),
            false);
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsMultipleCounters(),
            true);
    }

    Y_UNIT_TEST(TestStringFieldExplicitValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
CpuWaitFilename: "override.wait"
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "CpuWaitFilename"));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCpuWaitFilename(), "override.wait");
    }

    Y_UNIT_TEST(TestStringFieldEmptyValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
CpuWaitFilename: ""
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "CpuWaitFilename"));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCpuWaitFilename(), "");
    }

    Y_UNIT_TEST(TestStringFieldDefaultValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig("");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(!HasField(protoConfig, "CpuWaitFilename"));
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetCpuWaitFilename(),
            "/sys/fs/cgroup/cpu/system.slice/nbs.service/cpuacct.wait");
    }

    Y_UNIT_TEST(TestRepeatedFieldExplicitValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
CloudIdsWithStrictSLA: "cloud1"
CloudIdsWithStrictSLA: "cloud2"
CloudIdsWithStrictSLA: "cloud3"
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "CloudIdsWithStrictSLA"));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCloudIdsWithStrictSLA().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(config.GetCloudIdsWithStrictSLA()[0], "cloud1");
        UNIT_ASSERT_VALUES_EQUAL(config.GetCloudIdsWithStrictSLA()[1], "cloud2");
        UNIT_ASSERT_VALUES_EQUAL(config.GetCloudIdsWithStrictSLA()[2], "cloud3");
    }

    Y_UNIT_TEST(TestRepeatedFieldDefaultValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig("");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(!HasField(protoConfig, "CloudIdsWithStrictSLA"));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCloudIdsWithStrictSLA().size(), 0);
    }

    Y_UNIT_TEST(TestVolumeCriticalEventsReportingMode)
    {
        TDiagnosticsConfig defaultConfig;
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EVolumeCriticalEventsReportingMode::APP_ONLY,
            defaultConfig.GetVolumeCriticalEventsReportingMode());

        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
VolumeCriticalEventsReportingMode: ALL
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EVolumeCriticalEventsReportingMode::ALL,
            config.GetVolumeCriticalEventsReportingMode());
    }
}

}   // namespace NCloud::NBlockStore
