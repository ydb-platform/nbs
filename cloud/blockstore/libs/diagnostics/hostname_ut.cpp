#include "hostname.h"

#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/list.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMonitoringUrlTest)
{
    Y_UNIT_TEST(ShouldReturnCorrectMonitoringVolumeUrl)
    {
        NProto::TDiagnosticsConfig protoConfig;
        const TMap<TString, TString> configMap = {
            {"MonitoringClusterName", "company_devlab"},
            {"MonitoringUrl", "https://monitoring.company.com"},
            {"MonitoringProject", "development.cloud"},
            {"MonitoringVolumeDashboard", "monitoring_dashboard"},
            {"MonitoringUrlTemplate",
             "{MonitoringUrl}/projects/{MonitoringProject}/dashboards/"
             "{MonitoringVolumeDashboard}?from=now-1d&to=now&refresh=60000&p."
             "cluster={MonitoringClusterName}&p.volume={diskId}"}};

        TString protoText;
        for (const auto& [key, value]: configMap) {
            protoText += key + ": \"" + value + "\"\n";
        }

        NCloud::ParseProtoTextFromStringRobust(protoText, protoConfig);

        TDiagnosticsConfig config(protoConfig);

        auto url = GetMonitoringVolumeUrl(config, "vol-1234567890abcdef");

        UNIT_ASSERT_EQUAL(
            url,
            "https://monitoring.company.com/projects/development.cloud/"
            "dashboards/"
            "monitoring_dashboard?from=now-1d&to=now&refresh=60000&p.cluster="
            "company_devlab&p.volume=vol-1234567890abcdef");
    }
}

}   // namespace

}   // namespace NCloud::NBlockStore
