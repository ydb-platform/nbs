#include "hostname.h"

#include "config.h"

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 GetServicePort(EHostService serviceType, const TDiagnosticsConfig& config)
{
    switch (serviceType) {
        case EHostService::Kikimr: return config.GetKikimrMonPort();
        case EHostService::Nbs: return config.GetNbsMonPort();

        default:
            Y_ABORT("Wrong EHostService: %d", serviceType);
    }
}

}    // namespace

////////////////////////////////////////////////////////////////////////////////

TString GetShortHostName(const TString& fullName)
{
    auto pos = fullName.find('.', 0);
    return pos != NPOS ? fullName.substr(0, pos) : fullName;
}

TString GetShortHostName()
{
    return GetShortHostName(HostName());
}

TString GetExternalHostUrl(
    const TString& hostName,
    EHostService serviceType,
    const TDiagnosticsConfig& config)
{
    TStringBuilder out;
    switch (config.GetHostNameScheme()) {
        case NProto::HOSTNAME_BASTION:
            out << "https://"
                << GetShortHostName(hostName)
                << '.'
                << config.GetBastionNameSuffix();

            if (serviceType != EHostService::Kikimr) {
                out << ':' << GetServicePort(serviceType, config);
            }
            break;

        case NProto::HOSTNAME_YDBVIEWER:
            out << "https://" << config.GetViewerHostName()
                << '/' << hostName
                << ':' << GetServicePort(serviceType, config);
            break;

        default:
            out << "http://" << hostName
                << ':' << GetServicePort(serviceType, config);
            break;
    }

    out << '/';
    return out;
}

TString GetMonitoringVolumeUrl(
    const TDiagnosticsConfig& config,
    const TString& diskId)
{
    TMonitoringUrlData data = config.GetMonitoringUrlData();
    return TStringBuilder()
           << data.MonitoringUrl << "/projects/" << data.MonitoringProject
           << "/dashboards/" << data.MonitoringVolumeDashboard
           << "?from=now-1d&to=now&refresh=60000&p.cluster="
           << data.MonitoringClusterName << "&p.volume=" << diskId;
}

TString GetMonitoringPartitionUrl(const TDiagnosticsConfig& config)
{
    TMonitoringUrlData data = config.GetMonitoringUrlData();
    return TStringBuilder()
           << data.MonitoringUrl << "/projects/" << data.MonitoringProject
           << "/dashboards/" << data.MonitoringPartitionDashboard
           << "?from=now-1d&to=now&"
              "refresh=60000&p.service=tablets&p.cluster="
           << data.MonitoringClusterName << "&p.host=" << GetShortHostName();
}

TString GetMonitoringNBSAlertsUrl(const TDiagnosticsConfig& config)
{
    TMonitoringUrlData data = config.GetMonitoringUrlData();
    return TStringBuilder()
           << data.MonitoringUrl << "/projects/" << data.MonitoringProject
           << "/dashboards/" << data.MonitoringNBSAlertsDashboard
           << "?from=now-1d&to=now&refresh=60000&p.cluster="
           << data.MonitoringClusterName << "&p.host=" << GetShortHostName();
}

TString GetMonitoringNBSOverviewToTVUrl(const TDiagnosticsConfig& config)
{
    TMonitoringUrlData data = config.GetMonitoringUrlData();
    return TStringBuilder()
           << data.MonitoringUrl << "/projects/" << data.MonitoringProject
           << "/dashboards/" << data.MonitoringNBSTVDashboard
           << "?from=now-1d&to=now&refresh=60000&p.cluster="
           << data.MonitoringClusterName << "&p.host=" << GetShortHostName();
}

TString GetMonitoringYDBGroupUrl(
    const TDiagnosticsConfig& config,
    ui32 groupId,
    const TString& storagePool)
{
    constexpr TStringBuf GetFast =
        R"(q.0.s=histogram_percentile(100, {project="kikimr", cluster="*", storagePool="%s", group="%)" PRIu32
        R"(", host="*", service="vdisks", subsystem="latency_histo", handleclass="GetFast"})&q.0.name=A)";
    constexpr TStringBuf PutUserData =
        R"(q.1.s=histogram_percentile(100, {project="kikimr", cluster="*", storagePool="%s", group="%)" PRIu32
        R"(", host="*", service="vdisks", subsystem="latency_histo", handleclass="PutUserData"})&q.1.name=B)";
    constexpr TStringBuf Url =
        "%s/projects/%s/explorer/"
        "queries?%s&%s&from=now-1d&to=now&refresh=60000";
    return Sprintf(
        Url.data(),
        config.GetMonitoringUrlData().MonitoringUrl.c_str(),
        config.GetMonitoringUrlData().MonitoringYDBProject.c_str(),
        Sprintf(GetFast.data(), storagePool.c_str(), groupId).c_str(),
        Sprintf(PutUserData.data(), storagePool.c_str(), groupId).c_str());
}

TString GetMonitoringDashboardYDBGroupUrl(
    const TDiagnosticsConfig& config,
    ui32 groupId)
{
    const auto& monitoringYDBProject =
        config.GetMonitoringUrlData().MonitoringYDBProject;
    if (monitoringYDBProject.empty()) {
        return "";
    }

    constexpr TStringBuf Url =
        "%s/projects/%s/dashboards/"
        "%s?from=now-1d&to=now&refresh=60000&p.cluster=*&p.group=%" PRIu32;

    return Sprintf(
        Url.data(),
        config.GetMonitoringUrlData().MonitoringUrl.c_str(),
        monitoringYDBProject.c_str(),
        config.GetMonitoringUrlData().MonitoringYDBGroupDashboard.c_str(),
        groupId);
}

}   // namespace NCloud::NBlockStore
