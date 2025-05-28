#include "hostname.h"

#include "config.h"

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/hostname.h>
#include <array>
#include <span>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr std::array<const char*, 2> DefaultHandleClasses(
    {"GetFast", "PutUserData"});
constexpr std::array<const char*, 3> LogHandleClasses(
    {"GetFast", "PutUserData", "PutTabletLog"});
constexpr std::array<const char*, 4> MergedAndMixedHandleClasses(
    {"GetFast", "PutUserData", "GetAsync", "PutAsyncBlob"});

ui32 GetServicePort(EHostService serviceType, const TDiagnosticsConfig& config)
{
    switch (serviceType) {
        case EHostService::Kikimr: return config.GetKikimrMonPort();
        case EHostService::Nbs: return config.GetNbsMonPort();

        default:
            Y_ABORT("Wrong EHostService: %d", serviceType);
    }
}

[[nodiscard]] std::span<const char* const> GetHandleClasses(
    const TString& dataKind)
{
    if (dataKind == "Log") {
        return LogHandleClasses;
    }

    if (dataKind == "Merged" || dataKind == "Mixed") {
        return MergedAndMixedHandleClasses;
    }

    return DefaultHandleClasses;
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

TString
GetQueries(ui32 groupId, const TString& storagePool, const TString& dataKind)
{
    constexpr TStringBuf QueryPattern =
        R"(q.%u.s=histogram_percentile(100, {project="kikimr", cluster="*", storagePool="%s", group="%)" PRIu32
        R"(", host="*", service="vdisks", subsystem="latency_histo", handleclass="%s"})&q.%u.name=%s)";

    auto handleClasses = GetHandleClasses(dataKind);

    TStringBuilder queries;

    char queryName = 'A';
    for (ui32 queryIdx = 0; queryIdx < handleClasses.size(); ++queryIdx) {
        queries << Sprintf(
                       QueryPattern.data(),
                       queryIdx,
                       storagePool.c_str(),
                       groupId,
                       handleClasses[queryIdx],
                       queryIdx,
                       TString(1, queryName).data())
                       .c_str()
                << "&";
        ++queryName;
    }

    return queries.c_str();
}

TString GetMonitoringYDBGroupUrl(
    const TDiagnosticsConfig& config,
    ui32 groupId,
    const TString& storagePool,
    const TString& dataKind)
{
    constexpr TStringBuf Url =
        "%s/projects/%s/explorer/"
        "queries?%sfrom=now-1d&to=now&refresh=60000";

    auto queries = GetQueries(groupId, storagePool, dataKind);

    return Sprintf(
        Url.data(),
        config.GetMonitoringUrlData().MonitoringUrl.c_str(),
        config.GetMonitoringUrlData().MonitoringYDBProject.c_str(),
        queries.c_str());
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
