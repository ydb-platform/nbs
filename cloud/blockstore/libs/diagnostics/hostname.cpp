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
            Y_FAIL("Wrong EHostService: %d", serviceType);
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

TString GetSolomonVolumeUrl(
    const TDiagnosticsConfig& config,
    const TString& diskId,
    const TString& dashboard)
{
    return TStringBuilder()
        << config.GetSolomonUrl()
        << "/?project=" << config.GetSolomonProject()
        << "&service=service_volume"
        << "&cluster="<< config.GetSolomonClusterName()
        << "&volume=" << diskId
        << "&dashboard=" << dashboard;
}

TString GetSolomonPartitionUrl(
    const TDiagnosticsConfig& config,
    const TString& dashboard)
{
    return TStringBuilder()
        << config.GetSolomonUrl()
        << "/?project=" << config.GetSolomonProject()
        << "&service=tablets"
        << "&cluster=" << config.GetSolomonClusterName()
        << "&host=" << GetShortHostName()
        << "&dashboard=" << dashboard;
}

TString GetSolomonServerUrl(
    const TDiagnosticsConfig& config,
    const TString& dashboard)
{
    return TStringBuilder()
        << config.GetSolomonUrl()
        << "/?project" << config.GetSolomonProject()
        << "&service=server"
        << "&cluster=" << config.GetSolomonClusterName()
        << "&host=" << GetShortHostName()
        << "&type=-"
        << "&dashboard="<< dashboard;
}

TString GetSolomonClientUrl(
    const TDiagnosticsConfig& config,
    const TString& dashboard)
{
    return TStringBuilder()
        << config.GetSolomonUrl()
        << "/?project=" << config.GetSolomonProject()
        << "&service=client"
        << "&cluster="<< config.GetSolomonClusterName()
        << "&host=" << GetShortHostName()
        << "&type=-"
        << "&dashboard=" << dashboard;
}

TString GetSolomonBsProxyUrl(
    const TDiagnosticsConfig& config,
    ui32 groupId,
    const TString& dashboard)
{
    return TStringBuilder()
        << config.GetSolomonUrl()
        << "/?project" << config.GetSolomonProject()
        << "&service=dsproxy_percentile"
        << "&cluster=" << config.GetSolomonClusterName()
        << "&host=" << GetShortHostName()
        << "&blobstorageproxy=" << groupId
        << "&dashboard=" << dashboard;
}

}   // namespace NCloud::NBlockStore
