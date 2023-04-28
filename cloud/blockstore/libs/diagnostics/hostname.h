#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class EHostService
{
    Kikimr,
    Nbs
};

////////////////////////////////////////////////////////////////////////////////

TString GetShortHostName(const TString& fullName);
TString GetShortHostName();

TString GetExternalHostUrl(
    const TString& hostName,
    EHostService serviceType,
    const TDiagnosticsConfig& config);

TString GetSolomonServerUrl(
    const TDiagnosticsConfig& config,
    const TString& dashboard);

TString GetSolomonClientUrl(
    const TDiagnosticsConfig& config,
    const TString& dashboard);

TString GetSolomonVolumeUrl(
    const TDiagnosticsConfig& config,
    const TString& diskId,
    const TString& dashboard);

TString GetSolomonPartitionUrl(
    const TDiagnosticsConfig& config,
    const TString& dashboard);

TString GetSolomonBsProxyUrl(
    const TDiagnosticsConfig& config,
    ui32 groupId,
    const TString& dashboard);

}   // namespace NCloud::NBlockStore
