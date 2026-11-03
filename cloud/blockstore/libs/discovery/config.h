#pragma once

#include "public.h"

#include <cloud/blockstore/config/discovery.pb.h>

#include <util/datetime/base.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

class TEdsConfig
{
private:
    const NProto::TEdsDiscoveryConfig Config;

public:
    explicit TEdsConfig(NProto::TEdsDiscoveryConfig config = {});

    TString GetEndpoint() const;
    TVector<TString> GetHostPatterns() const;
    TDuration GetRequestTimeout() const;
    TDuration GetRequestInterval() const;
    ui32 GetInstancePort() const;
    ui32 GetSecureInstancePort() const;
    TString GetClientId() const;
};

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryConfig
{
private:
    const NProto::TDiscoveryServiceConfig Config;
    const TEdsConfig EdsConfig;

public:
    TDiscoveryConfig(NProto::TDiscoveryServiceConfig discoveryServiceConfig = {});

    const NProto::TDiscoveryServiceConfig* GetConfig() const;

    TString GetConductorApiUrl() const;
    TString GetInstanceListFile() const;
    TString GetBannedInstanceListFile() const;
    TDuration GetConductorRequestInterval() const;
    TDuration GetLocalFilesReloadInterval() const;
    TDuration GetHealthCheckInterval() const;
    TVector<TString> GetConductorGroups() const;
    ui32 GetConductorInstancePort() const;
    ui32 GetConductorSecureInstancePort() const;
    TDuration GetConductorRequestTimeout() const;
    TDuration GetPingRequestTimeout() const;
    ui32 GetMaxPingRequestsPerHealthCheck() const;

    TString GetEdsEndpoint() const;
    TVector<TString> GetEdsHostPatterns() const;
    TDuration GetEdsRequestTimeout() const;
    TDuration GetEdsRequestInterval() const;
    ui32 GetEdsInstancePort() const;
    ui32 GetEdsSecureInstancePort() const;
    TString GetEdsClientId() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore::NDiscovery
