#pragma once

#include "public.h"

#include <cloud/blockstore/config/discovery.pb.h>

#include <util/datetime/base.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryConfig
{
private:
    const NProto::TDiscoveryServiceConfig Config;

public:
    TDiscoveryConfig(
        NProto::TDiscoveryServiceConfig discoveryServiceConfig = {});

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

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore::NDiscovery
