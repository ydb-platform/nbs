#pragma once

#include "public.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/discovery/config.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/public.h>

#include <cloud/storage/core/libs/daemon/config_initializer.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerCommon
    : public virtual NCloud::TConfigInitializerBase
{
    const TOptionsCommonPtr Options;

    NDiscovery::TDiscoveryConfigPtr DiscoveryConfig;
    TServerAppConfigPtr ServerConfig;
    NClient::TClientAppConfigPtr EndpointConfig;
    NStorage::TDiskAgentConfigPtr DiskAgentConfig;
    NStorage::TDiskRegistryProxyConfigPtr DiskRegistryProxyConfig;
    TDiagnosticsConfigPtr DiagnosticsConfig;
    NSpdk::TSpdkEnvConfigPtr SpdkEnvConfig;
    NClient::THostPerformanceProfile HostPerformanceProfile;
    NRdma::TRdmaConfigPtr RdmaConfig;

    TString Rack;
    TLog Log;

    TConfigInitializerCommon(TOptionsCommonPtr options);
    virtual ~TConfigInitializerCommon();

    void InitDiagnosticsConfig();
    void InitDiscoveryConfig();
    void InitDiskAgentConfig();
    void InitDiskRegistryProxyConfig();
    void InitEndpointConfig();
    void InitHostPerformanceProfile();
    void InitServerConfig();
    void InitSpdkEnvConfig();
    void InitRdmaConfig();

    virtual bool GetUseNonreplicatedRdmaActor() const = 0;
    virtual TDuration GetInactiveClientsTimeout() const = 0;

protected:
    std::optional<NJson::TJsonValue> ReadJsonFile(const TString& filename);

    void SetupDiscoveryPorts(NProto::TDiscoveryServiceConfig& discoveryConfig) const;
    void SetupServerPorts(NProto::TServerConfig& config) const;
    void SetupGrpcThreadsLimit() const;
};

}   // namespace NCloud::NBlockStore::NServer
