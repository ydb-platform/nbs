#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/discovery/config.h>
#include <cloud/blockstore/libs/rdma/iface/config.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/sharding/config.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
#include <cloud/storage/core/libs/grpc/utils.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NServer {

using namespace NCloud::NBlockStore::NDiscovery;

////////////////////////////////////////////////////////////////////////////////

TConfigInitializerCommon::TConfigInitializerCommon(TOptionsCommonPtr options)
    : Options(std::move(options))
{}

TConfigInitializerCommon::~TConfigInitializerCommon()
{}

void TConfigInitializerCommon::InitDiscoveryConfig()
{
    NProto::TDiscoveryServiceConfig discoveryConfig;
    if (Options->DiscoveryConfig) {
        ParseProtoTextFromFileRobust(Options->DiscoveryConfig, discoveryConfig);
    }

    SetupDiscoveryPorts(discoveryConfig);

    DiscoveryConfig = std::make_shared<TDiscoveryConfig>(discoveryConfig);
}

void TConfigInitializerCommon::InitDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig diagnosticsConfig;
    if (Options->DiagnosticsConfig) {
        ParseProtoTextFromFileRobust(
            Options->DiagnosticsConfig,
            diagnosticsConfig);
    }

    if (Options->MonitoringPort) {
        diagnosticsConfig.SetNbsMonPort(Options->MonitoringPort);
    }

    DiagnosticsConfig = std::make_shared<TDiagnosticsConfig>(diagnosticsConfig);
}

void TConfigInitializerCommon::InitDiskAgentConfig()
{
    NProto::TDiskAgentConfig diskAgentConfig;
    if (Options->DiskAgentConfig) {
        ParseProtoTextFromFileRobust(Options->DiskAgentConfig, diskAgentConfig);
    }

    if (Options->TemporaryServer || diskAgentConfig.GetDedicatedDiskAgent()) {
        diskAgentConfig.SetEnabled(false);
    }

    if (diskAgentConfig.GetAgentId().empty()) {
        diskAgentConfig.SetAgentId(FQDNHostName());
    }

    DiskAgentConfig = std::make_shared<NStorage::TDiskAgentConfig>(
        std::move(diskAgentConfig),
        Rack,
        HostPerformanceProfile.NetworkMbitThroughput);
}

void TConfigInitializerCommon::InitRdmaConfig()
{
    NProto::TRdmaConfig rdmaConfig;

    if (Options->RdmaConfig) {
        ParseProtoTextFromFileRobust(Options->RdmaConfig, rdmaConfig);
    } else {
        // no rdma config file is given fallback to legacy config
        if (DiskAgentConfig->DeprecatedHasRdmaTarget()) {
            rdmaConfig.SetServerEnabled(true);
            const auto& rdmaTarget = DiskAgentConfig->DeprecatedGetRdmaTarget();
            rdmaConfig.MutableServer()->CopyFrom(rdmaTarget.GetServer());
        }

        if (ServerConfig->DeprecatedGetRdmaClientEnabled()) {
            rdmaConfig.SetClientEnabled(true);
            rdmaConfig.MutableClient()->CopyFrom(
                ServerConfig->DeprecatedGetRdmaClientConfig());
        }
    }

    RdmaConfig =
        std::make_shared<NRdma::TRdmaConfig>(rdmaConfig);
}

void TConfigInitializerCommon::InitShardingConfig()
{
    NProto::TShardingConfig shardingConfig;

    if (Options->ShardingConfig) {
        ParseProtoTextFromFileRobust(Options->ShardingConfig, shardingConfig);
    }

    ShardingConfig =
        std::make_shared<NSharding::TShardingConfig>(std::move(shardingConfig));
}

void TConfigInitializerCommon::InitDiskRegistryProxyConfig()
{
    NProto::TDiskRegistryProxyConfig config;
    if (Options->DiskRegistryProxyConfig) {
        ParseProtoTextFromFileRobust(Options->DiskRegistryProxyConfig, config);
    }

    DiskRegistryProxyConfig = std::make_shared<NStorage::TDiskRegistryProxyConfig>(
        std::move(config));
}

void TConfigInitializerCommon::InitServerConfig()
{
    NProto::TServerAppConfig appConfig;
    if (Options->ServerConfig) {
        ParseProtoTextFromFileRobust(Options->ServerConfig, appConfig);
    }

    auto& serverConfig = *appConfig.MutableServerConfig();
    SetupServerPorts(serverConfig);

    ServerConfig = std::make_shared<TServerAppConfig>(appConfig);
    SetupGrpcThreadsLimit();
}

void TConfigInitializerCommon::InitEndpointConfig()
{
    NProto::TClientAppConfig appConfig;
    if (Options->EndpointConfig) {
        ParseProtoTextFromFileRobust(Options->EndpointConfig, appConfig);
    }

    EndpointConfig = std::make_shared<NClient::TClientAppConfig>(appConfig);
}

std::optional<NJson::TJsonValue> TConfigInitializerCommon::ReadJsonFile(
    const TString& filename)
{
    if (filename.empty()) {
        return {};
    }

    try {
        TFileInput in(filename);
        return NJson::ReadJsonTree(&in, true);
    } catch (...) {
        STORAGE_ERROR("Failed to read file: " << filename.Quote()
            << " with error: " << CurrentExceptionMessage().c_str());
        return {};
    }
}

void TConfigInitializerCommon::InitHostPerformanceProfile()
{
    const auto& tc = EndpointConfig->GetClientConfig().GetThrottlingConfig();
    ui32 networkThroughput = tc.GetDefaultNetworkMbitThroughput();
    ui32 hostCpuCount = tc.GetDefaultHostCpuCount();

    if (auto json = ReadJsonFile(tc.GetInfraThrottlingConfigPath())) {
        try {
            if (auto* value = json->GetValueByPath("interfaces.[0].eth0.speed")) {
                networkThroughput = FromString<ui64>(value->GetStringSafe());
            }
        } catch (...) {
            STORAGE_ERROR("Failed to read NetworkMbitThroughput. Error: "
                << CurrentExceptionMessage().c_str());
        }

        try {
            if (auto* value = json->GetValueByPath("compute_cores_num")) {
                hostCpuCount = value->GetUIntegerSafe();
            }
        } catch (...) {
            STORAGE_ERROR("Failed to read HostCpuCount. Error: "
                << CurrentExceptionMessage().c_str());
        }
    }

    HostPerformanceProfile = {
        .CpuCount = hostCpuCount,
        .NetworkMbitThroughput = networkThroughput,
    };
}

void TConfigInitializerCommon::InitSpdkEnvConfig()
{
    NProto::TSpdkEnvConfig config;
    SpdkEnvConfig = std::make_shared<NSpdk::TSpdkEnvConfig>(config);
}

void TConfigInitializerCommon::SetupDiscoveryPorts(
    NProto::TDiscoveryServiceConfig& discoveryConfig) const
{
    Y_ABORT_UNLESS(ServerConfig);

    if (!discoveryConfig.GetConductorInstancePort()) {
        discoveryConfig.SetConductorInstancePort(ServerConfig->GetPort());
    }

    if (!discoveryConfig.GetConductorSecureInstancePort()) {
        discoveryConfig.SetConductorSecureInstancePort(ServerConfig->GetSecurePort());
    }
}

void TConfigInitializerCommon::SetupServerPorts(
    NProto::TServerConfig& config) const
{
    if (Options->ServerPort) {
        config.SetPort(Options->ServerPort);
    }
    if (Options->DataServerPort) {
        config.SetDataPort(Options->DataServerPort);
    }
    if (Options->SecureServerPort) {
        config.SetSecurePort(Options->SecureServerPort);
    }
    if (Options->UnixSocketPath) {
        config.SetUnixSocketPath(Options->UnixSocketPath);
    } else if (Options->TemporaryServer) {
        config.SetUnixSocketPath({});
    }
}

void TConfigInitializerCommon::SetupGrpcThreadsLimit() const
{
    ui32 maxThreads = ServerConfig->GetGrpcThreadsLimit();
    SetExecutorThreadsLimit(maxThreads);
    SetDefaultThreadPoolLimit(maxThreads);
}

}   // namespace NCloud::NBlockStore::NServer
