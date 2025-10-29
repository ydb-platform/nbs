#pragma once

#include "private.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/public.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/features/features_config.h>

#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializer
{
    const TOptionsPtr Options;

    NKikimrConfig::TAppConfigPtr KikimrConfig;
    TServerAppConfigPtr ServerConfig;
    NStorage::TStorageConfigPtr StorageConfig;
    NStorage::TDiskAgentConfigPtr DiskAgentConfig;
    NStorage::TDiskRegistryProxyConfigPtr DiskRegistryProxyConfig;
    TDiagnosticsConfigPtr DiagnosticsConfig;
    NSpdk::TSpdkEnvConfigPtr SpdkEnvConfig;
    NFeatures::TFeaturesConfigPtr FeaturesConfig;
    NRdma::TRdmaConfigPtr RdmaConfig;

    TString Rack;
    TLog Log;

    TConfigInitializer(TOptionsPtr options)
        : Options(std::move(options))
    {}

    void ApplyCMSConfigs(NKikimrConfig::TAppConfig cmsConfig);
    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config);

    void InitKikimrConfig();
    void InitDiagnosticsConfig();
    void InitStorageConfig();
    void InitDiskAgentConfig();
    void InitDiskRegistryProxyConfig();
    void InitServerConfig();
    void InitSpdkEnvConfig();
    void InitFeaturesConfig();
    void InitRdmaConfig();

    NKikimrConfig::TLogConfig GetLogConfig() const;
    NKikimrConfig::TMonitoringConfig GetMonitoringConfig() const;

private:
    TString GetFullSchemeShardDir() const;

    void SetupMonitoringConfig(NKikimrConfig::TMonitoringConfig& monConfig) const;
    void SetupLogConfig(NKikimrConfig::TLogConfig& logConfig) const;
    void SetupDiskAgentConfig(NProto::TDiskAgentConfig& config) const;

    void ApplyLogConfig(const TString& text);
    void ApplyAuthConfig(const TString& text);
    void ApplyFeaturesConfig(const TString& text);
    void ApplyServerAppConfig(const TString& text);
    void ApplyMonitoringConfig(const TString& text);
    void ApplyActorSystemConfig(const TString& text);
    void ApplyDiagnosticsConfig(const TString& text);
    void ApplyInterconnectConfig(const TString& text);
    void ApplyStorageServiceConfig(const TString& text);
    void ApplyDiskAgentConfig(const TString& text);
    void ApplyDiskRegistryProxyConfig(const TString& text);

    void ApplySpdkEnvConfig(const NProto::TSpdkEnvConfig& config);
};

}   // namespace NCloud::NBlockStore::NServer
