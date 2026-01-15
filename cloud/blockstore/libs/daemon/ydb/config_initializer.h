#pragma once

#include "public.h"

#include <cloud/blockstore/config/grpc_client.pb.h>
#include <cloud/blockstore/config/root_kms.pb.h>

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/daemon/common/config_initializer.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/discovery/config.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>
#include <cloud/blockstore/libs/notify/iface/public.h>
#include <cloud/blockstore/libs/root_kms/iface/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/public.h>
#include <cloud/blockstore/libs/ydbstats/config.h>

#include <cloud/storage/core/config/opentelemetry_client.pb.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/iam/iface/public.h>
#include <cloud/storage/core/libs/kikimr/config_initializer.h>

#include <contrib/ydb/core/protos/config.pb.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/logger/log.h>

namespace google::protobuf {

////////////////////////////////////////////////////////////////////////////////

class Message;

}   // namespace google::protobuf

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerYdb final
    : public TConfigInitializerCommon
    , public NCloud::NStorage::TConfigInitializerYdbBase
{
    TOptionsYdbPtr Options;

    NYdbStats::TYdbStatsConfigPtr StatsConfig;
    NStorage::TStorageConfigPtr StorageConfig;
    NFeatures::TFeaturesConfigPtr FeaturesConfig;
    NLogbroker::TLogbrokerConfigPtr LogbrokerConfig;
    NNotify::TNotifyConfigPtr NotifyConfig;
    NIamClient::TIamClientConfigPtr IamClientConfig;
    NProto::TGrpcClientConfig KmsClientConfig;
    NProto::TGrpcClientConfig ComputeClientConfig;
    NProto::TRootKmsConfig RootKmsConfig;

    TConfigInitializerYdb(TOptionsYdbPtr options);

    void InitFeaturesConfig();
    void InitLogbrokerConfig();
    void InitNotifyConfig();
    void InitStatsUploadConfig();
    void InitStorageConfig();
    void InitIamClientConfig();
    void InitKmsClientConfig();
    void InitRootKmsConfig();
    void InitComputeClientConfig();
    void InitTraceServiceClientConfig();

    bool GetUseNonreplicatedRdmaActor() const override;
    TDuration GetInactiveClientsTimeout() const override;

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) override;

private:
    void SetupStorageConfig(NProto::TStorageServiceConfig& config) const;

    void ApplyDiagnosticsConfig(const TString& text);
    void ApplyDiscoveryServiceConfig(const TString& text);
    void ApplyDiskAgentConfig(const TString& text);
    void ApplyDiskRegistryProxyConfig(const TString& text);
    void ApplyFeaturesConfig(const TString& text);
    void ApplyLogbrokerConfig(const TString& text);
    void ApplyNotifyConfig(const TString& text);
    void ApplyServerAppConfig(const TString& text);
    void ApplySpdkEnvConfig(const TString& text);
    void ApplyStorageServiceConfig(const TString& text);
    void ApplyYdbStatsConfig(const TString& text);
    void ApplyIamClientConfig(const TString& text);
    void ApplyKmsClientConfig(const TString& text);
    void ApplyRootKmsConfig(const TString& text);
    void ApplyComputeClientConfig(const TString& text);

    void ApplyNamedConfigs(const NKikimrConfig::TAppConfig& config);
    void ApplyBlockstoreConfig(const NKikimrConfig::TAppConfig& config);
};

}   // namespace NCloud::NBlockStore::NServer
