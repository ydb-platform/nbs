/*******************************************************************************

The read-only Blockstore configuration interface. Readers retain one
configuration for a logical operation. Source processing and configuration
factories are declared in blockstore_config_management.h.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/config/grpc_client.pb.h>
#include <cloud/blockstore/config/root_kms.pb.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/discovery/config.h>
#include <cloud/blockstore/libs/local_nvme/config.h>
#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/notify/iface/config.h>
#include <cloud/blockstore/libs/rdma/config.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/blockstore/libs/ydbstats/config.h>

#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/iam/iface/config.h>

#include <util/generic/ptr.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using TGrpcClientConfigConstPtr =
    std::shared_ptr<const NProto::TGrpcClientConfig>;
using TRootKmsConfigConstPtr = std::shared_ptr<const NProto::TRootKmsConfig>;

////////////////////////////////////////////////////////////////////////////////

// An independently owned, read-only set of top-level Blockstore configuration
// sections. MakeBlockstoreConfig() creates implementations that own all
// sections, see blockstore_config_management.h.
// Getters return const owning pointers, so callers may retain the
// whole configuration or an individual section. The Storage wrapper shares
// live ICB controls: its effective values may change while its protobuf-backed
// state remains immutable.
class IBlockstoreConfig: public TAtomicRefCount<IBlockstoreConfig>
{
public:
    IBlockstoreConfig() = default;

    IBlockstoreConfig(const IBlockstoreConfig&) = delete;
    IBlockstoreConfig& operator=(const IBlockstoreConfig&) = delete;
    IBlockstoreConfig(IBlockstoreConfig&&) = delete;
    IBlockstoreConfig& operator=(IBlockstoreConfig&&) = delete;

    virtual ~IBlockstoreConfig() = default;

    [[nodiscard]] virtual const NServer::TServerAppConfigConstPtr&
    GetServerConfig() const = 0;

    // Return the top-level Features section for configuration inspection and
    // serialization. Storage decisions must use the typed feature accessors
    // provided by TStorageConfig instead of reading this section directly.
    [[nodiscard]] virtual const NFeatures::TFeaturesConfigConstPtr&
    GetFeaturesConfig() const = 0;

    [[nodiscard]] virtual const NStorage::TStorageConfigConstPtr&
    GetStorageConfig() const = 0;
    [[nodiscard]] virtual const TDiagnosticsConfigConstPtr&
    GetDiagnosticsConfig() const = 0;
    [[nodiscard]] virtual const NDiscovery::TDiscoveryConfigConstPtr&
    GetDiscoveryServiceConfig() const = 0;
    [[nodiscard]] virtual const NClient::TClientAppConfigConstPtr&
    GetEndpointConfig() const = 0;
    [[nodiscard]] virtual const NStorage::TDiskAgentConfigConstPtr&
    GetDiskAgentConfig() const = 0;
    [[nodiscard]] virtual const NStorage::TDiskRegistryProxyConfigConstPtr&
    GetDiskRegistryProxyConfig() const = 0;
    [[nodiscard]] virtual const NSpdk::TSpdkEnvConfigConstPtr&
    GetSpdkEnvConfig() const = 0;
    [[nodiscard]] virtual const NRdma::TRdmaConfigConstPtr&
    GetRdmaConfig() const = 0;
    [[nodiscard]] virtual const NYdbStats::TYdbStatsConfigConstPtr&
    GetYdbStatsConfig() const = 0;
    [[nodiscard]] virtual const NLogbroker::TLogbrokerConfigConstPtr&
    GetLogbrokerConfig() const = 0;
    [[nodiscard]] virtual const NNotify::TNotifyConfigConstPtr&
    GetNotifyConfig() const = 0;
    [[nodiscard]] virtual const NIamClient::TIamClientConfigConstPtr&
    GetIamClientConfig() const = 0;
    [[nodiscard]] virtual const TGrpcClientConfigConstPtr&
    GetKmsClientConfig() const = 0;
    [[nodiscard]] virtual const TGrpcClientConfigConstPtr&
    GetComputeClientConfig() const = 0;
    [[nodiscard]] virtual const TRootKmsConfigConstPtr&
    GetRootKmsConfig() const = 0;
    [[nodiscard]] virtual const NCells::TCellsConfigConstPtr&
    GetCellsConfig() const = 0;
    [[nodiscard]] virtual const TLocalNVMeConfigConstPtr&
    GetLocalNVMeConfig() const = 0;
};

using IBlockstoreConfigPtr = TIntrusivePtr<IBlockstoreConfig>;
using IBlockstoreConfigConstPtr = TIntrusiveConstPtr<IBlockstoreConfig>;

}   // namespace NCloud::NBlockStore
