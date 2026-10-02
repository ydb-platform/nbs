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

// Configuration pointer aliases provide read-only access by default:
// ...ConfigPtr points to a const configuration. Consumers must always accept
// and retain ...ConfigPtr. Only configuration builders may retain
// ...ConfigMutablePtr to prepare and publish configurations. Define a mutable
// alias only when needed.

using TGrpcClientConfigPtr = std::shared_ptr<const NProto::TGrpcClientConfig>;
using TRootKmsConfigPtr = std::shared_ptr<const NProto::TRootKmsConfig>;

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

    [[nodiscard]] virtual const NServer::TServerAppConfigPtr&
    GetServerConfig() const = 0;

    // Return the top-level Features section for configuration inspection and
    // serialization. Storage decisions must use the typed feature accessors
    // provided by TStorageConfig instead of reading this section directly.
    [[nodiscard]] virtual const NFeatures::TFeaturesConfigPtr&
    GetFeaturesConfig() const = 0;

    [[nodiscard]] virtual const NStorage::TStorageConfigPtr&
    GetStorageConfig() const = 0;
    [[nodiscard]] virtual const TDiagnosticsConfigPtr&
    GetDiagnosticsConfig() const = 0;
    [[nodiscard]] virtual const NDiscovery::TDiscoveryConfigPtr&
    GetDiscoveryServiceConfig() const = 0;
    [[nodiscard]] virtual const NClient::TClientAppConfigPtr&
    GetEndpointConfig() const = 0;
    [[nodiscard]] virtual const NStorage::TDiskAgentConfigPtr&
    GetDiskAgentConfig() const = 0;
    [[nodiscard]] virtual const NStorage::TDiskRegistryProxyConfigPtr&
    GetDiskRegistryProxyConfig() const = 0;
    [[nodiscard]] virtual const NSpdk::TSpdkEnvConfigPtr&
    GetSpdkEnvConfig() const = 0;
    [[nodiscard]] virtual const NRdma::TRdmaConfigPtr&
    GetRdmaConfig() const = 0;
    [[nodiscard]] virtual const NYdbStats::TYdbStatsConfigPtr&
    GetYdbStatsConfig() const = 0;
    [[nodiscard]] virtual const NLogbroker::TLogbrokerConfigPtr&
    GetLogbrokerConfig() const = 0;
    [[nodiscard]] virtual const NNotify::TNotifyConfigPtr&
    GetNotifyConfig() const = 0;
    [[nodiscard]] virtual const NIamClient::TIamClientConfigPtr&
    GetIamClientConfig() const = 0;
    [[nodiscard]] virtual const TGrpcClientConfigPtr&
    GetKmsClientConfig() const = 0;
    [[nodiscard]] virtual const TGrpcClientConfigPtr&
    GetComputeClientConfig() const = 0;
    [[nodiscard]] virtual const TRootKmsConfigPtr&
    GetRootKmsConfig() const = 0;
    [[nodiscard]] virtual const NCells::TCellsConfigPtr&
    GetCellsConfig() const = 0;
    [[nodiscard]] virtual const TLocalNVMeConfigPtr&
    GetLocalNVMeConfig() const = 0;
};

using IBlockstoreConfigPtr = TIntrusiveConstPtr<IBlockstoreConfig>;
using IBlockstoreConfigMutablePtr = TIntrusivePtr<IBlockstoreConfig>;

}   // namespace NCloud::NBlockStore
