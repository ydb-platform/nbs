#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>
#include <cloud/blockstore/libs/notify/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/public.h>
#include <cloud/blockstore/libs/ydbstats/public.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/kikimr/public.h>

#include <contrib/ydb/core/driver_lib/run/factories.h>

#include <contrib/ydb/library/actors/core/defs.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TServerActorSystemArgs
{
    std::shared_ptr<NKikimr::TModuleFactories> ModuleFactories;

    ui32 NodeId;
    NActors::TScopeId ScopeId;
    NKikimrConfig::TAppConfigPtr AppConfig;

    TDiagnosticsConfigPtr DiagnosticsConfig;
    TStorageConfigPtr StorageConfig;
    TDiskAgentConfigPtr DiskAgentConfig;
    NRdma::TRdmaConfigPtr RdmaConfig;
    TDiskRegistryProxyConfigPtr DiskRegistryProxyConfig;

    ILoggingServicePtr Logging;
    IAsyncLoggerPtr AsyncLogger;
    IStatsAggregatorPtr StatsAggregator;
    NYdbStats::IYdbVolumesStatsUploaderPtr StatsUploader;
    NDiscovery::IDiscoveryServicePtr DiscoveryService;
    NSpdk::ISpdkEnvPtr Spdk;
    ICachingAllocatorPtr Allocator;
    IStorageProviderPtr AioStorageProvider;
    IProfileLogPtr ProfileLog;
    IBlockDigestGeneratorPtr BlockDigestGenerator;
    ITraceSerializerPtr TraceSerializer;
    NLogbroker::IServicePtr LogbrokerService;
    NNotify::IServicePtr NotifyService;
    IVolumeStatsPtr VolumeStats;
    NRdma::IServerPtr RdmaServer;
    NRdma::IClientPtr RdmaClient;
    NCloud::NStorage::IStatsFetcherPtr StatsFetcher;
    TManuallyPreemptedVolumesPtr PreemptedVolumes;
    NNvme::INvmeManagerPtr NvmeManager;
    IVolumeBalancerSwitchPtr VolumeBalancerSwitch;
    NServer::IEndpointEventHandlerPtr EndpointEventHandler;
    IRootKmsKeyProviderPtr RootKmsKeyProvider;

    TVector<NCloud::NStorage::IUserMetricsSupplierPtr> UserCounterProviders;

    bool IsDiskRegistrySpareNode = false;
    bool TemporaryServer = false;
};

////////////////////////////////////////////////////////////////////////////////

IActorSystemPtr CreateActorSystem(const TServerActorSystemArgs& args);

}   // namespace NCloud::NBlockStore::NStorage
