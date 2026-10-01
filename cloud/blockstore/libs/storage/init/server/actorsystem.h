#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/config/blockstore_config.h>
#include <cloud/blockstore/libs/config/blockstore_config_holder.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/local_nvme/public.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>
#include <cloud/blockstore/libs/notify/iface/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/rdma/config.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/public.h>
#include <cloud/blockstore/libs/ydbstats/public.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/kikimr/public.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

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

    // Local configuration with CLI overrides, captured before CMS. RDMA is
    // initialized from its own file or legacy fields.
    NProto::TBlockstoreConfig StaticBlockstoreConfigProto;

    // Normalized PrivateDatabaseConfig received from CMS in YAML mode.
    // Empty in PROTO mode, for temporary servers, or without accepted
    // PrivateDatabaseConfig overrides.
    NProto::TBlockstoreConfig CmsBlockstoreConfig;

    // Complete startup proto after CMS and PrivateDatabaseConfig YAML
    // application. Retained unchanged as the source of startup-only runtime
    // parameters.
    NProto::TBlockstoreConfig StartupBlockstoreConfigProto;

    // Effective startup snapshot in both YAML and PROTO modes.
    // In YAML mode, includes accepted PrivateDatabaseConfig overrides.
    IBlockstoreConfigPtr StartupBlockstoreConfig;

    // Configuration publication point initialized by bootstrap; non-null and
    // shared with its read-only provider and the process-wide getter.
    TBlockstoreConfigHolderPtr ConfigHolder;

    ILoggingServicePtr Logging;
    IAsyncLoggerPtr AsyncLogger;
    IStatsAggregatorPtr StatsAggregator;
    NYdbStats::IYdbVolumesStatsUploaderPtr StatsUploader;
    NDiscovery::IDiscoveryServicePtr DiscoveryService;
    NSpdk::ISpdkEnvPtr Spdk;
    ICachingAllocatorPtr Allocator;
    IStorageProviderPtr LocalStorageProvider;
    IProfileLogPtr ProfileLog;
    IBlockDigestGeneratorPtr BlockDigestGenerator;
    IBlockDigestGeneratorFactoryPtr BlockDigestGeneratorFactory;
    ITraceSerializerPtr TraceSerializer;
    NLogbroker::IServicePtr LogbrokerService;
    NNotify::IServicePtr NotifyService;
    IVolumeStatsPtr VolumeStats;
    NCloud::NStorage::NRdma::IServerPtr RdmaServer;
    NCloud::NStorage::NRdma::IClientPtr RdmaClient;
    NCloud::NStorage::IStatsFetcherPtr StatsFetcher;
    TManuallyPreemptedVolumesPtr PreemptedVolumes;
    NNvme::INvmeManagerPtr NvmeManager;
    IVolumeBalancerSwitchPtr VolumeBalancerSwitch;
    NServer::IEndpointEventHandlerPtr EndpointEventHandler;
    IRootKmsKeyProviderPtr RootKmsKeyProvider;
    TPartitionBudgetManagerPtr PartitionBudgetManager;

    TVector<NCloud::NStorage::IUserMetricsSupplierPtr> UserCounterProviders;

    ITaskQueuePtr BackgroundThreadPool;

    ILocalNVMeServicePtr LocalNVMeService;

    bool IsDiskRegistrySpareNode = false;
    bool TemporaryServer = false;

    bool IsHiveLocalServiceEnabled = false;
};

////////////////////////////////////////////////////////////////////////////////

IActorSystemPtr CreateActorSystem(const TServerActorSystemArgs& args);

}   // namespace NCloud::NBlockStore::NStorage
