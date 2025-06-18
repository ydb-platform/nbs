#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/endpoint_proxy/client/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/endpoints_grpc/public.h>
#include <cloud/blockstore/libs/nbd/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service_local/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/vhost/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/coroutine/public.h>

#include <contrib/ydb/library/actors/util/should_continue.h>
#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrapBase
{
private:
    TConfigInitializerCommonPtr Configs;

protected:
    IDeviceHandlerFactoryPtr DeviceHandlerFactory;
    ILoggingServicePtr BootstrapLogging;
    TLog Log;

    ILoggingServicePtr Logging;
    TLog GrpcLog;
    TLog SpdkLog;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ITaskQueuePtr BackgroundThreadPool;
    ISchedulerPtr BackgroundScheduler;
    IMonitoringServicePtr Monitoring;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    IServerStatsPtr ServerStats;
    IStatsUpdaterPtr ServerStatsUpdater;
    TVector<ITraceReaderPtr> TraceReaders;
    ITraceProcessorPtr TraceProcessor;
    NDiscovery::IDiscoveryServicePtr DiscoveryService;
    IProfileLogPtr ProfileLog;
    IBlockDigestGeneratorPtr BlockDigestGenerator;
    IBlockStorePtr Service;
    ISocketEndpointListenerPtr GrpcEndpointListener;
    NVhost::IServerPtr VhostServer;
    NVhost::TVhostCallbacks VhostCallbacks;
    NBD::IServerPtr NbdServer;
    IFileIOServiceProviderPtr FileIOServiceProvider;
    IStorageProviderPtr StorageProvider;
    IKmsKeyProviderPtr KmsKeyProvider;
    IRootKmsKeyProviderPtr RootKmsKeyProvider;
    TExecutorPtr Executor;
    IServerPtr Server;
    NSpdk::ISpdkEnvPtr Spdk;
    ICachingAllocatorPtr Allocator;
    IStorageProviderPtr AioStorageProvider;
    NClient::IEndpointProxyClientPtr EndpointProxyClient;
    IEndpointManagerPtr EndpointManager;
    IEndpointEventProxyPtr EndpointEventHandler;
    NRdma::IServerPtr RdmaServer;
    NRdma::IClientPtr RdmaClient;
    ITaskQueuePtr RdmaThreadPool;
    NNvme::INvmeManagerPtr NvmeManager;
    IVolumeBalancerSwitchPtr VolumeBalancerSwitch;
    NBD::IErrorHandlerMapPtr NbdErrorHandlerMap;

    TProgramShouldContinue ShouldContinue;
    TVector<TString> PostponedCriticalEvents;

public:
    TBootstrapBase(IDeviceHandlerFactoryPtr deviceHandlerFactory);
    virtual ~TBootstrapBase();

    void ParseOptions(int argc, char** argv);
    void Init();

    void Start();
    void Stop();

    virtual TProgramShouldContinue& GetShouldContinue() = 0;

    IBlockStorePtr GetBlockStoreService();

protected:
    virtual TConfigInitializerCommonPtr InitConfigs(int argc, char** argv) = 0;

    virtual IStartable* GetActorSystem() = 0;
    virtual IStartable* GetAsyncLogger() = 0;
    virtual IStartable* GetStatsAggregator() = 0;
    virtual IStartable* GetClientPercentiles() = 0;
    virtual IStartable* GetStatsUploader() = 0;
    virtual IStartable* GetYdbStorage() = 0;
    virtual IStartable* GetTraceSerializer() = 0;
    virtual IStartable* GetLogbrokerService() = 0;
    virtual IStartable* GetNotifyService() = 0;
    virtual IStartable* GetStatsFetcher() = 0;
    virtual IStartable* GetIamTokenClient() = 0;
    virtual IStartable* GetComputeClient() = 0;
    virtual IStartable* GetKmsClient() = 0;
    virtual IStartable* GetRootKmsClient() = 0;

    virtual void InitSpdk() = 0;
    virtual void InitRdmaClient() = 0;
    virtual void InitRdmaServer() = 0;
    virtual void InitKikimrService() = 0;
    virtual void InitAuthService() = 0;

    virtual void WarmupBSGroupConnections() = 0;

    void InitLWTrace();
    void InitProfileLog();
    void InitLogs();

private:
    void InitLocalService();
    void InitNullService();

    void InitDbgConfigs();
};

}   // namespace NCloud::NBlockStore::NServer
