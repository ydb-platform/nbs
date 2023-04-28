#pragma once

#include "private.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/rdma/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service_local/public.h>
#include <cloud/blockstore/libs/spdk/public.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>

#include <ydb/core/driver_lib/run/factories.h>

#include <library/cpp/actors/util/should_continue.h>
#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializer;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    std::shared_ptr<NKikimr::TModuleFactories> ModuleFactories;

    std::unique_ptr<TConfigInitializer> Configs;

    ILoggingServicePtr BootstrapLogging;
    TLog Log;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    IActorSystemPtr ActorSystem;
    ILoggingServicePtr Logging;
    IAsyncLoggerPtr AsyncLogger;
    IMonitoringServicePtr Monitoring;
    TVector<ITraceReaderPtr> TraceReaders;
    ITraceProcessorPtr TraceProcessor;
    IProfileLogPtr ProfileLog;
    IBlockDigestGeneratorPtr BlockDigestGenerator;
    IFileIOServicePtr FileIOService;
    NSpdk::ISpdkEnvPtr Spdk;
    ICachingAllocatorPtr Allocator;
    IStorageProviderPtr AioStorageProvider;
    NNvme::INvmeManagerPtr NvmeManager;
    NRdma::IServerPtr RdmaServer;

    TProgramShouldContinue ShouldContinue;
    TVector<TString> PostponedCriticalEvents;

public:
    TBootstrap(std::shared_ptr<NKikimr::TModuleFactories> moduleFactories);
    ~TBootstrap();

    void ParseOptions(int argc, char** argv);
    void Init();

    void Start();
    void Stop();

    TProgramShouldContinue& GetShouldContinue();

private:
    void InitLWTrace();
    void InitProfileLog();
    void InitKikimrService();

    void InitRdmaServer(NStorage::TDiskAgentConfig& config);
};

}   // namespace NCloud::NBlockStore::NServer
