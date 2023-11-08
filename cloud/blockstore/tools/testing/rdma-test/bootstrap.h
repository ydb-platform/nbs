#pragma once

#include "private.h"

#include <cloud/blockstore/libs/rdma/impl/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    TOptionsPtr Options;

    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ITaskQueuePtr ThreadPool;

    ITraceProcessorPtr TraceProcessor;

    NRdma::NVerbs::IVerbsPtr Verbs;
    NRdma::IClientPtr Client;
    NRdma::IServerPtr Server;

    IStoragePtr Storage;

public:
    TBootstrap(TOptionsPtr options);

    void Init();
    void Start();
    void Stop();

    IRunnablePtr CreateTest();

private:
    void InitLogging();
    void InitTracing();
};

}   // namespace NCloud::NBlockStore
