#pragma once

#include "private.h"

#include <cloud/filestore/tools/testing/loadtest/lib/public.h>

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/logger/log.h>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    TOptionsPtr Options;

    ILoggingServicePtr Logging;
    TLog Log;
    TLog GrpcLog;

    IMonitoringServicePtr Monitoring;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

    NClient::TClientConfigPtr ClientConfig;
    IClientFactoryPtr ClientFactory;

public:
    TBootstrap(TOptionsPtr options);
    ~TBootstrap();

    void Init();
    void Start();
    void Stop();

    TOptionsPtr GetOptions()
    {
        return Options;
    }

    ITimerPtr GetTimer()
    {
        return Timer;
    }

    ISchedulerPtr GetScheduler()
    {
        return Scheduler;
    }

    ILoggingServicePtr GetLogging()
    {
        return Logging;
    }

    IClientFactoryPtr GetClientFactory()
    {
        return ClientFactory;
    }

private:
    void InitDbgConfig();
    void InitClientConfig();
};

}   // namespace NCloud::NFileStore::NLoadTest
