#pragma once

#include "private.h"

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/logger/log.h>

#include <util/generic/vector.h>

namespace NCloud::NFastShard::NJdServer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    const TOptionsPtr Options;

    ILoggingServicePtr Logging;
    TLog Log;

    IFileIOServicePtr FileIO;
    TExecutorPtr Executor;
    // The journalled device server over the file device.
    IStartablePtr Server;

    // The components that Start has started, in the start order. Stop
    // stops only them.
    TVector<IStartablePtr> Started;

public:
    explicit TBootstrap(TOptionsPtr options);
    ~TBootstrap();

    void Init();
    void Start();
    void Stop();

    TOptionsPtr GetOptions()
    {
        return Options;
    }

    ILoggingServicePtr GetLogging()
    {
        return Logging;
    }

private:
    void StartComponent(IStartablePtr component);
};

}   // namespace NCloud::NFastShard::NJdServer
