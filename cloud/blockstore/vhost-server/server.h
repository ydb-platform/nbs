#pragma once

#include "public.h"

#include "options.h"
#include "stats.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <memory>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct IServer
{
    virtual ~IServer() = default;

    virtual void Start(const TOptions& options) = 0;
    virtual void Stop() = 0;
    virtual TCompleteStats GetStats(const TSimpleStats& prevStats) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IServer> CreateServer(
    ILoggingServicePtr logging,
    IBackendPtr backend);

}   // namespace NCloud::NBlockStore::NVHostServer
