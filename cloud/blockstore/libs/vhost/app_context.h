#pragma once

#include "public.h"

#include "server.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <atomic>

namespace NCloud::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

// Server-wide state shared with the objects the server creates. TServer derives
// from it and passes itself by reference to its endpoints.
struct TAppContext
{
    IServerStatsPtr ServerStats;
    TLog Log;

    std::atomic_flag ShouldStop = false;

    virtual ~TAppContext() = default;
};

}   // namespace NCloud::NBlockStore::NVhost
