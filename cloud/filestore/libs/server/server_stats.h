#pragma once

#include "public.h"

#include "server_memory_state.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <memory>

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TServerStats final
{
private:
    NMonitoring::TDynamicCountersPtr Counters;
    NMonitoring::TDynamicCounters::TCounterPtr MmapRegionCount;
    NMonitoring::TDynamicCounters::TCounterPtr TotalMmapSize;

public:
    explicit TServerStats(NMonitoring::TDynamicCountersPtr counters);

    void Update(const TServerStateStats& info);
};

using TServerStatsPtr = std::shared_ptr<TServerStats>;

}   // namespace NCloud::NFileStore::NServer
