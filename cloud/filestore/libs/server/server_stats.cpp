#include "server_stats.h"

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

TServerStats::TServerStats(NMonitoring::TDynamicCountersPtr counters)
    : Counters(std::move(counters))
    , MmapRegionCount(Counters->GetCounter("MmapRegionCount", false))
    , TotalMmapSize(Counters->GetCounter("TotalMmapSizeBytes", false))
{}

void TServerStats::Update(const TServerStateStats& info)
{
    MmapRegionCount->Set(info.MmapRegionCount);
    TotalMmapSize->Set(info.TotalMmapSize);
}

}   // namespace NCloud::NFileStore::NServer
