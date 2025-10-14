#pragma once

#include "public.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache.h>

namespace NCloud::NFileStore {

struct IWriteBackCacheStats
    : public NFuse::IWriteBackCacheStats
{
    virtual void UpdateStats(bool updatePercentiles) = 0;
};

IWriteBackCacheStatsPtr CreateWriteBackCacheStats(
    NMonitoring::TDynamicCounters& counters,
    ITimerPtr timer);

}   // namespace NCloud::NFileStore
