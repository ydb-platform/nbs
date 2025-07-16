#pragma once

#include "disk_counters.h"

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDiskRegistryBasedPartCounters
{
    ui64 NetworkBytes;
    TDuration CpuUsage;
    TPartitionDiskCountersPtr Stats;

    TDiskRegistryBasedPartCounters(
            ui64 networkBytes,
            TDuration cpuUsage,
            TPartitionDiskCountersPtr stats)
        : NetworkBytes(networkBytes)
        , CpuUsage(cpuUsage)
        , Stats(std::move(stats))
    {}
};

}   // namespace NCloud::NBlockStore::NStorage
