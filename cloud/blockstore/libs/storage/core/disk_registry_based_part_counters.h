#pragma once

#include "disk_counters.h"

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TPartNonreplCountersData
{
    ui64 NetworkBytes;
    TDuration CpuUsage;
    TPartitionDiskCountersPtr DiskCounters;

    TPartNonreplCountersData(
            ui64 networkBytes,
            TDuration cpuUsage,
            TPartitionDiskCountersPtr diskCounters)
        : NetworkBytes(networkBytes)
        , CpuUsage(cpuUsage)
        , DiskCounters(std::move(diskCounters))
    {}
};

}   // namespace NCloud::NBlockStore::NStorage
