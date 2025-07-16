#pragma once

#include <cloud/blockstore/libs/storage/core/disk_counters.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TUpdateCounters
{
    NActors::TActorId Sender;
    ui64 NetworkBytes;
    TDuration CpuUsage;
    TPartitionDiskCountersPtr DiskCounters;

    TUpdateCounters(
            const NActors::TActorId& sender,
            ui64 networkBytes,
            TDuration cpuUsage,
            TPartitionDiskCountersPtr diskCounters)
        : Sender(sender)
        , NetworkBytes(networkBytes)
        , CpuUsage(cpuUsage)
        , DiskCounters(std::move(diskCounters))
    {}
};

}   // namespace NCloud::NBlockStore::NStorage
