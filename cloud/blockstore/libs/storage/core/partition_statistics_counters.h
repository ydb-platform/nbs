#pragma once

#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>

#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

struct TPartitionStatisticsCounters
{
    ui64 DiffSysCpuConsumption;
    ui64 UserCpuConsumption;
    TPartitionDiskCountersPtr PartCounters;
    NBlobMetrics::TBlobLoadMetrics OffsetLoadMetrics;
    NKikimrTabletBase::TMetrics Metrics;

    TPartitionStatisticsCounters(
            ui64 diffSysCpuConsumption,
            ui64 userCpuConsumption,
            TPartitionDiskCountersPtr partCounters,
            NBlobMetrics::TBlobLoadMetrics offsetLoadMetrics,
            NKikimrTabletBase::TMetrics metrics)
        : DiffSysCpuConsumption(diffSysCpuConsumption)
        , UserCpuConsumption(userCpuConsumption)
        , PartCounters(std::move(partCounters))
        , OffsetLoadMetrics(std::move(offsetLoadMetrics))
        , Metrics(std::move(metrics))
    {}
};

}   // namespace NCloud::NBlockStore::NStorage
