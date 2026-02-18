#pragma once

#include <contrib/ydb/core/tablet/tablet_metrics.h>

#include <util/datetime/base.h>
#include <util/thread/lfstack.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

struct TUpdateWriteThroughput
{
    TInstant Now;
    NKikimr::NMetrics::TChannel Channel;
    NKikimr::NMetrics::TGroupId Group;
    ui64 Value;

    TUpdateWriteThroughput(
        const TInstant& now,
        const NKikimr::NMetrics::TChannel& channel,
        const NKikimr::NMetrics::TGroupId& group,
        ui64 value)
        : Now(now)
        , Channel(channel)
        , Group(group)
        , Value(value)
    {}
};

struct TUpdateReadThroughput
{
    TInstant Now;
    NKikimr::NMetrics::TChannel Channel;
    NKikimr::NMetrics::TGroupId Group;
    ui64 Value;
    bool IsOverlayDisk;

    TUpdateReadThroughput(
        const TInstant& now,
        const NKikimr::NMetrics::TChannel& channel,
        const NKikimr::NMetrics::TGroupId& group,
        ui64 value,
        bool isOverlayDisk)
        : Now(now)
        , Channel(channel)
        , Group(group)
        , Value(value)
        , IsOverlayDisk(isOverlayDisk)
    {}
};

struct TUpdateNetworkStat
{
    TInstant Now;
    ui64 Value;

    TUpdateNetworkStat(const TInstant& now, ui64 value)
        : Now(now)
        , Value(value)
    {}
};

struct TUpdateStorageStat
{
    i64 Value;

    explicit TUpdateStorageStat(i64 value)
        : Value(value)
    {}
};

struct TUpdateCPUUsageStat
{
    TInstant Now;
    ui64 Value;

    TUpdateCPUUsageStat(const TInstant& now, ui64 value)
        : Now(now)
        , Value(value)
    {}
};

using TResourceMetricsUpdate = std::variant<
    TUpdateWriteThroughput,
    TUpdateReadThroughput,
    TUpdateNetworkStat,
    TUpdateStorageStat,
    TUpdateCPUUsageStat>;

// Thread safe
class TResourceMetricsQueue
{
private:
    TLockFreeStack<TResourceMetricsUpdate> ResourceMetricsUpdates;

public:
    void Push(TResourceMetricsUpdate update)
    {
        ResourceMetricsUpdates.Enqueue(update);
    }

    TVector<TResourceMetricsUpdate> PopAll()
    {
        TVector<TResourceMetricsUpdate> resCollection;
        ResourceMetricsUpdates.DequeueAllSingleConsumer(&resCollection);
        return resCollection;
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
