#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TNodeStateHolderMetrics
{
    struct TNodeMetrics
    {
        NMetrics::IMetricPtr Count;
        NMetrics::IMetricPtr MaxCount;
    };

    struct TPinMetrics
    {
        NMetrics::IMetricPtr ActiveCount;
        NMetrics::IMetricPtr ActiveMaxCount;
        NMetrics::IMetricPtr CompletedCount;
        NMetrics::IMetricPtr CompletedTime;
        NMetrics::IMetricPtr MaxTime;
    };

    TNodeMetrics Nodes;
    TNodeMetrics DeletedNodes;
    TPinMetrics Pins;

    void Register(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const;
};

////////////////////////////////////////////////////////////////////////////////

struct INodeStateHolderStats
{
    virtual ~INodeStateHolderStats() = default;

    virtual void IncrementNodeCount() = 0;
    virtual void DecrementNodeCount() = 0;

    virtual void IncrementDeletedNodeCount() = 0;
    virtual void DecrementDeletedNodeCount() = 0;

    virtual void Pinned() = 0;
    virtual void Unpinned(TDuration holdDuration) = 0;

    virtual TNodeStateHolderMetrics CreateMetrics() const = 0;
    virtual void UpdateStats(TDuration maxActivePinDuration) = 0;
};

using INodeStateHolderStatsPtr = std::shared_ptr<INodeStateHolderStats>;

INodeStateHolderStatsPtr CreateNodeStateHolderStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
