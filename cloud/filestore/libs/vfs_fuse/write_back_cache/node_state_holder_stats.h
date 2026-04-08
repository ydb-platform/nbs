#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TNodeStateHolderMetrics
{
    struct TNodeMetrics
    {
        NMetrics::IMetricPtr Count;
        NMetrics::IMetricPtr MaxCount;
    };

    TNodeMetrics Nodes;
};

////////////////////////////////////////////////////////////////////////////////

struct INodeStateHolderStats
{
    virtual ~INodeStateHolderStats() = default;

    virtual void IncrementNodeCount() = 0;
    virtual void DecrementNodeCount() = 0;

    virtual TNodeStateHolderMetrics CreateMetrics() const = 0;
    virtual void UpdateStats() = 0;
};

using INodeStateHolderStatsPtr = std::shared_ptr<INodeStateHolderStats>;

INodeStateHolderStatsPtr CreateNodeStateHolderStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
