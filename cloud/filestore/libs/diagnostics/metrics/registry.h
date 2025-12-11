#pragma once

#include "public.h"

#include "key.h"
#include "label.h"
#include "metric.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <atomic>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

struct IMetricsRegistry
{
    virtual ~IMetricsRegistry() = default;

    virtual TMetricKey Register(
        const TLabels& labels,
        IMetricPtr metric,
        EAggregationType aggrType = EAggregationType::AT_SUM,
        EMetricType metrType = EMetricType::MT_ABSOLUTE) = 0;

    // WARNING: Source must live until Unregister or ~Dtor() is called!!!
    template <typename TSource>
    TMetricKey Register(
        const TLabels& labels,
        TSource&& source,
        EAggregationType aggrType = EAggregationType::AT_SUM,
        EMetricType metrType = EMetricType::MT_ABSOLUTE)
    {
        return Register(
            labels,
            CreateMetric(std::forward<TSource>(source)),
            aggrType,
            metrType);
    }

    virtual void Unregister(const TMetricKey& key) = 0;
};

struct IMainMetricsRegistry: public IMetricsRegistry
{
    virtual ~IMainMetricsRegistry() = default;

    virtual void Visit(TInstant time, IRegistryVisitor& visitor) = 0;
    virtual void Update(TInstant time) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IMainMetricsRegistryPtr CreateMetricsRegistry(
    TLabels commonLabels,
    NMonitoring::TDynamicCountersPtr rootCounters);

IMetricsRegistryPtr CreateScopedMetricsRegistry(
    TLabels commonLabels,
    IMetricsRegistryPtr subRegistry);

IMetricsRegistryPtr CreateScopedMetricsRegistry(
    TLabels commonLabels,
    TVector<IMetricsRegistryPtr> subRegistries);

IMetricsRegistryPtr CreateMetricsRegistryStub();

}   // namespace NCloud::NFileStore::NMetrics
