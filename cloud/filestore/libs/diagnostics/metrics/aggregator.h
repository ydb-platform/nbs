#pragma once

#include "public.h"

#include "key.h"

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NMetrics::NImpl {

////////////////////////////////////////////////////////////////////////////////

class TAggregator: private TMetricNextFreeKey
{
    using TMetricsMap = THashMap<TMetricKey, IMetricPtr>;

private:
    const EAggregationType AggregationType;
    const EMetricType MetricType;

    i64 Base{0};

    TMetricsMap Metrics;
    TAdaptiveLock MetricsLock;

public:
    TAggregator(EAggregationType aggrType, EMetricType metrType);

    TMetricKey Register(IMetricPtr metric);

    // Returns true if the current metric is the last one.
    bool Unregister(const TMetricKey& key);

    i64 Aggregate(TInstant time) const;

    EAggregationType GetAggregationType() const;
    EMetricType GetMetricType() const;
};

}   // namespace NCloud::NFileStore::NMetrics::NImpl
