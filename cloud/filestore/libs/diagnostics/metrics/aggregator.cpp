#include "aggregator.h"

#include "metric.h"

#include <algorithm>
#include <numeric>

namespace NCloud::NFileStore::NMetrics::NImpl {

namespace {

////////////////////////////////////////////////////////////////////////////////

i64 AggregateSum(const auto& metrics, i64 result)
{
    for (const auto& [_, m]: metrics) {
        result += m->Get();
    }
    return result;
}

i64 AggregateAvg(const auto& metrics, i64 result)
{
    i64 count = 0;
    for (const auto& [_, m]: metrics) {
        result += m->Get();
        ++count;
    }
    return count == 0 ? result : result / count;
}

i64 AggregateMax(const auto& metrics, i64 result)
{
    for (const auto& [_, m]: metrics) {
        result = std::max(result, m->Get());
    }
    return result;
}

i64 AggregateMin(const auto& metrics, i64 result)
{
    for (const auto& [_, m]: metrics) {
        result = std::min(result, m->Get());
    }
    return result;
}

i64 Aggregate(const auto& metrics, i64 base, EAggregationType type)
{
    switch (type) {
        case EAggregationType::AT_SUM:
            return AggregateSum(metrics, base);
        case EAggregationType::AT_AVG:
            return AggregateAvg(metrics, base);
        case EAggregationType::AT_MIN:
            return AggregateMin(metrics, Max<i64>());
        case EAggregationType::AT_MAX:
            return AggregateMax(metrics, Min<i64>());
        default:
            Y_ABORT("Unknown aggregation type!!!");
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TAggregator::TAggregator(EAggregationType aggrType, EMetricType metrType)
    : AggregationType(aggrType)
    , MetricType(metrType)
{}

TMetricKey TAggregator::Register(IMetricPtr metric)
{
    const auto _ = Guard(MetricsLock);

    if (MetricType == EMetricType::MT_DERIVATIVE) {
        // Avoid jumps in case we (re)register non-zero value.
        Base -= metric->Get();
    }

    const TMetricKey key(this, GenerateNextFreeKey());
    const bool inserted = Metrics.try_emplace(key, std::move(metric)).second;
    Y_ABORT_UNLESS(inserted);

    return key;
}

bool TAggregator::Unregister(const TMetricKey& key)
{
    const auto _ = Guard(MetricsLock);

    auto it = Metrics.find(key);
    if (it == Metrics.end()) {
        return false;
    }

    const auto metric = it->second;
    Metrics.erase(it);

    if (MetricType == EMetricType::MT_DERIVATIVE) {
        // Move removed value into Base,
        // because it will not be accessible any more.
        Base += metric->Get();
    }
    return Metrics.empty();
}

i64 TAggregator::Aggregate(TInstant /*time*/) const
{
    const auto _ = Guard(MetricsLock);
    return NImpl::Aggregate(Metrics, Base, AggregationType);
}

EAggregationType TAggregator::GetAggregationType() const
{
    return AggregationType;
}

EMetricType TAggregator::GetMetricType() const
{
    return MetricType;
}

}   // namespace NCloud::NFileStore::NMetrics::NImpl
