#pragma once

#include <util/system/types.h>

#include <memory>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

enum class EAggregationType : ui8
{
    AT_SUM /* "sum" */,
    AT_AVG /* "average" */,
    AT_MIN /* "min" */,
    AT_MAX /* "max" */,
};

enum class EMetricType : ui8
{
    MT_ABSOLUTE /* "absolute" */,
    MT_DERIVATIVE /* "derivative" */,
};

////////////////////////////////////////////////////////////////////////////////

struct IMetric;
using IMetricPtr = std::shared_ptr<IMetric>;

struct IMetricsRegistry;
using IMetricsRegistryPtr = std::shared_ptr<IMetricsRegistry>;

struct IMainMetricsRegistry;
using IMainMetricsRegistryPtr = std::shared_ptr<IMainMetricsRegistry>;

struct IMetricVisitor;
using IMetricVisitorPtr = std::shared_ptr<IMetricVisitor>;

struct IRegistryVisitor;
using IRegistryVisitorPtr = std::shared_ptr<IRegistryVisitor>;

struct IMetricsService;
using IMetricsServicePtr = std::shared_ptr<IMetricsService>;

}   // namespace NCloud::NFileStore::NMetrics
