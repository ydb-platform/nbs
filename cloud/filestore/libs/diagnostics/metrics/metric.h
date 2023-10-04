#pragma once

#include "public.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <atomic>
#include <functional>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

struct IMetric
{
    virtual ~IMetric() = default;

    virtual i64 Get() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

// WARNING: Source must live longer than metric ptr!!!

IMetricPtr CreateMetric(std::atomic<i64>&& source) = delete;
IMetricPtr CreateMetric(TAtomic&& source) = delete;

IMetricPtr CreateMetric(const std::atomic<i64>& source);
IMetricPtr CreateMetric(const TAtomic& source);
IMetricPtr CreateMetric(std::function<i64()> source);

}   // namespace NCloud::NFileStore::NMetrics
