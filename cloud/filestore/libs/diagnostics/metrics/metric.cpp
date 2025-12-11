#include "metric.h"

namespace NCloud::NFileStore::NMetrics {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAtomicMetric: public IMetric
{
private:
    const std::atomic<i64>& Source;

public:
    explicit TAtomicMetric(const std::atomic<i64>& source)
        : Source(source)
    {}

    // IMetric
    i64 Get() const override
    {
        return Source.load(std::memory_order_relaxed);
    }
};

class TDeprecatedAtomicMetric: public IMetric
{
private:
    const TAtomic& Source;

public:
    explicit TDeprecatedAtomicMetric(const TAtomic& source)
        : Source(source)
    {}

    // IMetric
    i64 Get() const override
    {
        return AtomicGet(Source);
    }
};

class TLazyMetric: public IMetric
{
private:
    std::function<i64()> Source;

public:
    explicit TLazyMetric(std::function<i64()> source)
        : Source(std::move(source))
    {}

    // IMetric
    i64 Get() const override
    {
        return Source();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMetricPtr CreateMetric(const std::atomic<i64>& source)
{
    return std::make_shared<TAtomicMetric>(source);
}

IMetricPtr CreateMetric(const TAtomic& source)
{
    return std::make_shared<TDeprecatedAtomicMetric>(source);
}

IMetricPtr CreateMetric(std::function<i64()> source)
{
    return std::make_shared<TLazyMetric>(std::move(source));
}

}   // namespace NCloud::NFileStore::NMetrics
