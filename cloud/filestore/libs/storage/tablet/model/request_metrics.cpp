#include "request_metrics.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TTabletRequestMetrics::Update(
    ui64 requestCount,
    ui64 requestBytes,
    TDuration d)
{
    Count.fetch_add(requestCount, std::memory_order_relaxed);
    RequestBytes.fetch_add(requestBytes, std::memory_order_relaxed);
    TimeSumUs.fetch_add(
        d.MicroSeconds(),
        std::memory_order_relaxed);
    Time.Record(d);
}

void TTabletRequestMetrics::UpdatePrev(TInstant now)
{
    PrevCount = Count.load(std::memory_order_relaxed);
    PrevRequestBytes = RequestBytes.load(std::memory_order_relaxed);
    PrevTimeSumUs = TimeSumUs.load(std::memory_order_relaxed);
    PrevTs = now;
}

double TTabletRequestMetrics::RPS(TInstant now) const
{
    return Rate(now, Count, PrevCount);
}

double TTabletRequestMetrics::Throughput(TInstant now) const
{
    return Rate(now, RequestBytes, PrevRequestBytes);
}

double TTabletRequestMetrics::AverageSecondsPerSecond(TInstant now) const
{
    return Rate(now, TimeSumUs, PrevTimeSumUs) * 1e-6;
}

ui64 TTabletRequestMetrics::AverageRequestSize() const
{
    const auto requestCount =
        Count.load(std::memory_order_relaxed) - PrevCount;
    if (!requestCount) {
        return 0;
    }

    const auto requestBytes =
        RequestBytes.load(std::memory_order_relaxed)
        - PrevRequestBytes;
    return requestBytes / requestCount;
}

TDuration TTabletRequestMetrics::AverageLatency() const
{
    const auto requestCount =
        Count.load(std::memory_order_relaxed) - PrevCount;
    if (!requestCount) {
        return TDuration::Zero();
    }

    const auto timeSumUs =
        TimeSumUs.load(std::memory_order_relaxed) - PrevTimeSumUs;
    return TDuration::MicroSeconds(timeSumUs / requestCount);
}

////////////////////////////////////////////////////////////////////////////////

double TTabletRequestMetrics::Rate(
    TInstant now,
    const std::atomic<i64>& counter,
    ui64 prevCounter) const
{
    if (!PrevTs) {
        return 0;
    }

    auto micros = (now - PrevTs).MicroSeconds();
    if (!micros) {
        return 0;
    }

    auto cur = counter.load(std::memory_order_relaxed);
    return (cur - prevCounter) * 1'000'000. / micros;
}

}   // namespace NCloud::NFileStore::NStorage
