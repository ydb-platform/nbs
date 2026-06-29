#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/histogram.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TLatHistogram =
    NMetrics::THistogram<NMetrics::EHistUnit::HU_TIME_MICROSECONDS>;

struct TTabletRequestMetrics
{
    std::atomic<i64> Count = 0;
    std::atomic<i64> RequestBytes = 0;
    std::atomic<i64> TimeSumUs = 0;
    TLatHistogram Time;

    ui64 PrevCount = 0;
    ui64 PrevRequestBytes = 0;
    ui64 PrevTimeSumUs = 0;
    TInstant PrevTs;

    void Update(ui64 requestCount, ui64 requestBytes, TDuration d);
    void UpdatePrev(TInstant now);
    double RPS(TInstant now) const;
    double Throughput(TInstant now) const;
    double AverageSecondsPerSecond(TInstant now) const;
    ui64 AverageRequestSize() const;
    TDuration AverageLatency() const;

private:
    double Rate(
        TInstant now,
        const std::atomic<i64>& counter,
        ui64 prevCounter) const;
};

}   // namespace NCloud::NFileStore::NStorage
