#pragma once

#include <util/system/types.h>

namespace NCloud::NFileStore::NDiagnostics {

////////////////////////////////////////////////////////////////////////////////

/* Using https://en.wikipedia.org/wiki/Bayesian_average
so that consistent latency across multiple requests ranks above sporadic spikes. */

inline constexpr double LatencyPriorRequestCount = 20.0;

inline double NormalizeLatencyUs(
    ui64 totalLatencyUs,
    ui64 requestCount,
    double priorAvgLatencyUs,
    double decayFactor)
{
    if (!requestCount) {
        return 0;
    }

    const double normalizedAvg =
        ((LatencyPriorRequestCount * priorAvgLatencyUs) + totalLatencyUs) /
        (LatencyPriorRequestCount + requestCount);

    return normalizedAvg * decayFactor;
}

}   // namespace NCloud::NFileStore::NDiagnostics
