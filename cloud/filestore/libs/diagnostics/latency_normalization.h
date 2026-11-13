#pragma once

#include <util/generic/types.h>

namespace NCloud::NFileStore::NDiagnostics {

////////////////////////////////////////////////////////////////////////////////

// The prior represents the latency of this many pseudo-requests.
inline constexpr double LatencyPriorRequestCount = 20.0;

inline double NormalizeLatencyUs(
    ui64 totalLatencyUs,
    ui64 requestCount,
    double priorMeanLatencyUs,
    double decayFactor)
{
    if (!requestCount) {
        return 0;
    }

    const double normalizedMean =
        (totalLatencyUs + LatencyPriorRequestCount * priorMeanLatencyUs) /
        (requestCount + LatencyPriorRequestCount);

    return normalizedMean * decayFactor;
}

}   // namespace NCloud::NFileStore::NDiagnostics
