#pragma once

#include "ranking.h"

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeAccessStats
{
    ui64 NodeId = 0;
    ui64 RequestCount = 0;
    double AccessScore = 0;
    TInstant LastAccessed;
};

double CalculateDecayedAccessScore(
    const TNodeAccessStats& stats,
    TInstant now,
    TDuration halfLife);

struct TNodeAccessKeyExtractor
{
    ui64 operator()(const TNodeAccessStats& stats) const
    {
        return stats.NodeId;
    }
};

struct TNodeAccessComparator
{
    TDuration HalfLife;

    bool operator()(
        const TNodeAccessStats& lhs,
        const TNodeAccessStats& rhs) const;
};

class TNodeAccessStatsTracker
{
private:
    using TRanking = TBoundedRanking<
        ui64,
        TNodeAccessStats,
        TNodeAccessComparator,
        TNodeAccessKeyExtractor>;

    TRanking Ranking;
    TDuration HalfLife;

public:
    TNodeAccessStatsTracker();

    TNodeAccessStatsTracker(size_t maxEntries, TDuration halfLife);

    void Reset(size_t maxEntries, TDuration halfLife);

    bool UpdateAccessStats(ui64 nodeId, TInstant now);

    TVector<TNodeAccessStats> GetStats(TInstant now, ui32 n) const;
};

}   // namespace NCloud::NFileStore::NStorage
