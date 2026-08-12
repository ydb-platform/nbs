#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeAccessStats
{
    ui64 NodeId = 0;
    ui64 RequestCount = 0;
    double AccessScore = 0;
    TInstant LastAccessed;
};

class TNodeAccessStatsTracker
{
private:
    struct TNodeAccessComparator
    {
        bool operator()(
            const TNodeAccessStats& lhs,
            const TNodeAccessStats& rhs) const
        {
            const auto comparisonTime = Max(lhs.LastAccessed, rhs.LastAccessed);
            const double lhsScore = DecayedScore(lhs, comparisonTime);
            const double rhsScore = DecayedScore(rhs, comparisonTime);

            // DecayedScore ASC, NodeId ASC
            return std::tie(lhsScore, lhs.NodeId) <
                   std::tie(rhsScore, rhs.NodeId);
        }
    };

    using TStatsSet = TSet<TNodeAccessStats, TNodeAccessComparator>;
    size_t MaxEntries = 0;
    THashMap<ui64, TStatsSet::iterator> NodeId2Stats;
    TStatsSet StatsRanking;

    void EvictLeastUsedNodes()
    {
        while (StatsRanking.size() > MaxEntries) {
            auto leastAccessed = StatsRanking.begin();
            const ui64 nodeId = leastAccessed->NodeId;

            NodeId2Stats.erase(nodeId);
            StatsRanking.erase(leastAccessed);
        }
    }

public:
    explicit TNodeAccessStatsTracker(size_t maxEntries);
    void RequestStarted(ui64 nodeId, TInstant now);
    static double DecayedScore(const TNodeAccessStats& stats, TInstant now);
    TVector<TNodeAccessStats> GetStats(TInstant now, ui32 n) const;
};

}   // namespace NCloud::NFileStore::NStorage
