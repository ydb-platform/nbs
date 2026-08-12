#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

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
        TDuration DecayHalfLife;

        explicit TNodeAccessComparator(TDuration decayHalfLife)
            : DecayHalfLife(decayHalfLife)
        {}

        bool operator()(
            const TNodeAccessStats& lhs,
            const TNodeAccessStats& rhs) const
        {
            const auto comparisonTime = Max(lhs.LastAccessed, rhs.LastAccessed);
            const double lhsScore = DecayedScore(lhs, comparisonTime, DecayHalfLife);
            const double rhsScore = DecayedScore(rhs, comparisonTime, DecayHalfLife);

            // DecayedScore ASC, NodeId ASC
            return std::tie(lhsScore, lhs.NodeId) <
                   std::tie(rhsScore, rhs.NodeId);
        }
    };

    size_t MaxEntries = 0;
    TDuration DecayHalfLife;
    using TStatsSet = TSet<TNodeAccessStats, TNodeAccessComparator>;
    THashMap<ui64, TStatsSet::iterator> NodeId2Stats;
    TStatsSet AccessStats;

    void EvictLeastUsedNodes()
    {
        while (AccessStats.size() > MaxEntries) {
            auto leastAccessed = AccessStats.begin();
            const ui64 nodeId = leastAccessed->NodeId;

            NodeId2Stats.erase(nodeId);
            AccessStats.erase(leastAccessed);
        }
    }

public:
    explicit TNodeAccessStatsTracker(
        size_t maxEntries,
        TDuration decayHalfLife);
    void RequestStarted(ui64 nodeId, TInstant now);
    static double DecayedScore(
        const TNodeAccessStats& stats,
        TInstant now,
        TDuration halfLife);
    TVector<TNodeAccessStats> GetStats(TInstant now, ui32 n) const;
};

}   // namespace NCloud::NFileStore::NStorage
