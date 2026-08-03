#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

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

            if (lhsScore == rhsScore) {
                return lhs.NodeId < rhs.NodeId;
            }
            return lhsScore < rhsScore;
        }
    };

    using TStatsSet = TSet<TNodeAccessStats, TNodeAccessComparator>;
    size_t MaxEntries = 0;
    THashMap<ui64, TStatsSet::iterator> NodeId2StatsIter;
    TStatsSet StatsRanking;

    void EvictLeastUsedNodes()
    {
        while (StatsRanking.size() > MaxEntries) {
            auto leastAccessed = StatsRanking.begin();
            const ui64 nodeId = leastAccessed->NodeId;

            NodeId2StatsIter.erase(nodeId);
            StatsRanking.erase(leastAccessed);
        }
    }

public:
    void Initialize(size_t maxEntries);
    void RequestStarted(ui64 nodeId, TInstant now);
    static double DecayedScore(const TNodeAccessStats& stats, TInstant now);

    TVector<TNodeAccessStats> GetStats(TInstant now) const
    {
        TVector<TNodeAccessStats> result;
        result.reserve(StatsRanking.size());
        for (auto it = StatsRanking.rbegin(); it != StatsRanking.rend(); ++it) {
            auto stats = *it;
            stats.AccessScore = DecayedScore(stats, now);
            result.push_back(stats);
        }
        return result;
    }
};

}   // namespace NCloud::NFileStore::NStorage
