#pragma once

#include <cloud/filestore/libs/service/request.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeLatencyStats
{
    ui64 NodeId = 0;
    EFileStoreRequest RequestType = EFileStoreRequest::MAX;
    ui64 RequestCount = 0;
    ui64 TotalLatencyMs = 0;
    double AverageLatencyDecayedMs = 0.0;
    TInstant LastAccessed;
};

struct TLatencyKey
{
    ui64 NodeId = 0;
    EFileStoreRequest RequestType = EFileStoreRequest::MAX;
};

class TNodeLatencyStatsTracker
{
private:
    struct TNodeLatencyStatsComparator
    {
        TDuration DecayHalfLife;

        explicit TNodeLatencyStatsComparator(TDuration decayHalfLife)
            : DecayHalfLife(decayHalfLife)
        {}
        bool operator()(
            const TNodeLatencyStats& lhs,
            const TNodeLatencyStats& rhs) const
        {
            const auto comparisonTime = Max(lhs.LastAccessed, rhs.LastAccessed);
            const double lhsScore = CalculateLatencyDecay(lhs, comparisonTime, DecayHalfLife);
            const double rhsScore = CalculateLatencyDecay(rhs, comparisonTime, DecayHalfLife);

            // AverageLatencyDecayedMs ASC, NodeId ASC
            return std::tie(lhsScore, lhs.NodeId) <
                   std::tie(rhsScore, rhs.NodeId);
        }
    };

    size_t MaxEntries = 0;
    TDuration DecayHalfLife;
    using TLatencyRanking = TSet<TNodeLatencyStats, TNodeLatencyStatsComparator>;
    THashMap<TLatencyKey, TLatencyRanking::iterator> Key2Stats;
    TLatencyRanking LatencyStats;

    void EvictSmallestLatencyEntries()
    {
        while (LatencyStats.size() > MaxEntries) {
            auto it = LatencyStats.begin();
            TLatencyKey key = {it->NodeId, it->RequestType};
            Key2Stats.erase(key);
            LatencyStats.erase(it);
        }
    }

public:
    TNodeLatencyStatsTracker(size_t maxEntries, TDuration decayHalfLife);
    void UpdateLatencyStats(
        ui64 nodeId,
        EFileStoreRequest requestType,
        TInstant now,
        TDuration latency);
    static double CalculateLatencyDecay(
        const TNodeLatencyStats& stats,
        TInstant now,
        TDuration halfLife);

    TVector<TNodeLatencyStats> GetLatencyStats(TInstant now) const;
};

}   // namespace NCloud::NFileStore::NStorage
