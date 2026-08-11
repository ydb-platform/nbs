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
    ui64 TotalLatencyUs = 0;
    double AverageLatencyDecayedUs = 0.0;
    TInstant LastAccessed;
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

            // AverageLatencyDecayedMs ASC, NodeId ASC, RequestType ASC
            return std::tie(lhsScore, lhs.NodeId, lhs.RequestType) <
                   std::tie(rhsScore, rhs.NodeId, rhs.RequestType);
        }
    };

    size_t MaxEntries = 0;
    TDuration DecayHalfLife;
    using TLatencyKey = std::pair<ui64, EFileStoreRequest>;
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

    TVector<TNodeLatencyStats> GetLatencyStats(TInstant now, ui32 n) const;
};

}   // namespace NCloud::NFileStore::NStorage
