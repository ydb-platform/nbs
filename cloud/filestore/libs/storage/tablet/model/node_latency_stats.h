#pragma once

#include <cloud/filestore/libs/service/request.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

struct TNodeLatencyStats
{
    ui64 NodeId = 0;
    EFileStoreRequest RequestType = EFileStoreRequest::MAX;
    ui64 RequestCount = 0;
    ui64 TotalLatencyMs = 0;
    double AverageLatencyDecayedMs = 0.0;
    TInstant LastAccessed;
};

class TNodeLatencyStatsTracker
{
private:
    struct TNodeLatencyStatsComparator
    {
        bool operator()(
            const TNodeLatencyStats& lhs,
            const TNodeLatencyStats& rhs) const
        {
            const auto comparisonTime = Max(lhs.LastAccessed, rhs.LastAccessed);
            const double lhsScore = CalculateLatencyDecay(lhs, comparisonTime);
            const double rhsScore = CalculateLatencyDecay(rhs, comparisonTime);

            // AverageLatencyDecayedMs ASC, NodeId ASC
            return std::tie(lhsScore, lhs.NodeId) <
                   std::tie(rhsScore, rhs.NodeId);
        }
    };

    using LatencyKey = std::pair<ui64, EFileStoreRequest>;
    size_t MaxEntries = 0;
    using LatencyRanking = TSet<TNodeLatencyStats, TNodeLatencyStatsComparator>;
    THashMap<LatencyKey, LatencyRanking::iterator> IdAndRequest2Stats;
    LatencyRanking NodeLatencyStats;

    void EvictSmallestLatencyEntries()
    {
        while (NodeLatencyStats.size() > MaxEntries) {
            auto it = NodeLatencyStats.begin();
            LatencyKey key = {it->NodeId, it->RequestType};
            IdAndRequest2Stats.erase(key);
            NodeLatencyStats.erase(it);
        }
    }

public:
    void Initialize(size_t maxEntries);
    void UpdateLatencyStats(
        ui64 nodeId,
        EFileStoreRequest requestType,
        TInstant now,
        TDuration latency);
    static double CalculateLatencyDecay(const TNodeLatencyStats& stats, TInstant now);

    TVector<TNodeLatencyStats> GetLatencyStats(TInstant now) const
    {
        TVector<TNodeLatencyStats> result;
        result.reserve(NodeLatencyStats.size());
        for (auto it = NodeLatencyStats.rbegin(); it != NodeLatencyStats.rend();
             ++it)
        {
            auto stats = *it;
            stats.AverageLatencyDecayedMs = CalculateLatencyDecay(stats, now);
            result.push_back(stats);
        };
        return result;
    }
};

}   // namespace NCloud::NFileStore::NStorage
