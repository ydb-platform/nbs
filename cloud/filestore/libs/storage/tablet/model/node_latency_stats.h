#pragma once

#include <cloud/filestore/libs/service/request.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

struct TNodeLatencyStats
{
    struct TNodeLatencyStatsKey
    {
        ui64 NodeId = 0;
        EFileStoreRequest RequestType = EFileStoreRequest::MAX;
    } Key;
    ui64 RequestCount = 0;
    ui64 TotalLatencyMs = 0;
    double AverageLatencyDecayedMs = 0.0;
    TInstant LastAccessed;
};

class TNodeLatencyStatsComparator
{
public:
    bool operator()(
        const TNodeLatencyStats& lhs,
        const TNodeLatencyStats& rhs) const
    {
        if (lhs.AverageLatencyDecayedMs == rhs.AverageLatencyDecayedMs) {
            return lhs.Key.NodeId < rhs.Key.NodeId;
        }
        return lhs.AverageLatencyDecayedMs < rhs.AverageLatencyDecayedMs;
    }
};

class TNodeLatencyStatsTracker
{
private:
    //using LatencyKey = std::pair<ui64, EFileStoreRequest>;
    size_t MaxEntries = 0;
    using LatencyRanking = TSet<TNodeLatencyStats, TNodeLatencyStatsComparator>;
    using LatencyKey = TNodeLatencyStats::TNodeLatencyStatsKey;
    THashMap<LatencyKey, LatencyRanking::iterator> IdAndRequest2Stats;
    LatencyRanking NodeLatencyStats;

    void EvictSmallestLatencyEntries()
    {
        while(NodeLatencyStats.size() > MaxEntries)
        {
            auto it = NodeLatencyStats.begin();
            IdAndRequest2Stats.erase(it->Key);
            NodeLatencyStats.erase(it);
        }
    };

public:
    void Initialize(size_t maxEntries);
    void UpdateLatencyStats(ui64 nodeId, EFileStoreRequest requestType, TInstant now, TDuration latency);
    void CalculateLatencyDecay(TNodeLatencyStats& stats, TInstant now) const;
    TVector<TNodeLatencyStats> GetLatencyStats(TInstant now) const
    {
        TVector<TNodeLatencyStats> result;
        result.reserve(NodeLatencyStats.size());
        for(auto it = NodeLatencyStats.rbegin(); it != NodeLatencyStats.rend(); ++it)
        {
            auto stats = *it;
            CalculateLatencyDecay(stats, now);
            result.push_back(stats);
        };
        return result;
    };
};

}   // namespace NCloud::NFileStore::NStorage
