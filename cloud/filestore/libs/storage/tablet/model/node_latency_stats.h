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

// struct TNodeLatencyStatsKey
// {
//     ui64 NodeId = 0;
//     EFileStoreRequest RequestType = EFileStoreRequest::MAX;

//     bool operator==(const TNodeLatencyStatsKey&) const = default;
// };

// struct TNodeLatencyStatsKeyHash
// {
//     size_t operator()(const TNodeLatencyStatsKey& key) const noexcept
//     {
//         return MultiHash(
//             key.NodeId,
//             static_cast<ui32>(key.RequestType));
//     }
// };

struct TNodeLatencyStats
{
    // TNodeLatencyStatsKey Key;
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
            if(lhs.AverageLatencyDecayedMs == rhs.AverageLatencyDecayedMs) {
                return lhs.NodeId < rhs.NodeId;
            }
            return lhs.AverageLatencyDecayedMs < rhs.AverageLatencyDecayedMs;
        }
    };
    using LatencyKey = std::pair<ui64, EFileStoreRequest>;
    size_t MaxEntries = 0;
    using LatencyRanking = TSet<TNodeLatencyStats, TNodeLatencyStatsComparator>;
    //using LatencyKey = TNodeLatencyStatsKey;
    THashMap<LatencyKey, LatencyRanking::iterator> IdAndRequest2Stats;
    LatencyRanking NodeLatencyStats;

    void EvictSmallestLatencyEntries()
    {
        while(NodeLatencyStats.size() > MaxEntries)
        {
            auto it = NodeLatencyStats.begin();
            LatencyKey key = {it->NodeId, it->RequestType};
            IdAndRequest2Stats.erase(key);
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
