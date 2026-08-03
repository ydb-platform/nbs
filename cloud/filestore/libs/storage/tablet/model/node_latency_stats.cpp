#include <cloud/filestore/libs/storage/tablet/model/node_latency_stats.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {


void TNodeLatencyStatsTracker::Initialize(size_t maxEntries)
{
    MaxEntries = maxEntries;
    NodeLatencyStats.clear();
    IdAndRequest2Stats.clear();
}

void TNodeLatencyStatsTracker::CalculateLatencyDecay(TNodeLatencyStats& stats, TInstant now) const
{
    const auto elapsedUs = now >= stats.LastAccessed ? now - stats.LastAccessed
                                                   : TDuration::Zero();
    stats.AverageLatencyDecayedMs *= exp(-log(2) * elapsedUs.GetValue() / TDuration::Minutes(10).MicroSeconds());
}

void TNodeLatencyStatsTracker::UpdateLatencyStats(ui64 nodeId, EFileStoreRequest requestType, TInstant now, TDuration latency)
{
    LatencyKey key = {nodeId, requestType};
    auto it = IdAndRequest2Stats.find(key);
    TNodeLatencyStats stats;
    if(it != IdAndRequest2Stats.end())
    {
        stats = *it->second;
        IdAndRequest2Stats.erase(it->second);
    }
    else
    {
        stats.Key = key;
    }
    CalculateLatencyDecay(stats, now);
    ++stats.RequestCount;
    stats.TotalLatencyMs += latency.GetValue();
    stats.AverageLatencyDecayedMs = stats.TotalLatencyMs / stats.RequestCount;

    auto [newLatencyIterator, inserted] = NodeLatencyStats.insert(stats);
    Y_ABORT_UNLESS(inserted);
    IdAndRequest2Stats[key] = newLatencyIterator;

    EvictSmallestLatencyEntries();
}

}   // namespace NCloud::NFileStore::NStorage
