#include <cloud/filestore/libs/storage/tablet/model/node_latency_stats.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

void TNodeLatencyStatsTracker::Initialize(size_t maxEntries)
{
    MaxEntries = maxEntries;
    NodeLatencyStats.clear();
    Key2Stats.clear();
}

double TNodeLatencyStatsTracker::CalculateLatencyDecay(
    const TNodeLatencyStats& stats,
    TInstant now)
{
    const auto elapsed = now >= stats.LastAccessed ? now - stats.LastAccessed
                                                   : TDuration::Zero();
    return stats.AverageLatencyDecayedMs *
           exp(-log(2) * elapsed.MilliSeconds() /
               TDuration::Minutes(10).MilliSeconds());
}

void TNodeLatencyStatsTracker::UpdateLatencyStats(
    ui64 nodeId,
    EFileStoreRequest requestType,
    TInstant now,
    TDuration latency)
{
    TLatencyKey key = {nodeId, requestType};
    auto it = Key2Stats.find(key);
    TNodeLatencyStats stats;
    if (it != Key2Stats.end()) {
        stats = *it->second;
        NodeLatencyStats.erase(it->second);
    } else {
        stats.NodeId = nodeId;
        stats.RequestType = requestType;
    }

    stats.AverageLatencyDecayedMs = CalculateLatencyDecay(stats, now);

    ++stats.RequestCount;
    stats.TotalLatencyMs += latency.MilliSeconds();
    stats.AverageLatencyDecayedMs =
        static_cast<double>(stats.TotalLatencyMs) / stats.RequestCount;
    stats.LastAccessed = now;

    auto [newLatencyIterator, inserted] = NodeLatencyStats.insert(stats);
    Y_ABORT_UNLESS(inserted);
    Key2Stats[key] = newLatencyIterator;

    EvictSmallestLatencyEntries();
}

}   // namespace NCloud::NFileStore::NStorage
