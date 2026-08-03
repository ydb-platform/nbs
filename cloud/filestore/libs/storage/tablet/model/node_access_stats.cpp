#include <cloud/filestore/libs/storage/tablet/model/node_access_stats.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

void TNodeAccessStatsTracker::Initialize(size_t maxEntries)
{
    MaxEntries = maxEntries;
    NodeId2StatsIter.clear();
    StatsRanking.clear();
}

double TNodeAccessStatsTracker::DecayedScore(
    const TNodeAccessStats& stats,
    TInstant now)
{
    const auto elapsed = now >= stats.LastAccessed ? now - stats.LastAccessed
                                                   : TDuration::Zero();

    // Access Score has a half-life of 10 minutes
    return stats.AccessScore * exp(-log(2.0) * elapsed.GetValue() /
                                   TDuration::Minutes(10).MicroSeconds());
}

void TNodeAccessStatsTracker::RequestStarted(ui64 nodeId, TInstant now)
{
    auto it = NodeId2StatsIter.find(nodeId);
    TNodeAccessStats stats;

    if (it != NodeId2StatsIter.end()) {
        stats = *it->second;
        StatsRanking.erase(it->second);
    } else {
        stats.NodeId = nodeId;
    }

    ++stats.RequestCount;
    stats.AccessScore = DecayedScore(stats, now) + 1;
    stats.LastAccessed = now;

    auto [newStatsIt, inserted] = StatsRanking.insert(stats);
    Y_ABORT_UNLESS(inserted);
    NodeId2StatsIter[nodeId] = newStatsIt;

    EvictLeastUsedNodes();
}

}   // namespace NCloud::NFileStore::NStorage
