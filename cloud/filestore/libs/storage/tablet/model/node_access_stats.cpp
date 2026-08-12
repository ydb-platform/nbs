#include <cloud/filestore/libs/storage/tablet/model/node_access_stats.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TNodeAccessStatsTracker::TNodeAccessStatsTracker(
    size_t maxEntries,
    TDuration decayHalfLife)
    : MaxEntries(maxEntries)
    , DecayHalfLife(decayHalfLife)
    , AccessStats(TNodeAccessComparator{decayHalfLife})
{}

double TNodeAccessStatsTracker::DecayedScore(
    const TNodeAccessStats& stats,
    TInstant now,
    TDuration halfLife)
{
    const auto elapsed = now >= stats.LastAccessed ? now - stats.LastAccessed
                                                   : TDuration::Zero();

    // Access Score has a parametarised half-life
    return stats.AccessScore *
           exp(-log(2.0) * elapsed.MilliSeconds() / halfLife.MilliSeconds());
}

void TNodeAccessStatsTracker::RequestStarted(ui64 nodeId, TInstant now)
{
    auto it = NodeId2Stats.find(nodeId);
    TNodeAccessStats stats;

    if (it != NodeId2Stats.end()) {
        stats = *it->second;
        AccessStats.erase(it->second);
    } else {
        stats.NodeId = nodeId;
    }

    ++stats.RequestCount;
    stats.AccessScore = DecayedScore(stats, now, DecayHalfLife) + 1;
    stats.LastAccessed = now;

    auto [newStatsIt, inserted] = AccessStats.insert(stats);
    Y_ABORT_UNLESS(inserted);
    NodeId2Stats[nodeId] = newStatsIt;
    if (it != NodeId2Stats.end()) {
        it->second = newStatsIt;
    } else {
        NodeId2Stats.emplace(nodeId, newStatsIt);
    }

    EvictLeastUsedNodes();
}

TVector<TNodeAccessStats> TNodeAccessStatsTracker::GetStats(
    TInstant now,
    ui32 n) const
{
    TVector<TNodeAccessStats> result;
    result.reserve(AccessStats.size());
    for (auto it = AccessStats.rbegin();
         it != AccessStats.rend() && result.size() < n;
         ++it)
    {
        auto stats = *it;
        stats.AccessScore = DecayedScore(stats, now, DecayHalfLife);
        result.push_back(stats);
    }
    return result;
}

}   // namespace NCloud::NFileStore::NStorage
