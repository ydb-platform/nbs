#include <cloud/filestore/libs/storage/tablet/model/node_latency_stats.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TNodeLatencyStatsTracker::TNodeLatencyStatsTracker(
    size_t maxEntries,
    TDuration decayHalfLife)
    : MaxEntries(maxEntries)
    , DecayHalfLife(decayHalfLife)
    , LatencyStats(
        TNodeLatencyStatsComparator{decayHalfLife})
{}

double TNodeLatencyStatsTracker::CalculateLatencyDecay(
    const TNodeLatencyStats& stats,
    TInstant now,
    TDuration halfLife)
{
    const auto elapsed = now >= stats.LastAccessed ? now - stats.LastAccessed
                                                   : TDuration::Zero();
    return stats.AverageLatencyDecayedMs *
           exp(-log(2) * elapsed.MilliSeconds() / halfLife.MilliSeconds());
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
        LatencyStats.erase(it->second);
    } else {
        stats.NodeId = nodeId;
        stats.RequestType = requestType;
    }

    stats.AverageLatencyDecayedMs =
        CalculateLatencyDecay(stats, now, DecayHalfLife);

    ++stats.RequestCount;
    stats.TotalLatencyMs += latency.MilliSeconds();
    stats.AverageLatencyDecayedMs =
        static_cast<double>(stats.TotalLatencyMs) / stats.RequestCount;
    stats.LastAccessed = now;

    auto [newLatencyIterator, inserted] = LatencyStats.insert(stats);
    Y_ABORT_UNLESS(inserted);
    Key2Stats[key] = newLatencyIterator;

    EvictSmallestLatencyEntries();
}

TVector<TNodeLatencyStats> TNodeLatencyStatsTracker::GetLatencyStats(TInstant now) const
{
    TVector<TNodeLatencyStats> result;
    result.reserve(LatencyStats.size());
    for (auto it = LatencyStats.rbegin(); it != LatencyStats.rend();
            ++it)
    {
        auto stats = *it;
        stats.AverageLatencyDecayedMs = CalculateLatencyDecay(stats, now, DecayHalfLife);
        result.push_back(stats);
    };
    return result;
}

}   // namespace NCloud::NFileStore::NStorage
