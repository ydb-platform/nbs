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
    , LatencyStats(TNodeLatencyStatsComparator{decayHalfLife})
{}

bool TNodeLatencyStatsTracker::TNodeLatencyStatsComparator::operator()(
    const TNodeLatencyStats& lhs,
    const TNodeLatencyStats& rhs) const
{
    const auto comparisonTime = Max(lhs.LastAccessed, rhs.LastAccessed);
    const double lhsScore =
        CalculateLatencyDecay(lhs, comparisonTime, DecayHalfLife);
    const double rhsScore =
        CalculateLatencyDecay(rhs, comparisonTime, DecayHalfLife);

    // AverageLatencyDecayedMs ASC, NodeId ASC, RequestType ASC
    return std::tie(lhsScore, lhs.NodeId, lhs.RequestType) <
           std::tie(rhsScore, rhs.NodeId, rhs.RequestType);
}

void TNodeLatencyStatsTracker::EvictSmallestLatencyEntries()
{
    while (LatencyStats.size() > MaxEntries) {
        auto it = LatencyStats.begin();
        TLatencyKey key = {it->NodeId, it->RequestType};
        Key2Stats.erase(key);
        LatencyStats.erase(it);
    }
}

double TNodeLatencyStatsTracker::CalculateLatencyDecay(
    const TNodeLatencyStats& stats,
    TInstant now,
    TDuration halfLife)
{
    const auto elapsed = now >= stats.LastAccessed ? now - stats.LastAccessed
                                                   : TDuration::Zero();

    // Average Latency has a parameterised half-life
    return stats.AverageLatencyDecayedUs *
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

    stats.AverageLatencyDecayedUs =
        CalculateLatencyDecay(stats, now, DecayHalfLife);

    ++stats.RequestCount;
    stats.TotalLatencyUs += latency.MicroSeconds();
    stats.AverageLatencyDecayedUs =
        static_cast<double>(stats.TotalLatencyUs) / stats.RequestCount;
    stats.LastAccessed = now;

    auto [newLatencyIterator, inserted] = LatencyStats.insert(stats);
    Y_ABORT_UNLESS(inserted);
    if (it != Key2Stats.end()) {
        it->second = newLatencyIterator;
    } else {
        Key2Stats.emplace(key, newLatencyIterator);
    }

    EvictSmallestLatencyEntries();
}

TVector<TNodeLatencyStats> TNodeLatencyStatsTracker::GetLatencyStats(
    TInstant now,
    ui32 n) const
{
    TVector<TNodeLatencyStats> result;
    result.reserve(Min<size_t>(n, LatencyStats.size()));
    for (auto it = LatencyStats.rbegin();
         it != LatencyStats.rend() && result.size() < n;
         ++it)
    {
        auto stats = *it;
        stats.AverageLatencyDecayedUs =
            CalculateLatencyDecay(stats, now, DecayHalfLife);
        result.push_back(stats);
    };
    return result;
}

}   // namespace NCloud::NFileStore::NStorage
