#include "node_latency_stats.h"

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TNodeLatencyStatsTracker::TNodeLatencyStatsTracker()
    : Ranking(0, TNodeLatencyStatsComparator{}, TLatencyKeyExtractor{})
    , Enabled(false)
{}

TNodeLatencyStatsTracker::TNodeLatencyStatsTracker(
    size_t maxEntries,
    TDuration decayHalfLife)
    : DecayHalfLife(decayHalfLife)
    , Ranking(
          maxEntries,
          TNodeLatencyStatsComparator{decayHalfLife},
          TLatencyKeyExtractor{})
    , Enabled(maxEntries != 0 && decayHalfLife != TDuration::Zero())
{}

void TNodeLatencyStatsTracker::Reset(size_t maxEntries, TDuration decayHalfLife)
{
    Ranking = TRanking(
        maxEntries,
        TNodeLatencyStatsComparator{decayHalfLife},
        TLatencyKeyExtractor{});

    DecayHalfLife = decayHalfLife;
    Enabled = maxEntries != 0 && decayHalfLife != TDuration::Zero();
}

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

double TNodeLatencyStatsTracker::CalculateLatencyDecay(
    const TNodeLatencyStats& stats,
    TInstant now,
    TDuration halfLife)
{
    const auto elapsed = now - stats.LastAccessed;

    return stats.AverageLatencyDecayedUs *
           exp(-log(2) * elapsed.MicroSeconds() / halfLife.MicroSeconds());
}

bool TNodeLatencyStatsTracker::UpdateLatencyStats(
    ui64 nodeId,
    EFileStoreRequest requestType,
    TInstant now,
    TDuration latency)
{
    if (!Enabled) {
        return true;
    }

    TLatencyKey key = {nodeId, requestType};
    TNodeLatencyStats stats;

    if (const auto* oldStats = Ranking.Find(key)) {
        stats = *oldStats;
    }
    stats.NodeId = nodeId;
    stats.RequestType = requestType;
    ++stats.RequestCount;
    stats.TotalLatencyUs += latency.MicroSeconds();
    stats.AverageLatencyDecayedUs =
        static_cast<double>(stats.TotalLatencyUs) / stats.RequestCount;
    stats.LastAccessed = now;

    return Ranking.InsertOrUpdate(std::move(stats));
}

TVector<TNodeLatencyStats> TNodeLatencyStatsTracker::GetLatencyStats(
    TInstant now,
    ui32 n) const
{
    auto result = Ranking.GetLastN(n);

    for (auto& stats: result) {
        stats.AverageLatencyDecayedUs =
            CalculateLatencyDecay(stats, now, DecayHalfLife);
    }

    return result;
}

}   // namespace NCloud::NFileStore::NStorage
