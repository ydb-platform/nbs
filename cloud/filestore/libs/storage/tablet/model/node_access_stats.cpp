#include "node_access_stats.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

double CalculateDecayedAccessScore(
    const TNodeAccessStats& stats,
    TInstant now,
    TDuration halfLife)
{
    const auto elapsed = now - stats.LastAccessed;

    return stats.AccessScore * std::exp(
                                   -std::log(2.0) * elapsed.MicroSeconds() /
                                   halfLife.MicroSeconds());
}

bool TNodeAccessComparator::operator()(
    const TNodeAccessStats& lhs,
    const TNodeAccessStats& rhs) const
{
    const auto comparisonTime = Max(lhs.LastAccessed, rhs.LastAccessed);

    const double lhsScore =
        CalculateDecayedAccessScore(lhs, comparisonTime, HalfLife);

    const double rhsScore =
        CalculateDecayedAccessScore(rhs, comparisonTime, HalfLife);

    return std::tie(lhsScore, lhs.NodeId) < std::tie(rhsScore, rhs.NodeId);
}

////////////////////////////////////////////////////////////////////////////////

TNodeAccessStatsTracker::TNodeAccessStatsTracker(
    size_t maxEntries,
    TDuration halfLife)
    : Ranking(
          maxEntries,
          TNodeAccessComparator{halfLife},
          TNodeAccessKeyExtractor{})
    , HalfLife(halfLife)
{}

void TNodeAccessStatsTracker::RequestStarted(ui64 nodeId, TInstant now)
{
    TNodeAccessStats stats;

    if (const auto* oldStats = Ranking.Find(nodeId)) {
        stats = *oldStats;
    }

    stats.NodeId = nodeId;
    stats.AccessScore = CalculateDecayedAccessScore(stats, now, HalfLife) + 1;
    ++stats.RequestCount;
    stats.LastAccessed = now;

    Ranking.InsertOrUpdate(std::move(stats));
}

TVector<TNodeAccessStats> TNodeAccessStatsTracker::GetStats(
    TInstant now,
    ui32 n) const
{
    auto result = Ranking.GetLastN(n);

    for (auto& stats: result) {
        stats.AccessScore = CalculateDecayedAccessScore(stats, now, HalfLife);
    }

    return result;
}

}   // namespace NCloud::NFileStore::NStorage
