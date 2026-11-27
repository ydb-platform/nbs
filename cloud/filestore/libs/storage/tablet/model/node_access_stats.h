#pragma once

#include "ranking.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeAccessStats
{
    ui64 NodeId = 0;
    NProto::ENodeType NodeType = NProto::E_INVALID_NODE;
    ui64 RequestCount = 0;
    double AccessScore = 0;
    TInstant LastAccessed;
};

double CalculateDecayedAccessScore(
    const TNodeAccessStats& stats,
    TInstant now,
    TDuration halfLife);

struct TNodeAccessKeyExtractor
{
    ui64 operator()(const TNodeAccessStats& stats) const
    {
        return stats.NodeId;
    }
};

struct TNodeAccessComparator
{
    TDuration HalfLife;

    bool operator()(
        const TNodeAccessStats& lhs,
        const TNodeAccessStats& rhs) const;
};

class TNodeAccessStatsTracker
{
private:
    using TRanking = TBoundedRanking<
        ui64,
        TNodeAccessStats,
        TNodeAccessComparator,
        TNodeAccessKeyExtractor>;

    TRanking Ranking;
    TDuration HalfLife;
    bool Enabled;

public:
    TNodeAccessStatsTracker();

    TNodeAccessStatsTracker(size_t maxEntries, TDuration halfLife);

    void Reset(size_t maxEntries, TDuration halfLife);

    bool UpdateAccessStats(
        ui64 nodeId,
        NProto::ENodeType nodeType,
        TInstant now);

    TVector<TNodeAccessStats> GetStats(TInstant now, ui32 n) const;
};

}   // namespace NCloud::NFileStore::NStorage
