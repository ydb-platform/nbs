#pragma once

#include "ranking.h"

#include <cloud/filestore/libs/service/request.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeLatencyStats
{
    ui64 NodeId = 0;
    EFileStoreRequest RequestType = EFileStoreRequest::MAX;
    ui64 RequestCount = 0;
    ui64 TotalLatencyUs = 0;
    double AverageLatencyDecayedUs = 0.0;
    TInstant LastAccessed;
};

struct TLatencyKey
{
    ui64 NodeId = 0;
    EFileStoreRequest RequestType = EFileStoreRequest::MAX;

    bool operator==(const TLatencyKey&) const = default;
};

struct TLatencyKeyHash
{
    size_t operator()(const TLatencyKey& key) const noexcept
    {
        return MultiHash(key.NodeId, static_cast<ui32>(key.RequestType));
    }
};

struct TLatencyKeyExtractor
{
    TLatencyKey operator()(const TNodeLatencyStats& stats) const
    {
        return {stats.NodeId, stats.RequestType};
    }
};

class TNodeLatencyStatsTracker
{
private:
    struct TNodeLatencyStatsComparator
    {
        TDuration DecayHalfLife;

        bool operator()(
            const TNodeLatencyStats& lhs,
            const TNodeLatencyStats& rhs) const;
    };

    TDuration DecayHalfLife;
    using TRanking = TBoundedRanking<
        TLatencyKey,
        TNodeLatencyStats,
        TNodeLatencyStatsComparator,
        TLatencyKeyExtractor,
        TLatencyKeyHash>;

    TRanking Ranking;

public:
    TNodeLatencyStatsTracker(size_t maxEntries, TDuration decayHalfLife);
    bool UpdateLatencyStats(
        ui64 nodeId,
        EFileStoreRequest requestType,
        TInstant now,
        TDuration latency);
    static double CalculateLatencyDecay(
        const TNodeLatencyStats& stats,
        TInstant now,
        TDuration halfLife);

    TVector<TNodeLatencyStats> GetLatencyStats(TInstant now, ui32 n) const;
};

}   // namespace NCloud::NFileStore::NStorage
