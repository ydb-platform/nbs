#pragma once

#include <cloud/filestore/libs/service/request.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/digest/multi.h>
#include <util/generic/set.h>

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
        return MultiHash(
            key.NodeId,
            static_cast<ui32>(key.RequestType));
    }
};

class TNodeLatencyStatsTracker
{
private:
    struct TNodeLatencyStatsComparator
    {
        TDuration DecayHalfLife;

        explicit TNodeLatencyStatsComparator(TDuration decayHalfLife)
            : DecayHalfLife(decayHalfLife)
        {}
        bool operator()(
            const TNodeLatencyStats& lhs,
            const TNodeLatencyStats& rhs) const;
    };

    size_t MaxEntries = 0;
    TDuration DecayHalfLife;
    using TLatencyRanking = TSet<TNodeLatencyStats, TNodeLatencyStatsComparator>;
    THashMap<TLatencyKey, TLatencyRanking::iterator, TLatencyKeyHash> Key2Stats;
    TLatencyRanking LatencyStats;

    void EvictSmallestLatencyEntries();

public:
    TNodeLatencyStatsTracker(size_t maxEntries, TDuration decayHalfLife);
    void UpdateLatencyStats(
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
