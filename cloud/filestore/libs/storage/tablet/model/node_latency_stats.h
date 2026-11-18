#pragma once

#include "ranking.h"

#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>

#include <array>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeLatencyStats
{
    ui64 NodeId = 0;
    NProto::ENodeType NodeType = NProto::E_INVALID_NODE;
    EFileStoreRequest RequestType = EFileStoreRequest::MAX;
    ui64 RequestCount = 0;
    ui64 TotalLatencyUs = 0;
    double AverageLatencyDecayedUs = 0.0;
    TInstant LastAccessed;
};

struct TRequestTypeLatencyStats
{
    EFileStoreRequest RequestType = EFileStoreRequest::MAX;
    ui64 RequestCount = 0;
    ui64 TotalLatencyUs = 0;
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
    std::array<TRequestTypeLatencyStats, FileStoreRequestCount>
        RequestTypeStats{};
    bool Enabled;

public:
    TNodeLatencyStatsTracker();
    TNodeLatencyStatsTracker(size_t maxEntries, TDuration decayHalfLife);
    void Reset(size_t maxEntries, TDuration decayHalfLife);
    bool UpdateLatencyStats(
        ui64 nodeId,
        NProto::ENodeType nodeType,
        EFileStoreRequest requestType,
        TInstant now,
        TDuration latency);
    bool UpdateLatencyStats(
        ui64 nodeId,
        EFileStoreRequest requestType,
        TInstant now,
        TDuration latency)
    {
        return UpdateLatencyStats(
            nodeId,
            NProto::E_INVALID_NODE,
            requestType,
            now,
            latency);
    }
    static double CalculateLatencyDecay(
        const TNodeLatencyStats& stats,
        TInstant now,
        TDuration halfLife);

    TVector<TNodeLatencyStats> GetLatencyStats(TInstant now, ui32 n) const;
    TVector<TRequestTypeLatencyStats> GetRequestTypeStats() const;
};

}   // namespace NCloud::NFileStore::NStorage
