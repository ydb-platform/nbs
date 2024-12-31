#include "shard_balancer.h"

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 FreeSpace(const TShardStats& s)
{
    return s.TotalBlocksCount - Min(s.UsedBlocksCount, s.TotalBlocksCount);
}

}   // namespace

struct TShardMetaComp
{
    bool operator()(
        const TShardBalancer::TShardMeta& lhs,
        const TShardBalancer::TShardMeta& rhs)
    {
        return FreeSpace(lhs.Stats) == FreeSpace(rhs.Stats)
            ? lhs.ShardIdx < rhs.ShardIdx
            : FreeSpace(lhs.Stats) > FreeSpace(rhs.Stats);
    }

    bool operator()(ui64 lhs, const TShardBalancer::TShardMeta& rhs)
    {
        return lhs > FreeSpace(rhs.Stats);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TShardBalancer::SetParameters(
    ui32 blockSize,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve)
{
    BlockSize = blockSize;
    DesiredFreeSpaceReserve = desiredFreeSpaceReserve;
    MinFreeSpaceReserve = minFreeSpaceReserve;
}

void TShardBalancer::UpdateShards(TVector<TString> shardIds)
{
    Ids = std::move(shardIds);
    Metas.clear();
    for (ui32 i = 0; i < Ids.size(); ++i) {
        Metas.emplace_back(i, TShardStats{DesiredFreeSpaceReserve, 0, 0, 0});
    }
    ShardSelector = 0;
}

void TShardBalancer::UpdateShardStats(const TVector<TShardStats>& stats)
{
    Y_DEBUG_ABORT_UNLESS(stats.size() == Metas.size());
    ui32 cnt = Min(stats.size(), Metas.size());
    for (ui32 i = 0; i < cnt; ++i) {
        Metas[i].Stats = {};
    }
    Sort(Metas.begin(), Metas.end(), TShardMetaComp());
    for (ui32 i = 0; i < cnt; ++i) {
        Metas[i].Stats = stats[i];
    }
    Sort(Metas.begin(), Metas.end(), TShardMetaComp());
    ShardSelector = 0;
}

NProto::TError TShardBalancer::SelectShard(ui64 fileSize, TString* shardId)
{
    auto* e = UpperBound(
        Metas.begin(),
        Metas.end(),
        (fileSize + DesiredFreeSpaceReserve) / BlockSize,
        TShardMetaComp());
    if (e == Metas.begin()) {
        e = UpperBound(
            Metas.begin(),
            Metas.end(),
            (fileSize + MinFreeSpaceReserve) / BlockSize,
            TShardMetaComp());
    }

    if (e == Metas.begin()) {
        return MakeError(E_FS_NOSPC, "all shards are full");
    }

    const auto endIdx = std::distance(Metas.begin(), e);
    if (ShardSelector >= endIdx) {
        ShardSelector = 0;
    }

    *shardId = Ids[Metas[ShardSelector++].ShardIdx];
    return {};
}

}   // namespace NCloud::NFileStore::NStorage
