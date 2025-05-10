#include "shard_balancer.h"

#include <util/generic/algorithm.h>
#include <util/random/random.h>

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
        const TShardBalancerBase::TShardMeta& lhs,
        const TShardBalancerBase::TShardMeta& rhs)
    {
        return FreeSpace(lhs.Stats) == FreeSpace(rhs.Stats)
            ? lhs.ShardIdx < rhs.ShardIdx
            : FreeSpace(lhs.Stats) > FreeSpace(rhs.Stats);
    }

    bool operator()(ui64 lhs, const TShardBalancerBase::TShardMeta& rhs)
    {
        return lhs > FreeSpace(rhs.Stats);
    }
};

////////////////////////////////////////////////////////////////////////////////

TShardBalancerBase::TShardBalancerBase(
        ui32 blockSize,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve)
    : BlockSize(blockSize)
    , DesiredFreeSpaceReserve(desiredFreeSpaceReserve)
    , MinFreeSpaceReserve(minFreeSpaceReserve)
{}

void TShardBalancerBase::UpdateShards(TVector<TString> shardIds)
{
    Ids = std::move(shardIds);
    Metas.clear();
    for (ui32 i = 0; i < Ids.size(); ++i) {
        Metas.emplace_back(
            i,
            TShardStats{
                .TotalBlocksCount = DesiredFreeSpaceReserve,
                .UsedBlocksCount = 0,
                .CurrentLoad = 0,
                .Suffer = 0,
            });
    }
}

void TShardBalancerBase::UpdateShardStats(const TVector<TShardStats>& stats)
{
    Y_DEBUG_ABORT_UNLESS(stats.size() == Metas.size());
    for (ui32 i = 0; i < Metas.size(); ++i) {
        Metas[i] = TShardMeta(i, stats[i]);
    }
    Sort(Metas.begin(), Metas.end(), TShardMetaComp());
}

std::optional<size_t> TShardBalancerBase::FindPrefix(ui64 fileSize) const
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
        return std::nullopt;
    }

    return std::distance(Metas.begin(), e);
}

////////////////////////////////////////////////////////////////////////////////

void TShardBalancerRoundRobin::UpdateShards(TVector<TString> shardIds)
{
    TShardBalancerBase::UpdateShards(std::move(shardIds));
    ShardSelector = 0;
}

void TShardBalancerRoundRobin::UpdateShardStats(
    const TVector<TShardStats>& stats)
{
    TShardBalancerBase::UpdateShardStats(stats);
    ShardSelector = 0;
}

NProto::TError TShardBalancerRoundRobin::SelectShard(
    ui64 fileSize,
    TString* shardId)
{
    const auto endIdx = FindPrefix(fileSize);
    if (!endIdx) {
        return MakeError(E_FS_NOSPC, "all shards are full");
    }

    if (ShardSelector >= endIdx.value()) {
        ShardSelector = 0;
    }

    *shardId = Ids[Metas[ShardSelector++].ShardIdx];
    return {};
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError TShardBalancerRandom::SelectShard(
    ui64 fileSize,
    TString* shardId)
{
    const auto endIdx = FindPrefix(fileSize);
    if (!endIdx) {
        return MakeError(E_FS_NOSPC, "all shards are full");
    }

    const auto idx = RandomNumber<ui32>(endIdx.value());
    *shardId = Ids[Metas[idx].ShardIdx];
    return {};
}

////////////////////////////////////////////////////////////////////////////////

IShardBalancerPtr CreateShardBalancer(
    NProto::EShardBalancerPolicy policy,
    ui32 blockSize,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve)
{
    switch (policy) {
        case NProto::SBP_ROUND_ROBIN:
            return std::make_shared<TShardBalancerRoundRobin>(
                blockSize,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve);
        case NProto::SBP_RANDOM:
            return std::make_shared<TShardBalancerRandom>(
                blockSize,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve);
        default:
            Y_ABORT(
                "unsupported shard balancer policy: %d",
                static_cast<int>(policy));
    }
}

}   // namespace NCloud::NFileStore::NStorage
