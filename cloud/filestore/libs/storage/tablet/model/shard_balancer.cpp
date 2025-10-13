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
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds)
    : BlockSize(blockSize)
    , DesiredFreeSpaceReserve(desiredFreeSpaceReserve)
    , MinFreeSpaceReserve(minFreeSpaceReserve)
    , Ids(std::move(shardIds))
{
    for (ui32 i = 0; i < Ids.size(); ++i) {
        Metas.emplace_back(
            i,
            // Before the first update shards are treated like empty with
            // infinite capacity Max<ui64>() / 4096 is large enough to serve as
            // infinity and on the other hand all these shards' sizes sum up to
            // a number that is less than Max<ui64>()
            TShardStats{
                .TotalBlocksCount = Max<ui64>() / 4096,
                .UsedBlocksCount = 0,
                .CurrentLoad = 0,
                .Suffer = 0,
            });
    }
}

void TShardBalancerBase::Update(
    const TVector<TShardStats>& stats,
    std::optional<ui64> desiredFreeSpaceReserve,
    std::optional<ui64> minFreeSpaceReserve)
{
    if (desiredFreeSpaceReserve.has_value()) {
        DesiredFreeSpaceReserve = desiredFreeSpaceReserve.value();
    }
    if (minFreeSpaceReserve.has_value()) {
        MinFreeSpaceReserve = minFreeSpaceReserve.value();
    }

    Y_ABORT_UNLESS(stats.size() == Metas.size());
    for (ui32 i = 0; i < stats.size(); ++i) {
        Metas[i] = TShardMeta(i, stats[i]);
    }
    Sort(Metas.begin(), Metas.end(), TShardMetaComp());
}

std::optional<size_t> TShardBalancerBase::FindUpperBoundAmongAllShardsToFitFile(
    ui64 fileSize) const
{
    const auto* e = UpperBound(
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

void TShardBalancerRoundRobin::Update(
    const TVector<TShardStats>& stats,
    std::optional<ui64> desiredFreeSpaceReserve,
    std::optional<ui64> minFreeSpaceReserve)
{
    TShardBalancerBase::Update(
        stats,
        desiredFreeSpaceReserve,
        minFreeSpaceReserve);
    ShardSelector = 0;
}

NProto::TError TShardBalancerRoundRobin::SelectShard(
    ui64 fileSize,
    TString* shardId)
{
    const auto endIdx = FindUpperBoundAmongAllShardsToFitFile(fileSize);
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
    const auto endIdx = FindUpperBoundAmongAllShardsToFitFile(fileSize);
    if (!endIdx) {
        return MakeError(E_FS_NOSPC, "all shards are full");
    }

    const auto idx = RandomNumber<ui32>(endIdx.value());
    *shardId = Ids[Metas[idx].ShardIdx];
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TShardBalancerWeightedRandom::TShardBalancerWeightedRandom(
        ui32 blockSize,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds)
    : TShardBalancerBase(
          blockSize,
          desiredFreeSpaceReserve,
          minFreeSpaceReserve,
          std::move(shardIds))
{
    UpdateWeightPrefixSums();
}

void TShardBalancerWeightedRandom::UpdateWeightPrefixSums()
{
    WeightPrefixSums.clear();
    WeightPrefixSums.push_back(0);
    for (const auto& meta: Metas) {
        WeightPrefixSums.push_back(
            WeightPrefixSums.back() + FreeSpace(meta.Stats));
    }
}

void TShardBalancerWeightedRandom::Update(
    const TVector<TShardStats>& stats,
    std::optional<ui64> desiredFreeSpaceReserve,
    std::optional<ui64> minFreeSpaceReserve)
{
    TShardBalancerBase::Update(
        stats,
        desiredFreeSpaceReserve,
        minFreeSpaceReserve);
    UpdateWeightPrefixSums();
}

NProto::TError TShardBalancerWeightedRandom::SelectShard(
    ui64 fileSize,
    TString* shardId)
{
    const auto endIdx = FindUpperBoundAmongAllShardsToFitFile(fileSize);
    if (!endIdx) {
        return MakeError(E_FS_NOSPC, "all shards are full");
    }

    // Now we need to select a random number from Metas[0...endIdx)
    // proportionally to the free space of the shards.

    const auto totalFreeSpace = WeightPrefixSums[endIdx.value()];
    if (totalFreeSpace == 0) {
        return MakeError(E_FS_NOSPC, "all shards are full");
    }
    const auto randomValue = RandomNumber<ui64>(totalFreeSpace);

    // For array [5, 3] the prefix sums will be [0, 5, 8]
    // random value in range [0, 5) will produce upper bound 1
    // random value in range [5, 8) will produce upper bound 2
    auto* it = UpperBound(
        WeightPrefixSums.begin(),
        WeightPrefixSums.end(),
        randomValue);
    Y_ABORT_UNLESS(it != WeightPrefixSums.begin());
    const size_t idx = std::distance(WeightPrefixSums.begin(), it) - 1;
    Y_ABORT_UNLESS(idx < Metas.size());
    *shardId = Ids[Metas[idx].ShardIdx];
    return {};
}

////////////////////////////////////////////////////////////////////////////////

IShardBalancerPtr CreateShardBalancer(
    NProto::EShardBalancerPolicy policy,
    ui32 blockSize,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve,
    TVector<TString> shardIds)
{
    switch (policy) {
        case NProto::SBP_ROUND_ROBIN:
            return std::make_shared<TShardBalancerRoundRobin>(
                blockSize,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                shardIds);
        case NProto::SBP_RANDOM:
            return std::make_shared<TShardBalancerRandom>(
                blockSize,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                shardIds);
        case NProto::SBP_WEIGHTED_RANDOM:
            return std::make_shared<TShardBalancerWeightedRandom>(
                blockSize,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                shardIds);
        default:
            Y_ABORT(
                "unsupported shard balancer policy: %d",
                static_cast<int>(policy));
    }
}

}   // namespace NCloud::NFileStore::NStorage
