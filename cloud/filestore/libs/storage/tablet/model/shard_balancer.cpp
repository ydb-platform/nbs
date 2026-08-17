#include "shard_balancer.h"

#include <util/generic/algorithm.h>
#include <util/random/random.h>
#include <util/string/builder.h>

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
    const ui64 Unit;

    TShardMetaComp(ui64 precisionBytes, ui32 blockSize)
        : Unit(Max(1UL, precisionBytes / blockSize))
    {}

    ui64 Score(const TShardStats& stats) const
    {
        return Score(FreeSpace(stats));
    }

    ui64 Score(ui64 freeSpace) const
    {
        return static_cast<ui64>(round(freeSpace / static_cast<double>(Unit)));
    }

    bool operator()(
        const TShardBalancerBase::TShardMeta& lhs,
        const TShardBalancerBase::TShardMeta& rhs)
    {
        return lhs.Score == rhs.Score
            ? lhs.ShardIdx < rhs.ShardIdx
            : lhs.Score > rhs.Score;
    }

    bool operator()(ui64 lhs, const TShardBalancerBase::TShardMeta& rhs)
    {
        return Score(lhs) > rhs.Score;
    }
};

////////////////////////////////////////////////////////////////////////////////

TShardBalancerBase::TShardBalancerBase(
        ui32 blockSize,
        ui64 precisionBytes,
        ui32 maxFileBlocks,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds)
    : BlockSize(blockSize)
    , PrecisionBytes(precisionBytes)
    , DesiredFreeSpaceReserve(desiredFreeSpaceReserve)
    , MinFreeSpaceReserve(minFreeSpaceReserve)
{
    // Before the first update shards are treated like empty with
    // infinite capacity. maxFileBlocks is used as infinity here.
    TShardStats emptyShardStats {
        .ShardId = "",
        .TotalBlocksCount = maxFileBlocks,
        .UsedBlocksCount = 0,
        .UsedNodesCount = 0,
        .CurrentLoad = 0,
        .Suffer = 0,
    };

    TShardMetaComp metaCmp(PrecisionBytes, BlockSize);
    Metas.reserve(shardIds.size());
    for (ui32 i = 0; i < shardIds.size(); ++i) {
        emptyShardStats.ShardId = shardIds[i];
        Metas.emplace_back(
            i,
            emptyShardStats,
            metaCmp.Score(emptyShardStats));
    }
}

NProto::TError TShardBalancerBase::Update(
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

    if (stats.size() != Metas.size()) {
        return MakeError(E_ARGUMENT, TStringBuilder() << "stats.size() "
            << stats.size() << " != Metas.size() " << Metas.size());
    }

    const TShardMetaComp metaCmp(PrecisionBytes, BlockSize);
    for (ui32 i = 0; i < stats.size(); ++i) {
        Metas[i] = TShardMeta(i, stats[i], metaCmp.Score(stats[i]));
    }
    Sort(Metas.begin(), Metas.end(), metaCmp);
    return {};
}

size_t TShardBalancerBase::FindUpperBoundAmongAllShardsToFitFile(
    ui64 fileSize) const
{
    const auto* e = UpperBound(
        Metas.begin(),
        Metas.end(),
        (fileSize + DesiredFreeSpaceReserve) / BlockSize,
        TShardMetaComp(PrecisionBytes, BlockSize));
    if (e == Metas.begin()) {
        e = UpperBound(
            Metas.begin(),
            Metas.end(),
            (fileSize + MinFreeSpaceReserve) / BlockSize,
            TShardMetaComp(PrecisionBytes, BlockSize));
    }

    return std::distance(Metas.begin(), e);
}

TVector<TShardStats>
TShardBalancerBase::MakeOrderedShardList() const
{
    TVector<TShardStats> shardStats;
    shardStats.reserve(Metas.size());
    for (const auto& meta: Metas) {
        shardStats.emplace_back(meta.Stats);
    }
    return shardStats;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError TShardBalancerRoundRobin::SelectShard(
    ui64 fileSize,
    TString* shardId)
{
    const auto endIdx = FindUpperBoundAmongAllShardsToFitFile(fileSize);
    if (!endIdx) {
        return MakeError(E_FS_NOSPC, "all shards are full");
    }

    *shardId = Metas[(ShardSelector++) % endIdx].Stats.ShardId;
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

    const auto idx = RandomNumber<ui32>(endIdx);
    *shardId = Metas[idx].Stats.ShardId;
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TShardBalancerWeightedRandom::TShardBalancerWeightedRandom(
        ui32 blockSize,
        ui64 precisionBytes,
        ui32 maxFileBlocks,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds)
    : TShardBalancerBase(
          blockSize,
          precisionBytes,
          maxFileBlocks,
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

NProto::TError TShardBalancerWeightedRandom::Update(
    const TVector<TShardStats>& stats,
    std::optional<ui64> desiredFreeSpaceReserve,
    std::optional<ui64> minFreeSpaceReserve)
{
    auto e = TShardBalancerBase::Update(
        stats,
        desiredFreeSpaceReserve,
        minFreeSpaceReserve);
    if (HasError(e)) {
        return e;
    }

    UpdateWeightPrefixSums();
    return {};
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

    const auto totalFreeSpace = WeightPrefixSums[endIdx];
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
    *shardId = Metas[idx].Stats.ShardId;
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TShardBalancerWeightedDeterministic::TShardBalancerWeightedDeterministic(
    ui32 blockSize,
    ui64 precisionBytes,
    ui32 maxFileBlocks,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve,
    TVector<TString> shardIds)
    : TShardBalancerBase(
          blockSize,
          precisionBytes,
          maxFileBlocks,
          desiredFreeSpaceReserve,
          minFreeSpaceReserve,
          std::move(shardIds))
{
    Y_UNUSED(precisionBytes);

    // Update with empty stats.
    TVector<TShardStats> shardStats(Metas.size());
    for (ui64 i = 0; i < shardStats.size(); ++i) {
        shardStats[i] = Metas[i].Stats;
    }
    Update(shardStats, {}, {});

    ShardSelector = Metas.size() - 1;
    CurrentScore = MaxScore();
}

void TShardBalancerWeightedDeterministic::CalcScore(
    const TVector<TShardStats>& stats)
{
    ui64 minBlocksCount = Max<ui64>();
    ui64 maxBlocksCount = 0;
    for (const auto& stat: stats) {
        minBlocksCount = Min<ui64>(stat.UsedBlocksCount, minBlocksCount);
        maxBlocksCount = Max<ui64>(stat.UsedBlocksCount, maxBlocksCount);
    }

    ui64 step = Max<ui64>(
        (maxBlocksCount - minBlocksCount) / ScoreLevelsCount,
        PrecisionBytes / BlockSize, 1UL);

    Metas.clear();
    Metas.reserve(stats.size());
    for (ui64 i = 0; i < stats.size(); ++i) {
        const ui64 score = Min<ui64>(
            MaxScore(),
            (maxBlocksCount - stats[i].UsedBlocksCount) / step);
        Metas.emplace_back(i, stats[i], score);
    }
}

void TShardBalancerWeightedDeterministic::CalcNextShard()
{
    NextShard.assign(ScoreLevelsCount * Metas.size(), Metas.size());

    auto setNextShard = [this](ui64 shard, ui64 nextShard, ui64 fromScore) {
        for (ui64 score = fromScore; score != Max<ui64>(); --score) {
            if (GetNextShard(shard, score) == Metas.size()) {
                GetNextShard(shard, score) = nextShard;
            } else {
                break;
            }
        }
    };

    const ui64 lastShardIdx = Metas.size() - 1;
    for (ui64 score = 0; score <= Metas[0].Score; ++score) {
        GetNextShard(lastShardIdx, score) = 0;
    }
    TVector<const TShardMeta*> stack;
    stack.push_back(&Metas[0]);

    for (ui64 i = 1; i < Metas.size(); ++i) {
        const auto& meta = Metas[i];
        // Update Next for the last shard.
        setNextShard(lastShardIdx, i, meta.Score);

        // Pop the stack.
        while (stack && meta.Score >= stack.back()->Score) {
            for (ui64 shardIdx = stack.back()->ShardIdx;
                 shardIdx < meta.ShardIdx;
                 ++shardIdx)
            {
                setNextShard(shardIdx, meta.ShardIdx, meta.Score);
            }
            stack.pop_back();
        }

        const ui64 leftBorder = stack ? stack.back()->ShardIdx : 0;
        for (ui64 shardIdx = leftBorder; shardIdx < meta.ShardIdx; ++shardIdx) {
            setNextShard(shardIdx, meta.ShardIdx, meta.Score);
        }

        stack.push_back(&meta);
    }

    // Fill the gap on the right side of the matrix.
    for (ui64 score = 0; score < ScoreLevelsCount &&
                         GetNextShard(lastShardIdx, score) < Metas.size();
         ++score)
    {
        const ui64 nextShard = GetNextShard(lastShardIdx, score);
        for (ui64 shardIdx = lastShardIdx - 1;
             shardIdx != Max<ui64>() &&
             GetNextShard(shardIdx, score) == Metas.size();
             --shardIdx)
        {
            GetNextShard(shardIdx, score) = nextShard;
        }
    }
}

NProto::TError TShardBalancerWeightedDeterministic::Update(
    const TVector<TShardStats>& stats,
    std::optional<ui64> desiredFreeSpaceReserve,
    std::optional<ui64> minFreeSpaceReserve)
{
    Y_UNUSED(desiredFreeSpaceReserve);
    Y_UNUSED(minFreeSpaceReserve);

    if (stats.size() != Metas.size()) {
        return MakeError(E_ARGUMENT, TStringBuilder() << "stats.size() "
            << stats.size() << " != Metas.size() " << Metas.size());
    }

    if (stats) {
        CalcScore(stats);
        CalcNextShard();
    }

    return {};
}

NProto::TError TShardBalancerWeightedDeterministic::SelectShard(
    ui64 fileSize,
    TString* shardId)
{
    Y_UNUSED(fileSize);

    if (!Metas) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Metas.size() is zero");
    }

    // If jumping to the next shard crosses the right boundary,
    // increment CurrentScore.
    const ui64 nextShardIdx = GetNextShard(ShardSelector, CurrentScore);
    if (nextShardIdx < Metas.size() && nextShardIdx <= ShardSelector) {
        CurrentScore = (++CurrentScore) % ScoreLevelsCount;
        ShardSelector = Metas.size() - 1;
    }

    // We are in an undefined part of the NextShard matrix.
    // That means that we should start from the initial state.
    if (GetNextShard(ShardSelector, CurrentScore) == Metas.size()) {
        CurrentScore = 0;
        ShardSelector = Metas.size() - 1;
    }

    ShardSelector = GetNextShard(ShardSelector, CurrentScore);
    *shardId = Metas[ShardSelector].Stats.ShardId;

    return {};
}

TVector<TShardStats>
TShardBalancerWeightedDeterministic::MakeOrderedShardList() const
{
    TVector<TShardMeta> metas(Metas);

    TShardMetaComp metaCmp(PrecisionBytes, BlockSize);
    Sort(metas.begin(), metas.end(), metaCmp);

    TVector<TShardStats> shardStats;
    shardStats.reserve(metas.size());
    for (const auto& meta: metas) {
        shardStats.emplace_back(meta.Stats);
    }

    return shardStats;
}

////////////////////////////////////////////////////////////////////////////////

IShardBalancerPtr CreateShardBalancer(
    NProto::EShardBalancerPolicy policy,
    ui32 blockSize,
    ui64 precisionBytes,
    ui32 maxFileBlocks,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve,
    TVector<TString> shardIds)
{
    switch (policy) {
        case NProto::SBP_ROUND_ROBIN:
            return std::make_shared<TShardBalancerRoundRobin>(
                blockSize,
                precisionBytes,
                maxFileBlocks,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                std::move(shardIds));
        case NProto::SBP_RANDOM:
            return std::make_shared<TShardBalancerRandom>(
                blockSize,
                precisionBytes,
                maxFileBlocks,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                std::move(shardIds));
        case NProto::SBP_WEIGHTED_RANDOM:
            return std::make_shared<TShardBalancerWeightedRandom>(
                blockSize,
                precisionBytes,
                maxFileBlocks,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                std::move(shardIds));
        case NProto::SBP_WEIGHTED_DETERMINISTIC:
            return std::make_shared<TShardBalancerWeightedDeterministic>(
                blockSize,
                precisionBytes,
                maxFileBlocks,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                std::move(shardIds));
        default:
            Y_ABORT(
                "unsupported shard balancer policy: %d",
                static_cast<int>(policy));
    }
}

}   // namespace NCloud::NFileStore::NStorage
