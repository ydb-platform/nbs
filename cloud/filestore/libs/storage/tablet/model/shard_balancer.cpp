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
        ui32 shardsPerDirectoryCount,
        TVector<TString> shardIds)
    : BlockSize(blockSize)
    , PrecisionBytes(precisionBytes)
    , DesiredFreeSpaceReserve(desiredFreeSpaceReserve)
    , MinFreeSpaceReserve(minFreeSpaceReserve)
{
    Y_UNUSED(shardsPerDirectoryCount);

    // Before the first update shards are treated like empty with
    // infinite capacity. maxFileBlocks is used as infinity here.
    TShardMetaComp metaCmp(PrecisionBytes, BlockSize);
    Metas.reserve(shardIds.size());
    for (ui32 i = 0; i < shardIds.size(); ++i) {
        TShardStats shardStats {
            .ShardId = shardIds[i],
            .TotalBlocksCount = maxFileBlocks,
            .UsedBlocksCount = 0,
            .UsedNodesCount = 0,
            .CurrentLoad = 0,
            .Suffer = 0,
        };
        Metas.emplace_back(
            i,
            shardStats,
            metaCmp.Score(shardStats));
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

TString TShardBalancerBase::Describe() const
{
    TStringBuilder sb;
    for (ui32 i = 0; i < Metas.size(); ++i) {
        if (i) {
            sb << ", ";
        }

        const auto& s = Metas[i].Stats;
        sb << "id=" << s.ShardId
            << " score=" << Metas[i].Score
            << " blocks=" << s.UsedBlocksCount
            << "/" << s.TotalBlocksCount
            << " nodes=" << s.UsedNodesCount
            << " load=" << s.CurrentLoad
            << " suffer=" << s.Suffer;
    }
    return sb;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError TShardBalancerRoundRobin::SelectShard(
    ui64 fileSize,
    TString* shardId,
    ui64 hint)
{
    Y_UNUSED(hint);

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
    TString* shardId,
    ui64 hint)
{
    Y_UNUSED(hint);

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
        ui32 shardsPerDirectoryCount,
        TVector<TString> shardIds)
    : TShardBalancerBase(
          blockSize,
          precisionBytes,
          maxFileBlocks,
          desiredFreeSpaceReserve,
          minFreeSpaceReserve,
          shardsPerDirectoryCount,
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
    TString* shardId,
    ui64 hint)
{
    Y_UNUSED(hint);

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
        ui32 shardsPerDirectoryCount,
        TVector<TString> shardIds)
    : TShardBalancerBase(
          blockSize,
          precisionBytes,
          maxFileBlocks,
          desiredFreeSpaceReserve,
          minFreeSpaceReserve,
          shardsPerDirectoryCount,
          std::move(shardIds)),
    ShardsPerDirectoryCount(shardsPerDirectoryCount)
{
    // Update with zero stats.
    TVector<TShardStats> shardStats(Metas.size());
    for (ui64 i = 0; i < shardStats.size(); ++i) {
        shardStats[i] = Metas[i].Stats;
    }
    Update(shardStats, {}, {});
}

bool TShardBalancerWeightedDeterministic::CalcScore(
    const TVector<TShardStats>& stats)
{
    ui64 minBlocksCount = Max<ui64>();
    for (const auto& stat: stats) {
        minBlocksCount = Min<ui64>(stat.UsedBlocksCount, minBlocksCount);
    }

    const ui64 maxBlocksCount =
        Max<ui64>(stats[0].TotalBlocksCount / stats.size(), minBlocksCount);
    ui64 step = Max<ui64>(
        (maxBlocksCount - minBlocksCount) / ScoreLevelsCount,
        PrecisionBytes / BlockSize, 1UL);

    bool scoreChanged = (Metas.size() != stats.size());
    Metas.resize(stats.size());
    for (ui64 i = 0; i < stats.size(); ++i) {
        const ui64 delta = maxBlocksCount > stats[i].UsedBlocksCount
                               ? maxBlocksCount - stats[i].UsedBlocksCount
                               : 0;
        const ui64 score = Min<ui64>(MaxScore, delta / step);
        scoreChanged = scoreChanged || (Metas[i].Score != score);
        Metas[i] = {static_cast<ui32>(i), stats[i], score};
    }

    return scoreChanged;
}

void TShardBalancerWeightedDeterministic::CalcNextShard()
{
    NextShard.SetSizes(Metas.size(), ScoreLevelsCount);
    NextShard.FillEvery(Metas.size());

    for (ui64 score = 0; score < ScoreLevelsCount; ++score) {
        ui64 firstQualifying = Metas.size();
        for (ui64 shardIdx = 0; shardIdx < Metas.size(); ++shardIdx) {
            if (Metas[shardIdx].Score >= score) {
                firstQualifying = shardIdx;
                break;
            }
        }
        if (firstQualifying == Metas.size()) {
            continue;
        }

        ui64 nextQualifying = firstQualifying;
        for (ui64 shardIdx = Metas.size(); shardIdx-- > 0;) {
            NextShard[score][shardIdx] = nextQualifying;
            if (Metas[shardIdx].Score >= score) {
                nextQualifying = shardIdx;
            }
        }
    }
}

void TShardBalancerWeightedDeterministic::UpdateIterators()
{
    // With empty Metas, the balancer does not make any sense and
    // SelectShard returns an error.
    if (Metas.empty()) {
        return;
    }

    if (ShardsPerDirectoryCount && ShardsPerDirectoryCount < Metas.size()) {
        if (Iterators.size() == Metas.size()) {
            return;
        }
        Iterators.resize(Metas.size());
        for (ui64 i = 0; i < Iterators.size(); ++i) {
            Iterators[i].Init(Metas.size(), i, ShardsPerDirectoryCount);
        }
    } else {
        if (Iterators.size() == 1) {
            return;
        }
        Iterators.resize(1);
        // In this case, we just have one iterator spanning all the shards.
        Iterators[0].Init(Metas.size(), 0, Metas.size());
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

    if (!stats.empty() && CalcScore(stats)) {
        CalcNextShard();
    }
    UpdateIterators();

    return {};
}

void TShardBalancerWeightedDeterministic::Step(TIterator& it)
{
    ui32 nextShardIdx = NextShard[it.CurrentScore][it.LastSelectedShard];

    if (!it.IsInside(nextShardIdx) ||
        (it.IsInside(it.LastSelectedShard) &&
         it.Unwrap(nextShardIdx) <= it.Unwrap(it.LastSelectedShard)))
    {
        it.CurrentScore = (it.CurrentScore + 1) % ScoreLevelsCount;
        it.LastSelectedShard = it.PrevToLeft();
    }

    nextShardIdx = NextShard[it.CurrentScore][it.LastSelectedShard];
    if (!it.IsInside(nextShardIdx)) {
        it.CurrentScore = 0;
        it.LastSelectedShard = it.Left;
    } else {
        it.LastSelectedShard = nextShardIdx;
    }
}

NProto::TError TShardBalancerWeightedDeterministic::SelectShard(
    ui64 fileSize,
    TString* shardId,
    ui64 hint)
{
    Y_UNUSED(fileSize);

    if (Metas.empty()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Metas.size() is zero");
    }

    TIterator& it = Iterators[hint % Iterators.size()];
    Step(it);
    *shardId = Metas[it.LastSelectedShard].Stats.ShardId;

    return {};
}

////////////////////////////////////////////////////////////////////////////////

IShardBalancerPtr CreateShardBalancer(
    NProto::EShardBalancerPolicy policy,
    ui32 blockSize,
    ui64 precisionBytes,
    ui32 maxFileBlocks,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve,
    ui32 shardsPerDirectoryCount,
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
                shardsPerDirectoryCount,
                std::move(shardIds));
        case NProto::SBP_RANDOM:
            return std::make_shared<TShardBalancerRandom>(
                blockSize,
                precisionBytes,
                maxFileBlocks,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                shardsPerDirectoryCount,
                std::move(shardIds));
        case NProto::SBP_WEIGHTED_RANDOM:
            return std::make_shared<TShardBalancerWeightedRandom>(
                blockSize,
                precisionBytes,
                maxFileBlocks,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                shardsPerDirectoryCount,
                std::move(shardIds));
        case NProto::SBP_WEIGHTED_DETERMINISTIC:
            return std::make_shared<TShardBalancerWeightedDeterministic>(
                blockSize,
                precisionBytes,
                maxFileBlocks,
                desiredFreeSpaceReserve,
                minFreeSpaceReserve,
                shardsPerDirectoryCount,
                std::move(shardIds));
        default:
            Y_ABORT(
                "unsupported shard balancer policy: %d",
                static_cast<int>(policy));
    }
}

}   // namespace NCloud::NFileStore::NStorage
