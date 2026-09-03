#pragma once

#include "public.h"

#include <cloud/filestore/config/storage.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/containers/2d_array/2d_array.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TShardStats
{
    TString ShardId;
    ui64 TotalBlocksCount = 0;
    ui64 UsedBlocksCount = 0;
    ui64 UsedNodesCount = 0;
    ui64 CurrentLoad = 0;
    ui64 Suffer = 0;
};

////////////////////////////////////////////////////////////////////////////////

class IShardBalancer
{
public:
    struct TShardMeta
    {
        ui32 ShardIdx;
        TShardStats Stats;
        ui64 Score;

        TShardMeta(ui32 shardIdx, TShardStats stats, ui64 score)
            : ShardIdx(shardIdx)
            , Stats(stats)
            , Score(score)
        {}

        TShardMeta() = default;
    };

    virtual ~IShardBalancer() = default;

    virtual NProto::TError Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve,
        std::optional<ui64> minFreeSpaceReserve) = 0;
    virtual NProto::TError SelectShard(
        ui64 fileSize,
        TString* shardId,
        ui64 hint) = 0;

    NProto::TError Update(const TVector<TShardStats>& stats)
    {
        return Update(stats, {}, {});
    }

    [[nodiscard]] virtual TString Describe() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TShardBalancerBase: public IShardBalancer
{
public:
    TShardBalancerBase(
        ui32 blockSize,
        ui64 precisionBytes,
        ui32 maxFileBlocks,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        ui32 shardsPerDirectoryCount,
        TVector<TString> shardIds);

protected:
    const ui32 BlockSize;
    const ui64 PrecisionBytes;
    ui64 DesiredFreeSpaceReserve = 0;
    ui64 MinFreeSpaceReserve = 0;

    TVector<TShardMeta> Metas;

    /**
     * @brief Finds the number of shards that can fit a file of the given size.
     *
     * This method assumes that the `Metas` vector is sorted in descending order
     * of free space. It performs a binary search to find the first shard that
     * cannot fit the target file size with the `DesiredFreeSpaceReserve`. If no
     * shard can fit the file size, the same operation is performed with the
     * `MinFreeSpaceReserve`.
     *
     * @param fileSize The size of the file to fit.
     * @return The number of shards that can fit the file size, zero if no shard
     * can fit the file size.
     */
    [[nodiscard]] size_t FindUpperBoundAmongAllShardsToFitFile(
        ui64 fileSize) const;

public:
    using IShardBalancer::Update;

    NProto::TError Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve,
        std::optional<ui64> minFreeSpaceReserve) override;

    [[nodiscard]] TString Describe() const override;
};

/////////////////////////////////////////////////////////////////////////////////

class TShardBalancerRoundRobin: public TShardBalancerBase
{
private:
    ui32 ShardSelector = 0;

public:
    using TShardBalancerBase::TShardBalancerBase;
    NProto::TError SelectShard(
        ui64 fileSize,
        TString* shardId,
        ui64 hint) final;
};

/////////////////////////////////////////////////////////////////////////////////

class TShardBalancerRandom: public TShardBalancerBase
{
public:
    using TShardBalancerBase::TShardBalancerBase;
    NProto::TError SelectShard(
        ui64 fileSize,
        TString* shardId,
        ui64 hint) final;
};

/////////////////////////////////////////////////////////////////////////////////

class TShardBalancerWeightedRandom: public TShardBalancerBase
{
private:
    // To be able to perform weighed sampling from a list of weights, we store
    // all weights prefix sums and use binary search to find the item
    // corresponding with a random number selected from the range [0,
    // sum(weights)).
    TVector<ui64> WeightPrefixSums;

    void UpdateWeightPrefixSums();

public:
    TShardBalancerWeightedRandom(
        ui32 blockSize,
        ui64 precisionBytes,
        ui32 maxFileBlocks,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        ui32 shardsPerDirectoryCount,
        TVector<TString> shardIds);

public:
    using IShardBalancer::Update;

    NProto::TError Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve,
        std::optional<ui64> minFreeSpaceReserve) final;

    NProto::TError SelectShard(
        ui64 fileSize,
        TString* shardId,
        ui64 hint) final;
};

////////////////////////////////////////////////////////////////////////////////

// Deterministically selects shards with frequencies proportional to their
// scores. The balancer traverses score levels from lowest to highest. At each
// level, it visits every shard whose score is at least that level. Therefore,
// a shard with score N is selected N + 1 times during a complete traversal.
//
// For each hint, the traversal is limited to ShardsPerDirectoryCount shards
// starting with the shard at hint % Metas.size(). The shard list is treated as
// circular, and each normalized hint has an independent traversal state.
//
// For example, five shards with scores {0, 2, 4, 3, 1} are visited according
// to the following table. Read it row by row from top to bottom, from left to
// right; an "x" marks a shard skipped at that score level:
//
// 0 1 2 3 4
// x 1 2 3 4
// x 1 2 3 x
// x x 2 3 x
// x x 2 x x
class TShardBalancerWeightedDeterministic: public TShardBalancerBase
{
    static constexpr ui32 ScoreLevelsCount = 8;
    static constexpr ui32 MaxScore = ScoreLevelsCount - 1;

    struct TIterator
    {
        ui32 LastSelectedShard = Max<ui32>();
        ui32 CurrentScore = MaxScore;
        ui32 Left = 0;
        ui32 Right = 0;
        ui32 ShardCount = 0;

        void Init(
            const ui32 shardCount,
            const ui32 iteratorIdx,
            const ui32 shardsPerDirectoryCount)
        {
            ShardCount = shardCount;
            Left = iteratorIdx % shardCount;
            Right = (Left + shardsPerDirectoryCount - 1) % ShardCount;
            if (!IsInside(LastSelectedShard)) {
                LastSelectedShard = Right;
                CurrentScore = MaxScore;
            }
        }

        ui32 PrevToLeft() const
        {
            if (Left > 0) {
                return Left - 1;
            } else {
                return ShardCount - 1;
            }
        }

        bool IsInside(const ui32 idx) const
        {
            if (Left <= Right) {
                return idx >= Left && idx <= Right;
            } else {
                return (idx >= Left && idx < ShardCount) || idx <= Right;
            }
        }

        ui32 Unwrap(const ui32 idx) const
        {
            if (idx >= Left) {
                return idx;
            } else {
                return idx + ShardCount;
            }
        }
    };

    const ui32 ShardsPerDirectoryCount;

    TVector<TIterator> Iterators;

    // Two-dimensional array of size:
    // Metas.size() * ScoreLevelsCount.
    // It's a mapping from (score, shardIdx) -> nextShardIdx.
    TArray2D<ui32> NextShard;

    // Calculates scores in Metas, returns true if some score has changed.
    bool CalcScore(const TVector<TShardStats>& stats);
    void CalcNextShard();
    void UpdateIterators();

    void Step(TIterator& it);

public:
    TShardBalancerWeightedDeterministic(
        ui32 blockSize,
        ui64 precisionBytes,
        ui32 maxFileBlocks,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        ui32 shardsPerDirectoryCount,
        TVector<TString> shardIds);

    using IShardBalancer::Update;

    NProto::TError Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve,
        std::optional<ui64> minFreeSpaceReserve) final;

    NProto::TError SelectShard(
        ui64 fileSize,
        TString* shardId,
        ui64 hint) final;
};

////////////////////////////////////////////////////////////////////////////////

IShardBalancerPtr CreateShardBalancer(
    NProto::EShardBalancerPolicy policy,
    ui32 blockSize,
    ui64 precisionBytes,
    ui32 maxFileBlocks,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve,
    ui32 shardsPerDirectoryCount,
    TVector<TString> shardIds);

}   // namespace NCloud::NFileStore::NStorage
