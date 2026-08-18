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
    };

    virtual ~IShardBalancer() = default;

    virtual NProto::TError Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve,
        std::optional<ui64> minFreeSpaceReserve) = 0;
    virtual NProto::TError SelectShard(ui64 fileSize, TString* shardId) = 0;

    NProto::TError Update(const TVector<TShardStats>& stats)
    {
        return Update(stats, {}, {});
    }

    /**
     * @brief Builds and returns the shard list ordered from best to worst.
     *
     * This method builds a new vector with TShardStats so it's not supposed to
     * be used often because it's expensive. The main intended use case is
     * introspection - log this info with debug loglevel or show it on monpages.
     *
     * @return The list of shard TShardStats.
     */
    [[nodiscard]] virtual TVector<TShardStats> MakeOrderedShardList() const = 0;
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

    [[nodiscard]] TVector<TShardStats> MakeOrderedShardList() const override;
};

/////////////////////////////////////////////////////////////////////////////////

class TShardBalancerRoundRobin: public TShardBalancerBase
{
private:
    ui32 ShardSelector = 0;

public:
    using TShardBalancerBase::TShardBalancerBase;
    NProto::TError SelectShard(ui64 fileSize, TString* shardId) final;
};

/////////////////////////////////////////////////////////////////////////////////

class TShardBalancerRandom: public TShardBalancerBase
{
public:
    using TShardBalancerBase::TShardBalancerBase;
    NProto::TError SelectShard(ui64 fileSize, TString* shardId) final;
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
        TVector<TString> shardIds);

public:
    using IShardBalancer::Update;

    NProto::TError Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve,
        std::optional<ui64> minFreeSpaceReserve) final;

    NProto::TError SelectShard(ui64 fileSize, TString* shardId) final;
};

////////////////////////////////////////////////////////////////////////////////

class TShardBalancerWeightedDeterministic: public TShardBalancerBase
{
    static constexpr ui64 ScoreLevelsCount = 8;
    static constexpr ui64 MaxScore = ScoreLevelsCount - 1;

    // Iterator state.
    ui64 ShardSelector;
    ui64 CurrentScore;

    // Two-dimensional array of size:
    // Metas.size() * ScoreLevelsCount.
    // It's a mapping from (score, shardIdx) -> nextShardIdx.
    TArray2D<ui64> NextShard;

    void CalcScore(const TVector<TShardStats>& stats);
    void CalcNextShard();

public:
    TShardBalancerWeightedDeterministic(
        ui32 blockSize,
        ui64 precisionBytes,
        ui32 maxFileBlocks,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds);

    using IShardBalancer::Update;

    NProto::TError Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve,
        std::optional<ui64> minFreeSpaceReserve) final;

    NProto::TError SelectShard(ui64 fileSize, TString* shardId) final;

    TVector<TShardStats> MakeOrderedShardList() const final;
};

////////////////////////////////////////////////////////////////////////////////

IShardBalancerPtr CreateShardBalancer(
    NProto::EShardBalancerPolicy policy,
    ui32 blockSize,
    ui64 precisionBytes,
    ui32 maxFileBlocks,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve,
    TVector<TString> shardIds);

}   // namespace NCloud::NFileStore::NStorage
