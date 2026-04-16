#pragma once

#include "public.h"

#include <cloud/filestore/config/storage.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TShardStats
{
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

        TShardMeta(ui32 shardIdx, TShardStats stats)
            : ShardIdx(shardIdx)
            , Stats(stats)
        {}
    };

    struct TShardDescr
    {
        TString ShardId;
        TShardStats Stats;

        TShardDescr(TString shardId, TShardStats stats)
            : ShardId(std::move(shardId))
            , Stats(stats)
        {}
    };

    virtual ~IShardBalancer() = default;

    virtual void Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve,
        std::optional<ui64> minFreeSpaceReserve) = 0;
    virtual NProto::TError SelectShard(ui64 fileSize, TString* shardId) = 0;

    /**
     * @brief Builds and returns the shard list ordered from best to worst.
     *
     * This method builds a new vector with shard descrs so it's not supposed to
     * be used often because it's expensive. The main intended use case is
     * introspection - log this info with debug loglevel or show it on monpages.
     *
     * @return The list of shard descrs.
     */
    [[nodiscard]] virtual TVector<TShardDescr> MakeOrderedShardList() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TShardBalancerBase: public IShardBalancer
{
public:
    TShardBalancerBase(
        ui32 blockSize,
        ui32 maxFileBlocks,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds);

private:
    const ui32 BlockSize = 4_KB;
    ui64 DesiredFreeSpaceReserve = 0;
    ui64 MinFreeSpaceReserve = 0;

protected:
    TVector<TString> Ids;
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
    void Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve = {},
        std::optional<ui64> minFreeSpaceReserve = {}) override;

    [[nodiscard]] TVector<TShardDescr> MakeOrderedShardList() const override;
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
        ui32 maxFileBlocks,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds);
    void Update(
        const TVector<TShardStats>& stats,
        std::optional<ui64> desiredFreeSpaceReserve = {},
        std::optional<ui64> minFreeSpaceReserve = {}) final;
    NProto::TError SelectShard(ui64 fileSize, TString* shardId) final;
};

/////////////////////////////////////////////////////////////////////////////////

IShardBalancerPtr CreateShardBalancer(
    NProto::EShardBalancerPolicy policy,
    ui32 blockSize,
    ui32 maxFileBlocks,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve,
    TVector<TString> shardIds);

}   // namespace NCloud::NFileStore::NStorage
