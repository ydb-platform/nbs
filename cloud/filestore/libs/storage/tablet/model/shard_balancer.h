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
    ui64 CurrentLoad = 0;
    ui64 Suffer = 0;
};

////////////////////////////////////////////////////////////////////////////////

class IShardBalancer
{
public:
    virtual ~IShardBalancer() = default;

    virtual void UpdateShardStats(const TVector<TShardStats>& stats) = 0;
    virtual NProto::TError SelectShard(ui64 fileSize, TString* shardId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TShardBalancerBase: public IShardBalancer
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

    TShardBalancerBase(
        ui32 blockSize,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds);

private:
    ui32 BlockSize = 4_KB;
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
     * @return The number of shards that can fit the file size, or
     * `std::nullopt` if no shard can fit the file size.
     */
    [[nodiscard]] std::optional<size_t> FindUpperBoundAmongAllShardsToFitFile(
        ui64 fileSize) const;

public:
    void UpdateShardStats(const TVector<TShardStats>& stats) override;
};

/////////////////////////////////////////////////////////////////////////////////

class TShardBalancerRoundRobin: public TShardBalancerBase
{
private:
    ui32 ShardSelector = 0;

public:
    using TShardBalancerBase::TShardBalancerBase;
    void UpdateShardStats(const TVector<TShardStats>& stats) final;
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
    TVector<ui64> PrefixSums;

    void UpdatePrefixSums();

public:
    TShardBalancerWeightedRandom(
        ui32 blockSize,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve,
        TVector<TString> shardIds);
    void UpdateShardStats(const TVector<TShardStats>& stats) final;
    NProto::TError SelectShard(ui64 fileSize, TString* shardId) final;
};

/////////////////////////////////////////////////////////////////////////////////

IShardBalancerPtr CreateShardBalancer(
    NProto::EShardBalancerPolicy policy,
    ui32 blockSize,
    ui64 desiredFreeSpaceReserve,
    ui64 minFreeSpaceReserve,
    TVector<TString> shardIds);

}   // namespace NCloud::NFileStore::NStorage
