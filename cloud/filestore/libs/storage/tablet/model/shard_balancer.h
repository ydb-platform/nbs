#pragma once

#include "public.h"

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

class TShardBalancer
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

private:
    ui32 BlockSize = 4_KB;
    ui64 DesiredFreeSpaceReserve = 0;
    ui64 MinFreeSpaceReserve = 0;

    TVector<TString> Ids;
    TVector<TShardMeta> Metas;
    ui32 ShardSelector = 0;

public:
    void SetParameters(
        ui32 blockSize,
        ui64 desiredFreeSpaceReserve,
        ui64 minFreeSpaceReserve);
    void UpdateShards(TVector<TString> shardIds);
    void UpdateShardStats(const TVector<TShardStats>& stats);
    NProto::TError SelectShard(ui64 fileSize, TString* shardId);
};

}   // namespace NCloud::NFileStore::NStorage
