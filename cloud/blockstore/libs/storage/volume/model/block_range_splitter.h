#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

class TBlockRangeSplitter
{
    ui64 BlockSize = 0;
    ui64 BlocksPerStripe = 0;
    TVector<ui64> BlockIndices;

public:
    TBlockRangeSplitter() = default;
    ~TBlockRangeSplitter() = default;

    void Reset(const NProto::TVolumeMeta& meta);

    [[nodiscard]] size_t CalculateRequestCount(
        TBlockRange64 blockRange,
        TVector<TBlockRange64>* splittedRanges) const;
};

}   // namespace NCloud::NBlockStore::NStorage
