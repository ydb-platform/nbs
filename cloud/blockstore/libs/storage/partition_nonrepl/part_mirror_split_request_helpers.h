#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

auto SplitReadRequest(
    const NProto::TReadBlocksRequest& originalRequest,
    std::span<const TBlockRange64> requestBlockRanges)
    -> TVector<NProto::TReadBlocksRequest>;

auto SplitReadRequest(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    std::span<const TBlockRange64> requestBlockRanges)
    -> TVector<NProto::TReadBlocksLocalRequest>;

auto MergeReadResponses(std::span<NProto::TReadBlocksResponse> responsesToMerge)
    -> NProto::TReadBlocksResponse;

}   // namespace NCloud::NBlockStore::NStorage
