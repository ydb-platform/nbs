#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/public/api/protos/io.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <span>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

auto SplitReadRequest(
    const NProto::TReadBlocksRequest& originalRequest,
    std::span<const TBlockRange64> requestBlockRanges)
    -> TResultOrError<TVector<NProto::TReadBlocksRequest>>;

auto SplitReadRequest(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    std::span<const TBlockRange64> requestBlockRanges)
    -> TResultOrError<TVector<NProto::TReadBlocksLocalRequest>>;

auto MergeReadResponses(std::span<NProto::TReadBlocksResponse> responsesToMerge)
    -> NProto::TReadBlocksResponse;

auto MergeReadResponses(
    std::span<NProto::TReadBlocksLocalResponse> responsesToMerge)
    -> NProto::TReadBlocksLocalResponse;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
