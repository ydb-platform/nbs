#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage::NSplitRequest {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
using TResponseRecordType = TMethod::TResponse::ProtoRecordType;

auto SplitReadRequest(
    const NProto::TReadBlocksRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TVector<NProto::TReadBlocksRequest>;

auto SplitReadRequest(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TVector<NProto::TReadBlocksLocalRequest>;

struct TSplitReadBlocksResponse
{
    NProto::TReadBlocksResponse Response;
    ui64 BlocksCountRequested = 0;
};

auto MergeReadResponses(
    std::span<TSplitReadBlocksResponse> responsesToMerge,
    size_t blockSize) -> NProto::TReadBlocksResponse;

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
