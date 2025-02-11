#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage::NSplitRequest {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
using TRequestRecordType = TMethod::TRequest::ProtoRecordType;

template <typename TMethod>
using TResponseRecordType = TMethod::TResponse::ProtoRecordType;

template <typename TMethod>
struct TRequestToBlockRange
{
    TRequestRecordType<TMethod> Request;
    TBlockRange64 BlockRangeForRequest;

    TRequestToBlockRange(
            TRequestRecordType<TMethod> request,
            TBlockRange64 blockRangeForRequest)
        : Request(std::move(request))
        , BlockRangeForRequest(blockRangeForRequest)
    {}
};

template <typename TMethod>
using TSplitRequest = TVector<TRequestToBlockRange<TMethod>>;

auto SplitReadRequest(
    const NProto::TReadBlocksRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TSplitRequest<TEvService::TReadBlocksMethod>;

auto SplitReadRequest(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TSplitRequest<TEvService::TReadBlocksLocalMethod>;

template <typename TMethod>
struct TMergeResponsesContext
{
    TResponseRecordType<TMethod> Response;
    size_t BlocksCountRequested;
};

auto MergeReadResponses(
    std::span<TMergeResponsesContext<TEvService::TReadBlocksMethod>>
        responsesToUnify,
    size_t blockSize) -> NProto::TReadBlocksResponse;

auto MergeReadResponses(
    std::span<TMergeResponsesContext<TEvService::TReadBlocksLocalMethod>>
        responsesToUnify,
    size_t blockSize) -> NProto::TReadBlocksResponse;

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
