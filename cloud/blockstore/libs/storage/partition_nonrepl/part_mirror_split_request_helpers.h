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
using TSplittedRequest = TVector<TRequestToBlockRange<TMethod>>;

TSplittedRequest<TEvService::TReadBlocksMethod> SplitRequestRead(
    const NProto::TReadBlocksRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders);

std::optional<TSplittedRequest<TEvService::TReadBlocksLocalMethod>>
SplitRequestReadLocal(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders);

template <typename TMethod>
std::optional<TSplittedRequest<TMethod>> SplitRequest(
    const TRequestRecordType<TMethod>& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
{
    if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksMethod>) {
        return SplitRequestRead(
            originalRequest,
            blockRangeSplittedByDeviceBorders);
    } else if constexpr (
        std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>)
    {
        return SplitRequestReadLocal(
            originalRequest,
            blockRangeSplittedByDeviceBorders);
    } else {
        return {};
    }
}

template <typename TMethod>
struct TUnifyResponsesContext
{
    TResponseRecordType<TMethod> Response;
    size_t BlocksCountRequested;
};

NProto::TReadBlocksResponse UnifyResponsesRead(
    std::span<const TUnifyResponsesContext<TEvService::TReadBlocksMethod>>
        responsesToUnify,
    size_t blockSize);

NProto::TReadBlocksResponse UnifyResponsesReadLocal(
    std::span<const TUnifyResponsesContext<TEvService::TReadBlocksLocalMethod>>
        responsesToUnify,
    size_t blockSize);

template <typename TMethod>
TResponseRecordType<TMethod> UnifyResponses(
    std::span<const TUnifyResponsesContext<TMethod>> responsesToUnify,
    size_t blockSize)
{
    if constexpr (
        std::is_same_v<TMethod, TEvService::TReadBlocksMethod>)
    {
        return UnifyResponsesRead(responsesToUnify, blockSize);
    } else if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>) {
        return UnifyResponsesReadLocal(responsesToUnify, blockSize);
    }
    else {
        return {};
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
