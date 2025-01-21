#pragma once
#include "public.h"

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
struct TRequestToPartitions
{
    TRequestRecordType<TMethod> Request;
    TVector<NActors::TActorId> Partitions;
    TBlockRange64 BlockRangeForRequest;

    TRequestToPartitions(
            TRequestRecordType<TMethod> request,
            TVector<NActors::TActorId> partitions,
            TBlockRange64 blockRangeForRequest)
        : Request(std::move(request))
        , Partitions(std::move(partitions))
        , BlockRangeForRequest(blockRangeForRequest)
    {}
};

template <typename TMethod>
using TSplittedRequest = TVector<TRequestToPartitions<TMethod>>;

TSplittedRequest<TEvService::TReadBlocksMethod> SplitRequestRead(
    const NProto::TReadBlocksRequest& originalRequest,
    TArrayRef<const TBlockRange64> blockRangeSplittedByDeviceBorders,
    TArrayRef<TVector<NActors::TActorId>> partitionsPerDevice);

std::optional<TSplittedRequest<TEvService::TReadBlocksLocalMethod>>
SplitRequestReadLocal(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    TArrayRef<const TBlockRange64> blockRangeSplittedByDeviceBorders,
    TArrayRef<TVector<NActors::TActorId>> partitionsPerDevice);

template <typename TMethod>
std::optional<TSplittedRequest<TMethod>> SplitRequest(
    const TRequestRecordType<TMethod>& originalRequest,
    TArrayRef<const TBlockRange64> blockRangeSplittedByDeviceBorders,
    TArrayRef<TVector<NActors::TActorId>> partitionsPerDevice)
{
    if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksMethod>) {
        return SplitRequestRead(
            originalRequest,
            blockRangeSplittedByDeviceBorders,
            std::move(partitionsPerDevice));
    } else if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>)
    {
        return SplitRequestReadLocal(
            originalRequest,
            blockRangeSplittedByDeviceBorders,
            std::move(partitionsPerDevice));
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
    TArrayRef<const TUnifyResponsesContext<TEvService::TReadBlocksMethod>>
        responsesToUnify,
    bool fillZeroResponses,
    size_t blockSize);

template <typename TMethod>
TResponseRecordType<TMethod> UnifyResponses(
    TArrayRef<const TUnifyResponsesContext<TMethod>> responsesToUnify,
    size_t blockSize)
{
    if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksMethod>) {
        return UnifyResponsesRead(
            responsesToUnify,
            true,   // fillZeroResponses
            blockSize);
    } else if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksMethod>)
    {
        return UnifyResponsesRead(
            responsesToUnify,
            false,   // fillZeroResponses
            blockSize);
    } else {
        return {};
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
