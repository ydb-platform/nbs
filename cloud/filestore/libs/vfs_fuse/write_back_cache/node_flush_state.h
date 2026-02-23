#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <util/generic/vector.h>

#include <atomic>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestWithResponse
{
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    NCloud::NProto::TError Error;

    explicit TWriteDataRequestWithResponse(
        std::shared_ptr<NProto::TWriteDataRequest> request)
        : Request(std::move(request))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeFlushState
{
    const ui64 NodeId;

    // WriteData requests generated during Flush that are executed in parallel
    TVector<TWriteDataRequestWithResponse> WriteDataRequests;

    // The amount of unflushed requests that will become flushed when all
    // requests from |WriteDataRequests| are completed
    size_t AffectedUnflushedRequestCount = 0;

    // The number of remaining executing requests from |WriteDataRequests|
    std::atomic<size_t> InFlightWriteDataRequestCount = 0;

    TNodeFlushState(
        ui64 nodeId,
        TVector<std::shared_ptr<NProto::TWriteDataRequest>> requests,
        size_t affectedUnflushedRequestCount);

    size_t CheckedDecrementInFlight();

    void EraseCompletedRequests();
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
