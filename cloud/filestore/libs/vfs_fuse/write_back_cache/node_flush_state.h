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

// The action that the caller should take after calling
// TNodeFlushState::OnWriteDataRequestCompleted
enum class EWriteDataRequestCompletedAction
{
    // The caller should continue executing other requests
    ContinueExecution,

    // The caller was the last executing thread and should collect flush result
    CollectFlushResult
};

////////////////////////////////////////////////////////////////////////////////

class TNodeFlushState
{
private:
    const ui64 NodeId;

    // WriteData requests generated during Flush that are executed in parallel
    TVector<TWriteDataRequestWithResponse> WriteDataRequests;

    // The amount of unflushed requests that will become flushed when all
    // requests from |WriteDataRequests| are completed
    ui64 AffectedUnflushedRequestCount = 0;

    // The number of remaining executing requests from |WriteDataRequests|
    std::atomic<ui64> InFlightWriteDataRequestCount = 0;

public:
    TNodeFlushState(
        ui64 nodeId,
        TVector<std::shared_ptr<NProto::TWriteDataRequest>> requests,
        ui64 affectedUnflushedRequestCount);

    ui64 GetNodeId() const
    {
        return NodeId;
    }

    ui64 GetAffectedUnflushedRequestCount() const
    {
        return AffectedUnflushedRequestCount;
    }

    size_t GetWriteDataRequestCount() const
    {
        return WriteDataRequests.size();
    }

    // The caller should execute the requests in parallel and call
    // OnWriteDataRequestCompleted for each response with the index
    // corresponding to the request in the returned vector
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> BeginFlush();

    // The method is thread-safe
    EWriteDataRequestCompletedAction OnWriteDataRequestCompleted(
        size_t index,
        const NProto::TWriteDataResponse& response);

    // When error is returned, the caller should retry flush
    NCloud::NProto::TError CollectFlushResult();
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
