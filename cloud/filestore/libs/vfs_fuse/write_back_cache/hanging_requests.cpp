#include "hanging_requests.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

void THangingRequests::Add(
    NThreading::TPromise<NProto::TReadDataResponse> promise)
{
    ReadDataPromises.push_back(std::move(promise));
}

NThreading::TFuture<TResultOrError<ui64>>
THangingRequests::CreateAcquireBarrierResponse()
{
    auto promise = NThreading::NewPromise<TResultOrError<ui64>>();
    auto future = promise.GetFuture();
    AcquireBarrierPromises.push_back(std::move(promise));
    return future;
}

NThreading::TFuture<NProto::TError>
THangingRequests::CreateFlushOrReleaseHandleResponse()
{
    auto promise = NThreading::NewPromise<NProto::TError>();
    auto future = promise.GetFuture();
    FlushOrReleaseHandlePromises.push_back(std::move(promise));
    return future;
}

NThreading::TFuture<NProto::TWriteDataResponse>
THangingRequests::CreateWriteDataResponse()
{
    auto promise = NThreading::NewPromise<NProto::TWriteDataResponse>();
    auto future = promise.GetFuture();
    WriteDataPromises.push_back(std::move(promise));
    return future;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
