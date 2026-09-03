#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// Requests that are hanging due to WriteBackCache failed state
// The class is not thread-safe
class THangingRequests
{
private:
    TVector<NThreading::TPromise<TResultOrError<ui64>>> AcquireBarrierPromises;
    TVector<NThreading::TPromise<NProto::TError>> FlushOrReleaseHandlePromises;
    TVector<NThreading::TPromise<NProto::TReadDataResponse>> ReadDataPromises;
    TVector<NThreading::TPromise<NProto::TWriteDataResponse>> WriteDataPromises;

public:
    void Add(NThreading::TPromise<NProto::TReadDataResponse> promise);
    NThreading::TFuture<TResultOrError<ui64>> CreateAcquireBarrierResponse();
    NThreading::TFuture<NProto::TError> CreateFlushOrReleaseHandleResponse();
    NThreading::TFuture<NProto::TWriteDataResponse> CreateWriteDataResponse();
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
