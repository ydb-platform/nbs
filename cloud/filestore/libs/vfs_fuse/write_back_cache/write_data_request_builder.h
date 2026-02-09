#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <util/generic/function_ref.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestBatch
{
    // Consolidated and non-overlapping WriteData requests to be executed in
    // parallel at Flush
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> Requests;

    // The number of initial WriteData requests used to construct this batch
    size_t AffectedRequestCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The class is thread safe
struct IWriteDataRequestBatchBuilder
{
    virtual ~IWriteDataRequestBatchBuilder() = default;

    // Add next WriteData request to the batch.
    // Returns true if the request was added.
    // Returns false if the request was rejected to due conditions in the
    // configuration (e.g. max request count exceeded)
    virtual bool AddRequest(ui64 handle, ui64 offset, TStringBuf data) = 0;

    virtual TWriteDataRequestBatch Build() = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The class is thread safe
struct IWriteDataRequestBuilder
{
    virtual ~IWriteDataRequestBuilder() = default;

    virtual std::unique_ptr<IWriteDataRequestBatchBuilder>
    CreateWriteDataRequestBatchBuilder(ui64 nodeId) = 0;
};

using IWriteDataRequestBuilderPtr = std::shared_ptr<IWriteDataRequestBuilder>;

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestBuilderConfig
{
    TString FileSystemId;

    // The maximum size of a single consolidated WriteData request
    ui32 MaxWriteRequestSize = 0;

    // The maximum number of consolidated WriteData requests
    ui32 MaxWriteRequestsCount = 0;

    // The maximum total size of all consolidated WriteData requests
    ui32 MaxSumWriteRequestsSize = 0;

    // Generate WriteData requests with iovecs
    bool ZeroCopyWriteEnabled = false;
};

////////////////////////////////////////////////////////////////////////////////

IWriteDataRequestBuilderPtr CreateWriteDataRequestBuilder(
    const TWriteDataRequestBuilderConfig& config);

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
