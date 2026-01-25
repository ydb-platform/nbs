#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <util/generic/function_ref.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataBatch
{
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> Requests;
    size_t AffectedRequestCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The class is thread safe
struct IWriteDataRequestBuilder
{
    using TDataVisitor =
        TFunctionRef<bool(ui64 handle, ui64 offset, TStringBuf data)>;

    using TDataSupplier = TFunctionRef<void(const TDataVisitor& visitor)>;

    virtual ~IWriteDataRequestBuilder() = default;

    virtual TWriteDataBatch BuildWriteDataRequests(
        ui64 nodeId,
        const TDataSupplier& supplier) const = 0;
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
