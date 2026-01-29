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

struct IWriteDataRequestBuilder
{
    using TCachedDataVisitor = TFunctionRef<bool(ui64 offset, TStringBuf data)>;

    using TCachedDataSupplier =
        TFunctionRef<void(const TCachedDataVisitor& visitor)>;

    virtual ~IWriteDataRequestBuilder() = default;

    virtual TWriteDataBatch BuildWriteDataRequests(
        const TString& fileSystemId,
        ui64 nodeId,
        ui64 handle,
        const TCachedDataSupplier& supplier) const = 0;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
