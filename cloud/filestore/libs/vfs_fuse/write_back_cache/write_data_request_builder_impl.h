#pragma once

#include "write_data_request_builder.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestBuilderConfig
{
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

class TWriteDataRequestBuilder: public IWriteDataRequestBuilder
{
private:
    const TWriteDataRequestBuilderConfig Config;

public:
    explicit TWriteDataRequestBuilder(
        const TWriteDataRequestBuilderConfig& config);

    TWriteDataBatch BuildWriteDataRequests(
        const TString& fileSystemId,
        ui64 nodeId,
        const TDataSupplier& supplier) const override;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
