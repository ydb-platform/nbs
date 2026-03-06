#pragma once

#include "write_back_cache_state.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TReadResponseBuilder
{
private:
    const NProto::TReadDataRequest& Request;
    const TCachedData CachedData;
    ui64 ContiguousCachedDataByteCount = 0;

public:
    TReadResponseBuilder(
        const NProto::TReadDataRequest& request,
        const TWriteBackCacheState& state);

    // Check if there are cached data parts in the requested range
    // Used to calculate statistics (e.g. hit/miss ratio)
    bool HasCachedData() const;

    // Check if cached data parts fully cover the requested range
    // In this case, the response can be served entirely from cache
    // without requesting the backend
    bool CanFullyServeFromCache() const;
    NProto::TReadDataResponse FullyServeFromCache() const;

    // Apply cached data on top of the response returned from backend
    void AugmentResponseWithCachedData(
        NProto::TReadDataResponse& response) const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
