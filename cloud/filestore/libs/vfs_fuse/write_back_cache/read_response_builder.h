#pragma once

#include "write_back_cache_state.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <optional>

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

    // Attempt to build a complete TReadDataResponse using only data from cache.
    // If the entire requested byte range is available in the cache, returns
    // a populated TReadDataResponse or std::nullopt otherwise
    std::optional<NProto::TReadDataResponse> TryFullyServeFromCache() const;

    // Apply cached data on top of the response returned from backend
    void AugmentResponseWithCachedData(
        NProto::TReadDataResponse& response) const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
