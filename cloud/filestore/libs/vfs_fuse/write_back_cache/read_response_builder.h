#pragma once

#include "write_back_cache_state.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TReadResponseBuilder
{
private:
    ui64 NodeId = 0;
    ui64 Offset = 0;
    ui64 Length = 0;
    TVector<TArrayRef<char>> Iovecs;

public:
    explicit TReadResponseBuilder(const NProto::TReadDataRequest& request);

    ui64 GetNodeId() const
    {
        return NodeId;
    }

    // Attempt to build a complete TReadDataResponse using only data from cache.
    // If the entire requested byte range is available in the cache, returns
    // a populated TReadDataResponse or std::nullopt otherwise
    std::optional<NProto::TReadDataResponse> TryFullyServeFromCache(
        TWriteBackCacheState& state) const;

    // Apply cached data on top of the response returned from backend.
    // Returns true if the response was augmented with cached data.
    // Returns false if no cached data was applied to the response.
    bool AugmentResponseWithCachedData(
        NProto::TReadDataResponse& response,
        TWriteBackCacheState& state) const;

private:
    void AugmentResponseWithCachedData(
        NProto::TReadDataResponse& response,
        const TCachedData& cachedData) const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
