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

    bool HasCachedData() const;
    bool CanFullyServeFromCache() const;
    NProto::TReadDataResponse FullyServeFromCache() const;
    void AugmentResponseWithCachedData(
        NProto::TReadDataResponse& response) const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
