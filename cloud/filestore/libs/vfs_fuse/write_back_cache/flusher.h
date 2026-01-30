#pragma once

#include "write_back_cache_state.h"
#include "write_back_cache_state_listener.h"
#include "write_data_request_builder.h"

#include <cloud/filestore/libs/service/public.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TFlusher: public IWriteBackCacheStateListener
{
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl;

public:
    TFlusher(
        TWriteBackCacheState& state,
        IWriteDataRequestBuilder& requestBuilder,
        IFileStorePtr session,
        const TString& fileSystemId);

    void ShouldFlushNode(ui64 nodeId) override;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
