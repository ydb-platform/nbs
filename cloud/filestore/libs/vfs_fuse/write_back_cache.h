#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache
{
private:
    const IFileStorePtr Session;

    TFileRingBuffer RequestsToProcess;

public:
    TWriteBackCache(
        IFileStorePtr session,
        const TString& filePath,
        ui32 size);

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    bool AddWriteDataRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<void> FlushData(ui64 handle);
    NThreading::TFuture<void> FlushAllData();
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCachePtr CreateWriteBackCache(
    IFileStorePtr session,
    const TString& filePath,
    ui32 size);

}   // namespace NCloud::NFileStore::NFuse
