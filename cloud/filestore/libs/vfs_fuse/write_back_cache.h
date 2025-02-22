#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache final
{
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl;

public:
    TWriteBackCache(
        IFileStorePtr session,
        const TString& filePath,
        ui32 capacity);

    ~TWriteBackCache();

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<void> FlushData(ui64 handle);

    NThreading::TFuture<void> FlushAllData();
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCachePtr CreateWriteBackCache(
    IFileStorePtr session,
    const TString& filePath,
    ui32 capacity);

}   // namespace NCloud::NFileStore::NFuse
