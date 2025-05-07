#include "write_back_cache.h"

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteBackCache(
        IFileStorePtr session,
        const TString& filePath,
        ui32 size)
    : Session(std::move(session))
    , RequestsToProcess(filePath, size)
{}

TFuture<NProto::TReadDataResponse> TWriteBackCache::ReadData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadDataRequest> request)
{
    Y_UNUSED(callContext);
    Y_UNUSED(request);
    // TODO(svartmetal): implement me
    return {};
}

bool TWriteBackCache::AddWriteDataRequest(
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    Y_UNUSED(request);
    // TODO(svartmetal): implement me
    return {};
}

TFuture<void> TWriteBackCache::FlushData(ui64 handle)
{
    // TODO(svartmetal): implement me
    Y_UNUSED(handle);
    auto promise = NewPromise<void>();
    promise.SetValue();
    return promise.GetFuture();
}

TFuture<void> TWriteBackCache::FlushAllData()
{
    // TODO(svartmetal): implement me
    auto promise = NewPromise<void>();
    promise.SetValue();
    return promise.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

TWriteBackCachePtr CreateWriteBackCache(
    IFileStorePtr session,
    const TString& filePath,
    ui32 size)
{
    return std::make_unique<TWriteBackCache>(
        std::move(session),
        filePath,
        size);
}

}   // namespace NCloud::NFileStore::NFuse
