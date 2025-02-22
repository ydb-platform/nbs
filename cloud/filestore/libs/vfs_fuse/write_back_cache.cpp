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
    // TODO(svartmetal): implement me
    return Session->ReadData(callContext, request);
}

bool TWriteBackCache::AddWriteDataRequest(
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    // TODO(svartmetal): get rid of double-copy
    TString result;
    Y_ABORT_UNLESS(request->SerializeToString(&result));
    return RequestsToProcess.Push(result);
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
