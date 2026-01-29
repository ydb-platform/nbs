#include "flusher.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TFlusher::TImpl
{
private:
    IWriteDataRequestBuilder& RequestBuilder;
    IFileStorePtr Session;

public:
    TImpl(IWriteDataRequestBuilder& requestBuilder, IFileStorePtr session)
        : RequestBuilder(requestBuilder)
        , Session(std::move(session))
    {
        Y_UNUSED(RequestBuilder);
    }

    void ShouldFlushNode(ui64 nodeId)
    {
        Y_UNUSED(nodeId);
    }
};

////////////////////////////////////////////////////////////////////////////////

TFlusher::TFlusher(
    IWriteDataRequestBuilder& requestBuilder,
    IFileStorePtr session)
    : Impl(std::make_shared<TImpl>(requestBuilder, std::move(session)))
{}

void TFlusher::ShouldFlushNode(ui64 nodeId)
{
    Impl->ShouldFlushNode(nodeId);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
