#include "async_client.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<IAsyncEndpointPtr> TAsyncClient::Connect(
    const TString& host,
    ui16 port)
{
    Y_UNUSED(host, port);
    return NThreading::MakeFuture<IAsyncEndpointPtr>(nullptr);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
