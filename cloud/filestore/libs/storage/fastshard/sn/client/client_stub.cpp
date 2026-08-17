#include "client.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateStorageNodeClient(TString host, ui16 port)
{
    Y_UNUSED(host);
    Y_UNUSED(port);
    return CreateStorageNodeStub();
}

IStorageNodePtr CreateStorageNodeClient(
    TString host,
    ui16 port,
    TStorageNodeClientMetricsPtr metrics)
{
    Y_UNUSED(metrics);
    return CreateStorageNodeClient(std::move(host), port);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
