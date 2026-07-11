#include "storage_node.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateStorageNodeClient(TString host, ui16 port)
{
    Y_UNUSED(host);
    Y_UNUSED(port);
    return CreateStorageNodeStub();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
