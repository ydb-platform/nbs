#include "storage_node.h"

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateNaiveFileStorageNode(TString host, ui16 port)
{
    Y_UNUSED(host);
    Y_UNUSED(port);
    return CreateStorageNodeStub();
}

}   // namespace NCloud::NFastShard
