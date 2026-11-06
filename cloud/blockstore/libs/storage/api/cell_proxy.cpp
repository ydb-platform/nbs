#include "cell_proxy.h"

namespace NCloud::NBlockStore::NStorage {

NActors::TActorId MakeCellProxyServiceId()
{
    return NActors::TActorId(0, "blk-cellprx");
}

}   // namespace NCloud::NBlockStore::NStorage
