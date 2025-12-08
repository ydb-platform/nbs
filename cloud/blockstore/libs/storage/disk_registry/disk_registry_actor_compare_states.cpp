#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCompareDiskRegistryState(
    const TEvDiskRegistry::TEvCompareDiskRegistryStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CompareDiskRegistryState);

    LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Received CompareDiskRegistryState request");

    auto response =
        std::make_unique<TEvDiskRegistry::TEvCompareDiskRegistryStateResponse>();
    //TODO States Comparsion
}

}