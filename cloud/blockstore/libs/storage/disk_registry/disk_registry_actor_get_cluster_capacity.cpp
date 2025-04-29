#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleGetClusterCapacity(
    const TEvDiskRegistry::TEvGetClusterCapacityRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(GetClusterCapacity);

    const auto* msg = ev->Get();
    const TString& diskId = msg->Record.GetDiskId();

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Received GetClusterCapacity request");


    auto response = std::make_unique<TEvDiskRegistry::TEvGetClusterCapacityResponse>();

    // response->Record.SetFolderId(diskInfo.FolderId);

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
