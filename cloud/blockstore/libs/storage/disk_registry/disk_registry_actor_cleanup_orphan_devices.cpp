#include "disk_registry_actor.h"

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareCleanupOrphanDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupOrphanDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCleanupOrphanDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupOrphanDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    auto removedDevices = State->CleanupOrphanDevices(db);

    if (!removedDevices.empty()) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Found devices without agent and remove them, removed "
            "DeviceUUIDs=%s",
            JoinSeq(" ", removedDevices).c_str());
    }
}

void TDiskRegistryActor::CompleteCleanupOrphanDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TCleanupOrphanDevices& args)
{
    Y_UNUSED(args);
    Y_UNUSED(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
