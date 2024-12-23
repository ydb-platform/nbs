#include "disk_registry_actor.h"
#include "util/string/join.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareCleanupDevicesWithoutAgent(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupDevicesWithoutAgent& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCleanupDevicesWithoutAgent(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupDevicesWithoutAgent& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);
    auto removedDevices = State->CleanupDevicesWithoutAgent(db);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Found devices without agent and remove them, removed DeviceUUIDs=%s",
        JoinSeq(" ", removedDevices).c_str());
}

void TDiskRegistryActor::CompleteCleanupDevicesWithoutAgent(
    const TActorContext& ctx,
    TTxDiskRegistry::TCleanupDevicesWithoutAgent& args)
{
    Y_UNUSED(args);
    Y_UNUSED(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
