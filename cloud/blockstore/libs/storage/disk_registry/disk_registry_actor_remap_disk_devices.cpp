#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/common/safe_debug_print.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleRemapDiskDevices(
    const TEvDiskRegistry::TEvRemapDiskDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Y_UNUSED(ev);
}

bool TDiskRegistryActor::PrepareRemapDiskDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemapDiskDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteRemapDiskDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRemapDiskDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TDiskRegistryActor::CompleteRemapDiskDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TRemapDiskDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

}   // namespace NCloud::NBlockStore::NStorage
