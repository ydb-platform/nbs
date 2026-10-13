#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/common/safe_debug_print.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleRestoreDiskFromBackup(
    const TEvDiskRegistry::TEvRestoreDiskFromBackupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Y_UNUSED(ev);
}

bool TDiskRegistryActor::PrepareRestoreDiskFromBackup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDiskFromBackup& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteRestoreDiskFromBackup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TRestoreDiskFromBackup& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TDiskRegistryActor::CompleteRestoreDiskFromBackup(
    const TActorContext& ctx,
    TTxDiskRegistry::TRestoreDiskFromBackup& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

}   // namespace NCloud::NBlockStore::NStorage
