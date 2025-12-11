#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleMarkDiskForCleanup(
    const TEvDiskRegistry::TEvMarkDiskForCleanupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto msg = ev->Get();
    auto diskId = msg->Record.GetDiskId();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "mark disk %s for cleanup",
        diskId.Quote().data());

    ExecuteTx<TMarkDiskForCleanup>(
        ctx,
        std::move(requestInfo),
        std::move(diskId));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareMarkDiskForCleanup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TMarkDiskForCleanup& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteMarkDiskForCleanup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TMarkDiskForCleanup& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->MarkDiskForCleanup(db, args.DiskId);
}

void TDiskRegistryActor::CompleteMarkDiskForCleanup(
    const TActorContext& ctx,
    TTxDiskRegistry::TMarkDiskForCleanup& args)
{
    auto reply =
        std::make_unique<TEvDiskRegistry::TEvMarkDiskForCleanupResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(reply));
}

}   // namespace NCloud::NBlockStore::NStorage
