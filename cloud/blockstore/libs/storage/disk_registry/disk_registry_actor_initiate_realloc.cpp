#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleInitiateDiskReallocation(
    const TEvDiskRegistryPrivate::TEvInitiateDiskReallocationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Initiate reallocation: volume %s",
        TabletID(),
        msg->DiskId.Quote().c_str());

    ExecuteTx<TAddNotifiedDisks>(
        ctx,
        CreateRequestInfo<
            TEvDiskRegistryPrivate::TInitiateDiskReallocationMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext),
        std::move(msg->DiskId));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareAddNotifiedDisks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddNotifiedDisks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteAddNotifiedDisks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAddNotifiedDisks& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    for (const auto& diskId: args.DiskIds) {
        State->AddReallocateRequest(db, diskId);
    }
}

void TDiskRegistryActor::CompleteAddNotifiedDisks(
    const TActorContext& ctx,
    TTxDiskRegistry::TAddNotifiedDisks& args)
{
    auto response = std::make_unique<
        TEvDiskRegistryPrivate::TEvInitiateDiskReallocationResponse>();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
