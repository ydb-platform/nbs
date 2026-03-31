#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleVolumeBroken(
    const TEvDiskRegistry::TEvVolumeBrokenRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(VolumeBroken);

    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Volume reported disk broken: DiskId=%s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.GetDiskId().Quote().c_str());

    ExecuteTx<TUpdateVolumeStateBroken>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        ctx.Now(),
        true);
}

void TDiskRegistryActor::HandleVolumeRecovered(
    const TEvDiskRegistry::TEvVolumeRecoveredRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(VolumeRecovered);

    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Volume reported disk recovered: DiskId=%s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.GetDiskId().Quote().c_str());

    ExecuteTx<TUpdateVolumeStateBroken>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        ctx.Now(),
        false);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdateVolumeStateBroken(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateVolumeStateBroken& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TDiskRegistryActor::ExecuteUpdateVolumeStateBroken(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateVolumeStateBroken& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->UpdateVolumeStateBroken(db, args.DiskId, args.Now, args.Broken);
}

void TDiskRegistryActor::CompleteUpdateVolumeStateBroken(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateVolumeStateBroken& args)
{
    if (args.Broken) {
        auto response =
            std::make_unique<TEvDiskRegistry::TEvVolumeBrokenResponse>(
                std::move(args.Error));
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    } else {
        auto response =
            std::make_unique<TEvDiskRegistry::TEvVolumeRecoveredResponse>(
                std::move(args.Error));
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }

    NotifyUsers(ctx);
    PublishDiskStates(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
