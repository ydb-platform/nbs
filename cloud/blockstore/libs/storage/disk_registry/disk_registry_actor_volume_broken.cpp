#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateVolumeHealth(
    const TEvDiskRegistry::TEvUpdateVolumeHealthRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateVolumeHealth);

    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const auto health = msg->Record.GetVolumeHealth();
    if (health != NProto::VOLUME_HEALTH_HEALTHY) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s Volume reported disk broken: DiskId=%s, Health=%s",
            LogTitle.GetWithTime().c_str(),
            msg->Record.GetDiskId().Quote().c_str(),
            NProto::EVolumeHealth_Name(health).c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s Volume reported disk recovered: DiskId=%s, Health=%s",
            LogTitle.GetWithTime().c_str(),
            msg->Record.GetDiskId().Quote().c_str(),
            NProto::EVolumeHealth_Name(health).c_str());
    }

    ExecuteTx<TUpdateVolumeHealth>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        ctx.Now(),
        health);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdateVolumeHealth(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateVolumeHealth& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TDiskRegistryActor::ExecuteUpdateVolumeHealth(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateVolumeHealth& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    args.Error =
        State->UpdateVolumeHealth(db, args.DiskId, args.Now, args.VolumeHealth);
}

void TDiskRegistryActor::CompleteUpdateVolumeHealth(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateVolumeHealth& args)
{
    auto response =
        std::make_unique<TEvDiskRegistry::TEvUpdateVolumeHealthResponse>(
            std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    NotifyUsers(ctx);
    PublishDiskStates(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
