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

    ExecuteTx<TVolumeBroken>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        ctx.Now());
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

    ExecuteTx<TVolumeRecovered>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        ctx.Now());
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareVolumeBroken(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TVolumeBroken& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TDiskRegistryActor::ExecuteVolumeBroken(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TVolumeBroken& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->OnVolumeBroken(db, args.DiskId, args.Now);
}

void TDiskRegistryActor::CompleteVolumeBroken(
    const TActorContext& ctx,
    TTxDiskRegistry::TVolumeBroken& args)
{
    auto response = std::make_unique<TEvDiskRegistry::TEvVolumeBrokenResponse>(
        std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    NotifyUsers(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareVolumeRecovered(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TVolumeRecovered& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TDiskRegistryActor::ExecuteVolumeRecovered(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TVolumeRecovered& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->OnVolumeRecovered(db, args.DiskId, args.Now);
}

void TDiskRegistryActor::CompleteVolumeRecovered(
    const TActorContext& ctx,
    TTxDiskRegistry::TVolumeRecovered& args)
{
    auto response =
        std::make_unique<TEvDiskRegistry::TEvVolumeRecoveredResponse>(
            std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    NotifyUsers(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
