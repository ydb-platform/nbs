#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleVolumeDiskBroken(
    const TEvDiskRegistry::TEvVolumeDiskBrokenRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(VolumeDiskBroken);

    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Volume reported disk broken: DiskId=%s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.GetDiskId().Quote().c_str());

    ExecuteTx<TVolumeDiskBroken>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        ctx.Now());
}

void TDiskRegistryActor::HandleVolumeDiskRecovered(
    const TEvDiskRegistry::TEvVolumeDiskRecoveredRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(VolumeDiskRecovered);

    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Volume reported disk recovered: DiskId=%s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.GetDiskId().Quote().c_str());

    ExecuteTx<TVolumeDiskRecovered>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetDiskId(),
        ctx.Now());
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareVolumeDiskBroken(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TVolumeDiskBroken& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TDiskRegistryActor::ExecuteVolumeDiskBroken(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TVolumeDiskBroken& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->OnVolumeDiskBroken(db, args.DiskId, args.Now);
}

void TDiskRegistryActor::CompleteVolumeDiskBroken(
    const TActorContext& ctx,
    TTxDiskRegistry::TVolumeDiskBroken& args)
{
    auto response =
        std::make_unique<TEvDiskRegistry::TEvVolumeDiskBrokenResponse>(
            std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    NotifyUsers(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareVolumeDiskRecovered(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TVolumeDiskRecovered& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TDiskRegistryActor::ExecuteVolumeDiskRecovered(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TVolumeDiskRecovered& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->OnVolumeDiskRecovered(db, args.DiskId, args.Now);
}

void TDiskRegistryActor::CompleteVolumeDiskRecovered(
    const TActorContext& ctx,
    TTxDiskRegistry::TVolumeDiskRecovered& args)
{
    auto response =
        std::make_unique<TEvDiskRegistry::TEvVolumeDiskRecoveredResponse>(
            std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    NotifyUsers(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
