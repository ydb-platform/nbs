#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdatePlacementGroupSettings(
    const TEvDiskRegistry::TEvUpdatePlacementGroupSettingsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdatePlacementGroupSettings);

    auto* msg = ev->Get();
    auto& record = msg->Record;

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received UpdatePlacementGroupSettings request."
        " GroupId=%s, ConfigVersion=%u, Settings=%s",
        TabletID(),
        record.GetGroupId().c_str(),
        record.GetConfigVersion(),
        record.GetSettings().DebugString().Quote().c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId));

    BLOCKSTORE_TRACE_RECEIVED(ctx, &requestInfo->TraceId, this, msg);

    ExecuteTx<TUpdatePlacementGroupSettings>(
        ctx,
        std::move(requestInfo),
        std::move(*record.MutableGroupId()),
        record.GetConfigVersion(),
        std::move(*record.MutableSettings()));
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdatePlacementGroupSettings(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdatePlacementGroupSettings& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdatePlacementGroupSettings(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdatePlacementGroupSettings& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->UpdatePlacementGroupSettings(
        db,
        args.GroupId,
        args.ConfigVersion,
        args.Settings);
}

void TDiskRegistryActor::CompleteUpdatePlacementGroupSettings(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdatePlacementGroupSettings& args)
{
    using TResponse = TEvDiskRegistry::TEvUpdatePlacementGroupSettingsResponse;

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TResponse>(args.Error));
}

}   // namespace NCloud::NBlockStore::NStorage
