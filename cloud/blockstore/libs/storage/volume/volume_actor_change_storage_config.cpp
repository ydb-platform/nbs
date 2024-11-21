#include "volume_actor.h"

#include "volume_database.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareChangeStorageConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TChangeStorageConfig& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    if (args.MergeWithStorageConfigFromVolumeDB) {
        return db.ReadStorageConfig(args.StorageConfigFromDB);
    }
    return true;
}

void TVolumeActor::ExecuteChangeStorageConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TChangeStorageConfig& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    if (args.StorageConfigFromDB.Defined()) {
        args.ResultStorageConfig = std::move(*args.StorageConfigFromDB);
    }
    args.ResultStorageConfig.MergeFrom(args.StorageConfigNew);
    db.WriteStorageConfig(args.ResultStorageConfig);
}

void TVolumeActor::CompleteChangeStorageConfig(
    const TActorContext& ctx,
    TTxVolume::TChangeStorageConfig& args)
{
    auto response =
        std::make_unique<TEvVolume::TEvChangeStorageConfigResponse>();
    *response->Record.MutableStorageConfig() = args.ResultStorageConfig;

    LWTRACK(
        ResponseSent_Volume,
        args.RequestInfo->CallContext->LWOrbit,
        "ChangeStorageConfig",
        args.RequestInfo->CallContext->RequestId);

    Config = std::make_shared<TStorageConfig>(*GlobalStorageConfig);
    Config->Merge(std::move(args.ResultStorageConfig));
    HasStorageConfigPatch = !Config->Equals(*GlobalStorageConfig);

    if (State->GetPartitionsState() == TPartitionInfo::READY ||
        State->GetPartitionsState() == TPartitionInfo::STARTED)
    {
        StopPartitions(ctx, {});
        StartPartitionsForUse(ctx);
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleChangeStorageConfig(
    const TEvVolume::TEvChangeStorageConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    const auto* msg = ev->Get();
    ExecuteTx(ctx, CreateTx<TChangeStorageConfig>(
        requestInfo,
        msg->Record.GetStorageConfig(),
        msg->Record.GetMergeWithStorageConfigFromVolumeDB()));
}

}   // namespace NCloud::NBlockStore::NStorage
