#include "tablet_actor.h"
#include "tablet_database.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_ChangeStorageConfig(
    const TActorContext& /* ctx */,
    TTransactionContext& tx,
    TTxIndexTablet::TChangeStorageConfig& args)
{
    auto db = CreateIndexTabletDatabase(tx.DB);
    if (args.MergeWithStorageConfigFromTabletDB) {
        return db->ReadStorageConfig(args.StorageConfigFromDB);
    }
    return true;
}

void TIndexTabletActor::ExecuteTx_ChangeStorageConfig(
    const TActorContext& /* ctx */,
    TTransactionContext& tx,
    TTxIndexTablet::TChangeStorageConfig& args)
{
    auto db = CreateIndexTabletDatabase(tx.DB);
    if (args.StorageConfigFromDB.Defined()) {
        auto* config = args.StorageConfigFromDB.Get();
        config->MergeFrom(args.StorageConfigNew);
        db->WriteStorageConfig(*config);
        args.ResultStorageConfig = std::move(*config);
        return;
    }
    args.ResultStorageConfig = args.StorageConfigNew;
    db->WriteStorageConfig(args.StorageConfigNew);
}

void TIndexTabletActor::CompleteTx_ChangeStorageConfig(
    const TActorContext& ctx,
    TTxIndexTablet::TChangeStorageConfig& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    auto response =
        std::make_unique<TEvIndexTablet::TEvChangeStorageConfigResponse>();
    *response->Record.MutableStorageConfig() =
        args.ResultStorageConfig;

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleChangeStorageConfig(
    const TEvIndexTablet::TEvChangeStorageConfigRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());
    requestInfo->StartedTs = ctx.Now();

    const auto* msg = ev->Get();

    // A broken tablet rejects ChangeStorageConfig, so a bad value could not
    // be fixed with it afterwards.
    const ui32 hasherTypeOverride =
        msg->Record.GetStorageConfig().GetRangeIdHasherTypeOverride();
    if (hasherTypeOverride && !CreateRangeIdHasher(hasherTypeOverride)) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvChangeStorageConfigResponse>(
                MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "unsupported RangeIdHasherTypeOverride "
                        << hasherTypeOverride));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    AddInFlightRequest<TEvIndexTablet::TChangeStorageConfigMethod>(
        *requestInfo);

    ExecuteTx<TChangeStorageConfig>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetStorageConfig(),
        msg->Record.GetMergeWithStorageConfigFromTabletDB());
}

}   // namespace NCloud::NFileStore::NStorage
