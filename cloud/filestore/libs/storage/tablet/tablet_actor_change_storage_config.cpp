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
    TIndexTabletDatabase db(tx.DB);
    if (args.MergeWithStorageConfigFromTabletDB) {
        return db.ReadStorageConfig(args.StorageConfigFromDB);
    }
    return true;
}

void TIndexTabletActor::ExecuteTx_ChangeStorageConfig(
    const TActorContext& /* ctx */,
    TTransactionContext& tx,
    TTxIndexTablet::TChangeStorageConfig& args)
{
    TIndexTabletDatabase db(tx.DB);
    if (args.StorageConfigFromDB.Defined()) {
        auto* config = args.StorageConfigFromDB.Get();
        config->MergeFrom(args.StorageConfigNew);
        db.WriteStorageConfig(*config);
        args.ResultStorageConfig = std::move(*config);
        return;
    }
    args.ResultStorageConfig = args.StorageConfigNew;
    db.WriteStorageConfig(args.StorageConfigNew);
}

void TIndexTabletActor::CompleteTx_ChangeStorageConfig(
    const TActorContext& ctx,
    TTxIndexTablet::TChangeStorageConfig& args)
{
    RemoveTransaction(*args.RequestInfo);

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

    AddTransaction<TEvIndexTablet::TChangeStorageConfigMethod>(*requestInfo);

    const auto* msg = ev->Get();
    ExecuteTx<TChangeStorageConfig>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetStorageConfig(),
        msg->Record.GetMergeWithStorageConfigFromTabletDB());
}

}   // namespace NCloud::NFileStore::NStorage
