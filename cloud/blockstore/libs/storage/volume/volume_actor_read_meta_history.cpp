#include "volume_actor.h"

#include "volume_database.h"

// #include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleReadMetaHistory(
    const TEvVolumePrivate::TEvReadMetaHistoryRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    ProcessReadMetaHistory(ctx, std::move(requestInfo));
}

void TVolumeActor::ProcessReadMetaHistory(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo)
{
    AddTransaction(*requestInfo);

    ExecuteTx<TReadMetaHistory>(ctx, std::move(requestInfo));
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareReadMetaHistory(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TReadMetaHistory& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    return db.ReadMetaHistory(args.MetaHistory);
}

void TVolumeActor::ExecuteReadMetaHistory(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TReadMetaHistory& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TVolumeActor::CompleteReadMetaHistory(
    const TActorContext& ctx,
    TTxVolume::TReadMetaHistory& args)
{
    auto response =
        std::make_unique<TEvVolumePrivate::TEvReadMetaHistoryResponse>();
    response->MetaHistory = std::move(args.MetaHistory);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    RemoveTransaction(*args.RequestInfo);
}

}   // namespace NCloud::NBlockStore::NStorage
