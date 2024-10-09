#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleReadHistory(
    const TEvVolumePrivate::TEvReadHistoryRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ProcessReadHistory(
        ctx,
        std::move(requestInfo),
        THistoryLogKey(msg->StartTs),
        msg->EndTs.value_or(ctx.Now() - Config->GetVolumeHistoryDuration()),
        msg->RecordCount,
        false);
}

void TVolumeActor::ProcessReadHistory(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    THistoryLogKey startTs,
    TInstant endTs,
    size_t recordCount,
    bool monRequest)
{
    AddTransaction(*requestInfo);

    ExecuteTx<TReadHistory>(
        ctx,
        std::move(requestInfo),
        startTs,
        endTs,
        recordCount,
        monRequest);
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareReadHistory(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TReadHistory& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    TVolumeDatabase db(tx.DB);

    return db.ReadHistory(
        args.Ts,
        args.OldestTs,
        args.RecordCount,
        args.History);
}

void TVolumeActor::ExecuteReadHistory(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TReadHistory& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TVolumeActor::CompleteReadHistory(
    const TActorContext& ctx,
    TTxVolume::TReadHistory& args)
{
    if (args.MonRequest) {
        HandleHttpInfo_Default(
            ctx,
            args.History,
            State ? State->GetMetaHistory() : TVector<TVolumeMetaHistoryItem>{},
            "History",
            {},
            args.RequestInfo);
    } else {
        auto response = std::make_unique<TEvVolumePrivate::TEvReadHistoryResponse>();
        response->History = std::move(args.History.Items);

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }

    RemoveTransaction(*args.RequestInfo);
}

}   // namespace NCloud::NBlockStore::NStorage
