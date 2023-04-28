#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleUpdateMigrationState(
    const TEvVolume::TEvUpdateMigrationState::TPtr& ev,
    const TActorContext& ctx)
{
    if (UpdateVolumeConfigInProgress) {
        // skipping this index update
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolume::TEvMigrationStateUpdated>());

        return;
    }

    auto* msg = ev->Get();

    LWTRACK(
        RequestReceived_Volume,
        msg->CallContext->LWOrbit,
        "UpdateMigrationState",
        msg->CallContext->RequestId);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId));

    ExecuteTx<TUpdateMigrationState>(
        ctx,
        std::move(requestInfo),
        msg->MigrationIndex);
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareUpdateMigrationState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateMigrationState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateMigrationState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateMigrationState& args)
{
    Y_UNUSED(ctx);
    Y_VERIFY(State);

    State->UpdateMigrationIndexInMeta(args.MigrationIndex);
    TVolumeDatabase db(tx.DB);
    db.WriteMeta(State->GetMeta());
}

void TVolumeActor::CompleteUpdateMigrationState(
    const TActorContext& ctx,
    TTxVolume::TUpdateMigrationState& args)
{
    LWTRACK(
        ResponseSent_Volume,
        args.RequestInfo->CallContext->LWOrbit,
        "UpdateMigrationState",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvVolume::TEvMigrationStateUpdated>());
}

}   // namespace NCloud::NBlockStore::NStorage
