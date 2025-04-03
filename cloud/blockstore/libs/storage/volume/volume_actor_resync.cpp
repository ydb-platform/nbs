#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleUpdateResyncState(
    const TEvVolume::TEvUpdateResyncState::TPtr& ev,
    const TActorContext& ctx)
{
    if (UpdateVolumeConfigInProgress) {
        // skipping this index update
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolume::TEvResyncStateUpdated>());

        return;
    }

    auto* msg = ev->Get();

    LWTRACK(
        RequestReceived_Volume,
        msg->CallContext->LWOrbit,
        "UpdateResyncState",
        msg->CallContext->RequestId);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TUpdateResyncState>(
        ctx,
        std::move(requestInfo),
        msg->ResyncIndex);
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareUpdateResyncState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateResyncState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateResyncState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateResyncState& args)
{
    Y_UNUSED(ctx);
    Y_ABORT_UNLESS(State);

    State->UpdateResyncIndexInMeta(args.ResyncIndex);
    TVolumeDatabase db(tx.DB);
    db.WriteMeta(State->GetMeta());
    // MetaHistory update not needed here
}

void TVolumeActor::CompleteUpdateResyncState(
    const TActorContext& ctx,
    TTxVolume::TUpdateResyncState& args)
{
    LWTRACK(
        ResponseSent_Volume,
        args.RequestInfo->CallContext->LWOrbit,
        "UpdateResyncState",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvVolume::TEvResyncStateUpdated>());
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleResyncFinished(
    const TEvVolume::TEvResyncFinished::TPtr& ev,
    const TActorContext& ctx)
{
    if (UpdateVolumeConfigInProgress) {
        State->SetResyncNeededInMeta(
            false,   // resyncEnabled
            false    // alertResyncChecksumMismatch
        );

        return;
    }

    auto* msg = ev->Get();

    LWTRACK(
        RequestReceived_Volume,
        msg->CallContext->LWOrbit,
        "ResyncFinished",
        msg->CallContext->RequestId);

    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Resync finished",
        TabletID());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TToggleResync>(
        ctx,
        std::move(requestInfo),
        false,   // resyncEnabled
        false    // alertResyncChecksumMismatch
    );
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareToggleResync(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TToggleResync& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteToggleResync(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TToggleResync& args)
{
    Y_UNUSED(ctx);
    Y_ABORT_UNLESS(State);

    args.ResyncWasNeeded = State->IsMirrorResyncNeeded();

    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Toggling resync: %d -> %d",
        TabletID(),
        State->GetMeta().GetResyncNeeded(),
        args.ResyncEnabled);

    State->SetResyncNeededInMeta(
        args.ResyncEnabled,
        args.AlertResyncChecksumMismatch);
    TVolumeDatabase db(tx.DB);
    db.WriteMeta(State->GetMeta());
    // MetaHistory update not needed here
}

void TVolumeActor::CompleteToggleResync(
    const TActorContext& ctx,
    TTxVolume::TToggleResync& args)
{
    Y_UNUSED(args);

    if (args.ResyncWasNeeded != State->IsMirrorResyncNeeded()) {
        RestartPartition(ctx, {});
    } else {
        State->SetReadWriteError({});
    }
}

}   // namespace NCloud::NBlockStore::NStorage
