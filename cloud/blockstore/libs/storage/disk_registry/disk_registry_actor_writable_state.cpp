#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleSetWritableState(
    const TEvDiskRegistry::TEvSetWritableStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(SetWritableState);

    const auto* msg = ev->Get();
    const bool writableState = msg->Record.GetState();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received SetWritableState request: State=%s",
        TabletID(),
        writableState ? "true" : "false");

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    if (CurrentState != STATE_WORK &&
        CurrentState != STATE_READ_ONLY)
    {
        const TString errorMsg = TStringBuilder()
            << "Can't change state in "
            << States[CurrentState].Name
            << " state";
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "SetWritableState error: %s",
            errorMsg.c_str());
        auto response =
            std::make_unique<TEvDiskRegistry::TEvSetWritableStateResponse>(
                MakeError(E_REJECTED, errorMsg));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    BecomeAux(ctx, writableState ? STATE_WORK : STATE_READ_ONLY);

    ExecuteTx<TWritableState>(
        ctx,
        std::move(requestInfo),
        writableState);
}

bool TDiskRegistryActor::PrepareWritableState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TWritableState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteWritableState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TWritableState& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    db.WriteWritableState(args.State);
}

void TDiskRegistryActor::CompleteWritableState(
    const TActorContext& ctx,
    TTxDiskRegistry::TWritableState& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "SetWritableState error: %s",
            FormatError(args.Error).c_str());
    }

    auto response = std::make_unique<
        TEvDiskRegistry::TEvSetWritableStateResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    BrokenDisksDestructionInProgress = false;
    DisksNotificationInProgress = false;
    UsersNotificationInProgress = false;
    DiskStatesPublicationInProgress = false;
    SecureEraseInProgressPerPool.assign(
        NProto::EDevicePoolKind_ARRAYSIZE,
        false);
    StartMigrationInProgress = false;
}

}   // namespace NCloud::NBlockStore::NStorage
