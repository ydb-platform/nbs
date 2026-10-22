#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/common/safe_debug_print.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleSetSuspendSecureErase(
    const TEvDiskRegistry::TEvSetSuspendSecureEraseRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(SetSuspendSecureErase);

    const auto* msg = ev->Get();
    const bool suspendSecureErase = msg->Record.GetState();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received SetSuspendSecureErase request: %s %s",
        LogTitle.GetWithTime().c_str(),
        SafeDebugPrint(msg->Record).c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    ExecuteTx<TSuspendSecureErase>(
        ctx,
        std::move(requestInfo),
        suspendSecureErase);
}

bool TDiskRegistryActor::PrepareSuspendSecureErase(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TSuspendSecureErase& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteSuspendSecureErase(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TSuspendSecureErase& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    db.WriteSuspendSecureErase(args.State);
}

void TDiskRegistryActor::CompleteSuspendSecureErase(
    const TActorContext& ctx,
    TTxDiskRegistry::TSuspendSecureErase& args)
{
    State->SetSuspendSecureErase(args.State);

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvDiskRegistry::TEvSetSuspendSecureEraseResponse>());

    SecureErase(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
