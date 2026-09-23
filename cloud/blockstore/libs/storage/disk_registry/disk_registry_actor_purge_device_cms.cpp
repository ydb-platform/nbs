#include "disk_registry_actor.h"

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandlePurgeDeviceCms(
    const TEvDiskRegistryPrivate::TEvPurgeDeviceCmsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(PurgeDeviceCms);

    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received PurgeDeviceCms request: host=%s, path=%s, "
        "State=%s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Host.c_str(),
        msg->Path.c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    const ui32 maxInFlight = Config->GetMaxInFlightCmsRequests();
    if (maxInFlight > 0 &&
        TransactionTimeTracker.GetInFlightOperationsCountByTransactionName(
            TPurgeDeviceCms::Name) >= maxInFlight)
    {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvDiskRegistryPrivate::TEvPurgeDeviceCmsResponse>(
                MakeError(E_REJECTED, "too many inflight transactions")));
        return;
    }

    ExecuteTx<TPurgeDeviceCms>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Host),
        std::move(msg->Path),
        std::move(msg->CustomMessage),
        msg->ShouldResumeDevice,
        msg->DryRun);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PreparePurgeDeviceCms(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TPurgeDeviceCms& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecutePurgeDeviceCms(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TPurgeDeviceCms& args)
{
    TDiskRegistryDatabase db(tx.DB);

    args.TxTs = ctx.Now();

    auto result = State->PurgeDevice(
        db,
        args.Host,
        args.Path,
        args.CustomMessage,
        args.TxTs,
        args.ShouldResumeDevice,
        args.DryRun);

    args.Error = std::move(result.Error);
    args.AffectedDisks = std::move(result.AffectedDisks);
    args.Timeout = result.Timeout;
    // Round up to seconds because TActionResult::Timeout is specified in
    // seconds
    if (args.Timeout) {
        args.Timeout = Max(args.Timeout, TDuration::Seconds(1));
    }
}

void TDiskRegistryActor::CompletePurgeDeviceCms(
    const TActorContext& ctx,
    TTxDiskRegistry::TPurgeDeviceCms& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s PurgeDeviceCms result: %s %u",
        LogTitle.GetWithTime().c_str(),
        FormatError(args.Error).c_str(),
        args.Timeout.Seconds());

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);

    SecureErase(ctx);
    StartMigration(ctx);
    ProcessPathsToAttach(ctx);

    using TResponse = TEvDiskRegistryPrivate::TEvPurgeDeviceCmsResponse;

    auto response = std::make_unique<TResponse>(
        std::move(args.Error),
        args.Timeout,
        std::move(args.AffectedDisks));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
