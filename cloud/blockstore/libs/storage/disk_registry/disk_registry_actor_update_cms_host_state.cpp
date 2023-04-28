#include "disk_registry_actor.h"
#include "disk_registry_database.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateCmsHostState(
    const TEvDiskRegistryPrivate::TEvUpdateCmsHostStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateCmsHostDeviceState);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId));

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received UpdateCmsHostState request: Host=%s, State=%u",
        TabletID(),
        msg->Host.c_str(),
        static_cast<ui32>(msg->State));

    BLOCKSTORE_TRACE_RECEIVED(ctx, &requestInfo->TraceId, this, msg);

    ExecuteTx<TUpdateCmsHostState>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Host),
        msg->State,
        msg->DryRun);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdateCmsHostState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateCmsHostState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateCmsHostState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateCmsHostState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);

    args.TxTs = ctx.Now();

    args.Error = State->UpdateCmsHostState(
        db,
        args.Host,
        args.State,
        args.TxTs,
        args.DryRun,
        args.AffectedDisks,
        args.Timeout);
}

void TDiskRegistryActor::CompleteUpdateCmsHostState(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateCmsHostState& args)
{
    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "UpdateCmsHost result: Host=%s Error=%s Timeout=%u AffectedDisks=%s",
        args.Host.c_str(),
        FormatError(args.Error).c_str(),
        args.Timeout.Seconds(),
        [&] {
            TStringStream out;
            out << "[";
            for (const auto& [state, seqNo]: args.AffectedDisks) {
                out << " " << state.GetDiskId()
                    << ":" << static_cast<int>(state.GetState());
            }
            out << "]";
            return out.Str();
        }().c_str());

    NotifyDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);

    SecureErase(ctx);
    StartMigration(ctx);

    using TResponse = TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse;

    auto response = std::make_unique<TResponse>(std::move(args.Error));
    response->Timeout = args.Timeout;
    response->DependentDiskIds.reserve(args.AffectedDisks.size());
    for (const auto& ad: args.AffectedDisks) {
        response->DependentDiskIds.push_back(ad.State.GetDiskId());
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
