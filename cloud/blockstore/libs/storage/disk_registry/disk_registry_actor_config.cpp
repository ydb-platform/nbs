#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateConfig(
    const TEvDiskRegistry::TEvUpdateConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateConfig);

    const auto* msg = ev->Get();
    auto newConfig = msg->Record.GetConfig();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received UpdateConfig request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TUpdateConfig>(
        ctx,
        std::move(requestInfo),
        std::move(newConfig),
        msg->Record.GetIgnoreVersion());
}

bool TDiskRegistryActor::PrepareUpdateConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateConfig& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateConfig& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->UpdateConfig(
        db,
        args.Config,
        args.IgnoreVersion,
        args.AffectedDisks);
}

void TDiskRegistryActor::CompleteUpdateConfig(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateConfig& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s UpdateConfig error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str());
    }

    auto response = std::make_unique<TEvDiskRegistry::TEvUpdateConfigResponse>(
        std::move(args.Error));

    auto& affectedDisks = *response->Record.MutableAffectedDisks();
    affectedDisks.Reserve(args.AffectedDisks.size());

    for (auto& id: args.AffectedDisks) {
        *affectedDisks.Add() = std::move(id);
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);
    StartMigration(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleDescribeConfig(
    const TEvDiskRegistry::TEvDescribeConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(DescribeConfig);

    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "%s Received DescribeConfig request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    auto response = std::make_unique<TEvDiskRegistry::TEvDescribeConfigResponse>();

    *response->Record.MutableConfig() = State->GetConfig();

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
