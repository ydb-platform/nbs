#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleAllocateCheckpoint(
    const TEvDiskRegistry::TEvAllocateCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AllocateCheckpoint);

    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received AllocateCheckpoint request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    NProto::TCheckpointReplica checkpointReplica;
    checkpointReplica.SetSourceDiskId(msg->Record.GetSourceDiskId());
    checkpointReplica.SetCheckpointId(msg->Record.GetCheckpointId());

    ExecuteTx<TAllocateCheckpoint>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        std::move(checkpointReplica));
}

bool TDiskRegistryActor::PrepareAllocateCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAllocateCheckpoint& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteAllocateCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAllocateCheckpoint& args)
{
    TDiskRegistryDatabase db(tx.DB);

    TDiskRegistryState::TAllocateCheckpointResult result{};

    args.Error = State->AllocateCheckpoint(
        ctx.Now(),
        db,
        args.CheckpointReplica.GetSourceDiskId(),
        args.CheckpointReplica.GetCheckpointId(),
        &result);
    args.ShadowDiskId = result.ShadowDiskId;
}

void TDiskRegistryActor::CompleteAllocateCheckpoint(
    const TActorContext& ctx,
    TTxDiskRegistry::TAllocateCheckpoint& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s AllocateCheckpoint error: %s. DiskId=%s, CheckpointId=%s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str(),
            args.CheckpointReplica.GetSourceDiskId().Quote().c_str(),
            args.CheckpointReplica.GetCheckpointId().Quote().c_str());
    }

    auto response =
        std::make_unique<TEvDiskRegistry::TEvAllocateCheckpointResponse>(
            std::move(args.Error));
    response->Record.SetShadowDiskId(args.ShadowDiskId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleDeallocateCheckpoint(
    const TEvDiskRegistry::TEvDeallocateCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(DeallocateCheckpoint);

    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received DeallocateCheckpoint request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TDeallocateCheckpoint>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        msg->Record.GetSourceDiskId(),
        msg->Record.GetCheckpointId());
}

bool TDiskRegistryActor::PrepareDeallocateCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeallocateCheckpoint& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteDeallocateCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeallocateCheckpoint& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);

    args.Error = State->DeallocateCheckpoint(
        db,
        args.SourceDiskId,
        args.CheckpointId,
        &args.ShadowDiskId);
}

void TDiskRegistryActor::CompleteDeallocateCheckpoint(
    const TActorContext& ctx,
    TTxDiskRegistry::TDeallocateCheckpoint& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s DeallocateCheckpoint error: %s. DiskId=%s, CheckpointId=%s, "
            "ShadowDiskId=%s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str(),
            args.SourceDiskId.Quote().c_str(),
            args.CheckpointId.Quote().c_str(),
            args.ShadowDiskId.Quote().c_str());
    } else {
        OnDiskDeallocated(args.ShadowDiskId);
    }

    auto response =
        std::make_unique<TEvDiskRegistry::TEvDeallocateCheckpointResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    SecureErase(ctx);
    ProcessPathsToAttachDetach(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleGetCheckpointDataState(
    const TEvDiskRegistry::TEvGetCheckpointDataStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AllocateCheckpoint);

    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received GetCheckpointDataState request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    NProto::ECheckpointState checkpointState = {};
    auto error = State->GetCheckpointDataState(
        msg->Record.GetSourceDiskId(),
        msg->Record.GetCheckpointId(),
        &checkpointState);

    auto response =
        std::make_unique<TEvDiskRegistry::TEvGetCheckpointDataStateResponse>(
            std::move(error));
    response->Record.SetCheckpointState(checkpointState);

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleSetCheckpointDataState(
    const TEvDiskRegistry::TEvSetCheckpointDataStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(SetCheckpointDataState);

    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received SetCheckpointDataState request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TSetCheckpointDataState>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        msg->Record.GetSourceDiskId(),
        msg->Record.GetCheckpointId(),
        msg->Record.GetCheckpointState());
}

bool TDiskRegistryActor::PrepareSetCheckpointDataState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TSetCheckpointDataState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteSetCheckpointDataState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TSetCheckpointDataState& args)
{
    TDiskRegistryDatabase db(tx.DB);

    TVector<TDiskId> affectedDisks;
    args.Error = State->SetCheckpointDataState(
        ctx.Now(),
        db,
        args.SourceDiskId,
        args.CheckpointId,
        args.CheckpointState);
}

void TDiskRegistryActor::CompleteSetCheckpointDataState(
    const TActorContext& ctx,
    TTxDiskRegistry::TSetCheckpointDataState& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s SetCheckpointDataState error: %s. CheckpointId=%s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str(),
            args.CheckpointId.Quote().c_str());
    }

    auto response =
        std::make_unique<TEvDiskRegistry::TEvSetCheckpointDataStateResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
