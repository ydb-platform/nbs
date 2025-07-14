#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::ProcessCheckpointQueue(const TActorContext& ctx)
{
    ui64 minCommitId = State->GetCommitQueue().GetMinCommitId();

    auto& checkpointsInflight = State->GetCheckpointsInFlight();

    std::unique_ptr<ITransactionBase> tx;
    while (tx = checkpointsInflight.GetTx(minCommitId)) {
        ExecuteTx(ctx, std::move(tx));
    }
}

void TPartitionActor::ProcessNextCheckpointRequest(
    const TActorContext& ctx,
    const TString& checkpointId)
{
    ui64 minCommitId = State->GetCommitQueue().GetMinCommitId();

    State->GetCheckpointsInFlight().PopTx(checkpointId);

    auto tx = State->GetCheckpointsInFlight().GetTx(checkpointId, minCommitId);
    if (tx) {
        ExecuteTx(ctx, std::move(tx));
    }
}

void TPartitionActor::HandleCreateCheckpoint(
    const TEvService::TEvCreateCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "CreateCheckpoint",
        requestInfo->CallContext->RequestId);

    const auto& checkpointId = msg->Record.GetCheckpointId();
    if (!checkpointId) {
        auto response = std::make_unique<TEvService::TEvCreateCheckpointResponse>(
            MakeError(E_ARGUMENT, "missing checkpoint identifier"));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "CreateCheckpoint",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    AddTransaction<TEvService::TCreateCheckpointMethod>(*requestInfo);

    TString idempotenceId = GetIdempotenceId(*msg);

    ui64 commitId = State->GetLastCommitId();

    auto tx = CreateTx<TCreateCheckpoint>(
        std::move(requestInfo),
        TCheckpoint(
            checkpointId,
            commitId,
            idempotenceId,
            ctx.Now(),
            {}),
        msg->Record.GetCheckpointType() == NProto::ECheckpointType::WITHOUT_DATA);

    ui64 minCommitId = State->GetCommitQueue().GetMinCommitId();

    State->GetCheckpointsInFlight().AddTx(checkpointId, std::move(tx), commitId);

    auto nextTx = State->GetCheckpointsInFlight().GetTx(checkpointId, minCommitId);
    if (nextTx) {
        ExecuteTx(ctx, std::move(nextTx));
    }
}

bool TPartitionActor::PrepareCreateCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCreateCheckpoint& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteCreateCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCreateCheckpoint& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    auto checkpoint = args.Checkpoint;  // copy to update
    checkpoint.Stats.CopyFrom(State->GetStats());

    bool added = (args.WithoutData)
                ? State->GetCheckpoints().AddCheckpointMapping(checkpoint)
                : State->GetCheckpoints().Add(checkpoint);
    if (added) {
        db.WriteCheckpoint(checkpoint, args.WithoutData);
    } else {
        auto existingIdempotenceId = State->GetCheckpoints().GetIdempotenceId(checkpoint.CheckpointId);
        if (existingIdempotenceId != checkpoint.IdempotenceId) {
            args.Error = MakeError(S_ALREADY, TStringBuilder()
                << "checkpoint already exists: " << checkpoint.CheckpointId.Quote());
        }
    }
}

void TPartitionActor::CompleteCreateCheckpoint(
    const TActorContext& ctx,
    TTxPartition::TCreateCheckpoint& args)
{
    auto response = std::make_unique<TEvService::TEvCreateCheckpointResponse>(args.Error);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "CreateCheckpoint",
        args.RequestInfo->CallContext->RequestId);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Checkpoint %s %s created (commit: %lu)",
        LogTitle.GetWithTime().c_str(),
        args.WithoutData ? "without data" : "",
        args.Checkpoint.CheckpointId.Quote().c_str(),
        args.Checkpoint.CommitId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);

    ProcessNextCheckpointRequest(ctx, args.Checkpoint.CheckpointId);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleDeleteCheckpoint(
    const TEvService::TEvDeleteCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    DeleteCheckpoint<TEvService::TDeleteCheckpointMethod>(ev, ctx, false);
}

void TPartitionActor::HandleDeleteCheckpointData(
    const TEvVolume::TEvDeleteCheckpointDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    DeleteCheckpoint<TEvVolume::TDeleteCheckpointDataMethod>(ev, ctx, true);
}

template <typename TMethod>
void TPartitionActor::DeleteCheckpoint(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx,
    bool deleteOnlyData)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        TMethod::Name,
        requestInfo->CallContext->RequestId);

    auto reply = [] (
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        const NProto::TError& error)
    {
        auto response = std::make_unique<typename TMethod::TResponse>(error);

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            TMethod::Name,
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    };

    const auto& checkpointId = msg->Record.GetCheckpointId();
    if (!checkpointId) {
        reply(ctx, requestInfo, MakeError(E_ARGUMENT, "missing checkpoint identifier"));
        return;
    }

    AddTransaction<TMethod>(*requestInfo);

    auto tx = CreateTx<TDeleteCheckpoint>(
        std::move(requestInfo),
        checkpointId,
        reply,
        deleteOnlyData);

    ui64 minCommitId = State->GetCommitQueue().GetMinCommitId();

    State->GetCheckpointsInFlight().AddTx(checkpointId, std::move(tx));

    auto nextTx = State->GetCheckpointsInFlight().GetTx(checkpointId, minCommitId);
    if (nextTx) {
        ExecuteTx(ctx, std::move(nextTx));
    }
}

bool TPartitionActor::PrepareDeleteCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteCheckpoint& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteDeleteCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteCheckpoint& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    bool deleted = State->GetCheckpoints().Delete(args.CheckpointId);
    if (!args.DeleteOnlyData) {
        deleted |= State->GetCheckpoints().DeleteCheckpointMapping(args.CheckpointId);
    }

    if (deleted) {
        db.DeleteCheckpoint(args.CheckpointId, args.DeleteOnlyData);
    } else {
        args.Error = MakeError(S_ALREADY, TStringBuilder()
            << "checkpoint not found: " << args.CheckpointId.Quote());
    }
}

void TPartitionActor::CompleteDeleteCheckpoint(
    const TActorContext& ctx,
    TTxPartition::TDeleteCheckpoint& args)
{
    args.ReplyCallback(ctx, args.RequestInfo, args.Error);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Checkpoint %s %s",
        LogTitle.GetWithTime().c_str(),
        args.CheckpointId.Quote().c_str(),
        args.DeleteOnlyData ? "data deleted" : "deleted");

    RemoveTransaction(*args.RequestInfo);

    ProcessNextCheckpointRequest(ctx, args.CheckpointId);
    EnqueueCleanupIfNeeded(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
