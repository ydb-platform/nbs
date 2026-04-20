#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError CheckCheckpointRequest(
    const TPartitionState& state,
    const TString& checkpointId)
{
    if (!checkpointId) {
        return MakeError(E_ARGUMENT, "missing checkpoint identifier");
    }

    if (state.GetCheckpointsInFlight()->HasCheckpoint(checkpointId)) {
        return MakeError(E_REJECTED, "checkpoint already in flight");
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::ProcessCheckpointQueue(const TActorContext& ctx)
{
    SharedState->ProcessCheckpointQueue(ctx);
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
    const bool withoutData = msg->Record.GetCheckpointType() ==
                             NProto::ECheckpointType::WITHOUT_DATA;

    auto error = CheckCheckpointRequest(*State, checkpointId);

    if (HasError(error)) {
        auto response =
            std::make_unique<TEvService::TEvCreateCheckpointResponse>(error);

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
        TCheckpoint(checkpointId, commitId, idempotenceId, ctx.Now(), {}),
        withoutData);

    //
    // In-flight checkpoint CommitIds are used to determine whether block
    // CommitIds are garbage or not alongside the CommitIds of the created
    // checkpoints. The only caveat is that in-flight checkpoints don't track
    // the WithoutData flag so there's a tiny chance that upon GetChangedBlocks
    // some of the bits would have a false-positive value (1). This should not
    // cause any problems in real scenarios because:
    // 1. we expect hi checkpoint id to refer to a checkpoint with data
    // 2. we expect the user of GetChangedBlocks to use those bits to determine
    //  whether a specific block should be read or can be skipped and assumed to
    //  be equal to some older value - in this case the only consequence can be
    //  reading some blocks when we don't have to.
    //

    SharedState
        ->WaitCommitForCheckpoint(ctx, std::move(tx), checkpointId, commitId);
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

    bool added =
        (args.WithoutData)
            ? State->AccessCheckpoints().AddCheckpointMapping(checkpoint)
            : State->AccessCheckpoints().Add(checkpoint);
    if (added) {
        db.WriteCheckpoint(checkpoint, args.WithoutData);
    } else {
        auto existingIdempotenceId =
            State->GetCheckpoints().GetIdempotenceId(checkpoint.CheckpointId);
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

    State->AccessCheckpointsInFlight()->PopTx(args.Checkpoint.CheckpointId);
    ProcessCheckpointQueue(ctx);
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

    auto reply = [](const TActorContext& ctx,
                    TRequestInfoPtr requestInfo,
                    NProto::TError error)
    {
        auto response =
            std::make_unique<typename TMethod::TResponse>(std::move(error));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            TMethod::Name,
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    };

    const auto& checkpointId = msg->Record.GetCheckpointId();

    auto error = CheckCheckpointRequest(*State, checkpointId);
    if (HasError(error)) {
        reply(ctx, std::move(requestInfo), error);
        return;
    }

    AddTransaction<TMethod>(*requestInfo);

    auto tx = CreateTx<TDeleteCheckpoint>(
        std::move(requestInfo),
        checkpointId,
        reply,
        deleteOnlyData);

    SharedState->WaitCommitForCheckpoint(
        ctx,
        std::move(tx),
        checkpointId,
        0 /* commitId */
    );
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

    bool deleted = State->AccessCheckpoints().Delete(args.CheckpointId);
    if (!args.DeleteOnlyData) {
        deleted |= State->AccessCheckpoints().DeleteCheckpointMapping(
            args.CheckpointId);
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

    State->AccessCheckpointsInFlight()->PopTx(args.CheckpointId);
    EnqueueCleanupIfNeeded(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
