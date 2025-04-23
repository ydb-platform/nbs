#include "part2_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleDeleteCheckpoint(
    const TEvService::TEvDeleteCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "DeleteCheckpoint",
        requestInfo->CallContext->RequestId);

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvService::TEvDeleteCheckpointResponse>(
            MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "DeleteCheckpoint",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    const auto& checkpointId = msg->Record.GetCheckpointId();
    if (!checkpointId) {
        replyError(ctx, *requestInfo, E_ARGUMENT, "missing checkpoint identifier");
        return;
    }

    ui64 commitId = State->GetCheckpoints().GetCommitId(checkpointId);
    if (!commitId) {
        replyError(ctx, *requestInfo, S_ALREADY, TStringBuilder()
            << "checkpoint not found: " << checkpointId.Quote());
        return;
    }

    AddTransaction<TEvService::TDeleteCheckpointMethod>(*requestInfo);

    auto tx = CreateTx<TDeleteCheckpoint>(
        requestInfo,
        commitId,
        checkpointId);

    auto& queue = State->GetCCCRequestQueue();
    // shouldn't wait for inflight fresh blocks to complete (commitID == 0)
    queue.push_back({ 0, std::move(tx) });

    ProcessCCCRequestQueue(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareDeleteCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteCheckpoint& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    return db.ReadCheckpointBlobs(args.CommitId, args.BlobIds);
}

void TPartitionActor::ExecuteDeleteCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteCheckpoint& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    auto marked = State->MarkCheckpointDeleted(
        db,
        ctx.Now(),
        args.CommitId,
        args.CheckpointId,
        std::move(args.BlobIds)
    );

    if (!marked) {
        args.Error = MakeError(S_ALREADY, TStringBuilder()
            << "checkpoint not found: " << args.CheckpointId.Quote());
        return;
    }
}

void TPartitionActor::CompleteDeleteCheckpoint(
    const TActorContext& ctx,
    TTxPartition::TDeleteCheckpoint& args)
{
    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    State->StopProcessingCCCRequest();

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Checkpoint %s deletion scheduled (commit: %lu)",
        TabletID(),
        args.CheckpointId.Quote().data(),
        args.CommitId);

    auto response = std::make_unique<TEvService::TEvDeleteCheckpointResponse>(args.Error);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "DeleteCheckpoint",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    UpdateCPUUsageStats(ctx, timer.Finish());

    EnqueueCompactionIfNeeded(ctx);

    ProcessCCCRequestQueue(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleDeleteCheckpointData(
    const TEvVolume::TEvDeleteCheckpointDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvVolume::TDeleteCheckpointDataMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    // TODO(NBS-2382) - support DeleteCheckpointData for partition v2
    auto response = std::make_unique<TEvVolume::TEvDeleteCheckpointDataResponse>(
        MakeError(S_FALSE, "nothing to delete"));

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
