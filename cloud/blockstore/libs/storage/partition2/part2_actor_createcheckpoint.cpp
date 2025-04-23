#include "part2_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleCreateCheckpoint(
    const TEvService::TEvCreateCheckpointRequest::TPtr& ev,
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
        "CreateCheckpoint",
        requestInfo->CallContext->RequestId);

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvService::TEvCreateCheckpointResponse>(
            MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "CreateCheckpoint",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    const auto& checkpointId = msg->Record.GetCheckpointId();
    if (!checkpointId) {
        replyError(ctx, *requestInfo, E_ARGUMENT, "missing checkpoint identifier");
        return;
    }

    if (State->GetCheckpoints().GetCommitId(checkpointId)) {
        replyError(ctx, *requestInfo, S_ALREADY, TStringBuilder()
            << "checkpoint already exists: " << checkpointId.Quote());
        return;
    }

    const ui64 commitId = State->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        requestInfo->CancelRequest(ctx);
        RebootPartitionOnCommitIdOverflow(ctx, "CreateCheckpoint");
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Create checkpoint (%s)",
        TabletID(),
        checkpointId.Quote().data());

    NProto::TCheckpointMeta meta;
    meta.SetCheckpointId(checkpointId);
    meta.SetCommitId(commitId);
    meta.SetDateCreated(ctx.Now().MicroSeconds());
    meta.MutableStats()->CopyFrom(State->GetStats());

    State->WriteCheckpoint(meta);

    AddTransaction<TEvService::TCreateCheckpointMethod>(*requestInfo);

    auto tx = CreateTx<TCreateCheckpoint>(
        requestInfo,
        std::move(meta));

    auto& queue = State->GetCCCRequestQueue();
    queue.push_back({ commitId, std::move(tx) });

    ProcessCCCRequestQueue(ctx);
}

////////////////////////////////////////////////////////////////////////////////

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

    db.WriteCheckpoint(args.Meta);
}

void TPartitionActor::CompleteCreateCheckpoint(
    const TActorContext& ctx,
    TTxPartition::TCreateCheckpoint& args)
{
    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    State->StopProcessingCCCRequest();

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Checkpoint %s created (commit: %lu)",
        TabletID(),
        args.Meta.GetCheckpointId().Quote().data(),
        args.Meta.GetCommitId());

    auto response = std::make_unique<TEvService::TEvCreateCheckpointResponse>();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "CreateCheckpoint",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    UpdateCPUUsageStats(ctx, timer.Finish());

    ProcessCCCRequestQueue(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
