#include "part2_actor.h"

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

class TZeroBlocksActor final
    : public TActorBootstrapped<TZeroBlocksActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TActorId Tablet;
    const ui64 CommitId;
    TVector<TAddBlob> Blobs;

    ui32 BlocksCount = 0;

    bool SafeToUseOrbit = true;

public:
    TZeroBlocksActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        ui64 commitId,
        TVector<TAddBlob> blobs);

    void Bootstrap(const TActorContext& ctx);

private:
    void AddBlobs(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvZeroBlocksResponse> response);

private:
    STFUNC(StateWork);

    void HandleAddBlobsResponse(
        const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TZeroBlocksActor::TZeroBlocksActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        ui64 commitId,
        TVector<TAddBlob> blobs)
    : RequestInfo(std::move(requestInfo))
    , Tablet(tablet)
    , CommitId(commitId)
    , Blobs(std::move(blobs))
{}

void TZeroBlocksActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ZeroBlocks",
        RequestInfo->CallContext->RequestId);

    AddBlobs(ctx);
}

void TZeroBlocksActor::AddBlobs(const TActorContext& ctx)
{
    for (const auto& blob: Blobs) {
        BlocksCount += blob.Blocks.size();
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvAddBlobsRequest>(
        RequestInfo->CallContext,
        ADD_ZERO_RESULT,
        std::move(Blobs));

    SafeToUseOrbit = false;

    NCloud::Send(
        ctx,
        Tablet,
        std::move(request));
}

void TZeroBlocksActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvZeroBlocksCompleted>(error);

    request->CommitId = CommitId;

    request->ExecCycles = RequestInfo->GetExecCycles();
    request->TotalCycles = RequestInfo->GetTotalCycles();

    {
        auto execTime = CyclesToDurationSafe(RequestInfo->GetExecCycles());
        auto waitTime = CyclesToDurationSafe(RequestInfo->GetWaitCycles());

        auto& counters = *request->Stats.MutableUserWriteCounters();
        counters.SetRequestsCount(1);
        counters.SetBlocksCount(BlocksCount);
        counters.SetExecTime(execTime.MicroSeconds());
        counters.SetWaitTime(waitTime.MicroSeconds());
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

bool TZeroBlocksActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvZeroBlocksResponse>(error));
        return true;
    }
    return false;
}

void TZeroBlocksActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvZeroBlocksResponse> response)
{
    NotifyCompleted(ctx, response->GetError());

    if (SafeToUseOrbit) {
        LWTRACK(
            ResponseSent_Partition,
            RequestInfo->CallContext->LWOrbit,
            "ZeroBlocks",
            RequestInfo->CallContext->RequestId);
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TZeroBlocksActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    SafeToUseOrbit = true;

    const auto& error = msg->GetError();
    if (HandleError(ctx, error)) {
        return;
    }

    ReplyAndDie(ctx, std::make_unique<TEvService::TEvZeroBlocksResponse>());
}

void TZeroBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto respose = std::make_unique<TEvService::TEvZeroBlocksResponse>(
        MakeError(E_REJECTED, "tablet is shutting down"));

    ReplyAndDie(ctx, std::move(respose));
}

STFUNC(TZeroBlocksActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvPartitionPrivate::TEvAddBlobsResponse, HandleAddBlobsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
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
        "ZeroBlocks",
        requestInfo->CallContext->RequestId);

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>(
            MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "ZeroBlocks",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    TBlockRange64 writeRange;

    auto ok = InitReadWriteBlockRange(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        &writeRange
    );

    if (!ok) {
        replyError(ctx, *requestInfo, E_ARGUMENT, TStringBuilder()
            << "invalid block range ["
            << "index: " << msg->Record.GetStartIndex()
            << ", count: " << msg->Record.GetBlocksCount()
            << "]");
        return;
    }

    if (State->GetBaseDiskId()) {
        ui64 commitId = State->GenerateCommitId();
        if (commitId == InvalidCommitId) {
            requestInfo->CancelRequest(ctx);
            RebootPartitionOnCommitIdOverflow(ctx, "WriteBlocks");
            return;
        }

        LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Start zero blocks @%lu (range: %s)",
            TabletID(),
            commitId,
            DescribeRange(writeRange).data()
        );

        ++WriteAndZeroRequestsInProgress;

        // needed to prevent the blobs updated during the AddBlobs stage from
        // being deleted before AddBlobs tx completes
        State->AcquireCollectBarrier(commitId);

        TVector<TAddBlob> requests(
            Reserve(std::ceil(double(writeRange.Size()) / State->GetMaxBlocksInBlob())));

        ui32 blobIndex = 0;
        for (ui64 blockIndex: xrange(writeRange, State->GetMaxBlocksInBlob())) {
            auto blobId = State->GenerateBlobId(
                EChannelDataKind::Merged,  // does not matter
                EChannelPermission::UserWritesAllowed,
                commitId,
                0,  // deletion marker
                blobIndex++);

            auto range = TBlockRange32::MakeClosedIntervalWithLimit(
                blockIndex,
                blockIndex + State->GetMaxBlocksInBlob() - 1,
                writeRange.End);

            TVector<TBlock> blocks(Reserve(range.Size()));
            for (ui32 blockIndex: xrange(range)) {
                // actual MinCommitId will be generated later
                blocks.emplace_back(
                    blockIndex,
                    InvalidCommitId,
                    InvalidCommitId,
                    true);  // zeroed
            }

            requests.emplace_back(blobId, std::move(blocks));
        }

        Y_ABORT_UNLESS(requests);
        auto actor = NCloud::Register<TZeroBlocksActor>(
            ctx,
            requestInfo,
            SelfId(),
            commitId,
            std::move(requests));

        Actors.insert(actor);
        return;
    }

    ++WriteAndZeroRequestsInProgress;

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Start zero blocks (range: %s)",
        TabletID(),
        DescribeRange(writeRange).data());

    AddTransaction<TEvService::TZeroBlocksMethod>(*requestInfo);

    ExecuteTx<TZeroBlocks>(
        ctx,
        requestInfo,
        ConvertRangeSafe(writeRange)
    );
}

void TPartitionActor::HandleZeroBlocksCompleted(
    const TEvPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Complete zero blocks @%lu",
        TabletID(),
        msg->CommitId);

    UpdateStats(msg->Stats);

    ui64 blocksCount = msg->Stats.GetUserWriteCounters().GetBlocksCount();
    ui64 requestBytes = blocksCount * State->GetBlockSize();

    UpdateCPUUsageStat(ctx, msg->ExecCycles);

    auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ZeroBlocks.AddRequest(time, requestBytes);

    State->ReleaseCollectBarrier(msg->CommitId);

    Actors.erase(ev->Sender);

    Y_DEBUG_ABORT_UNLESS(WriteAndZeroRequestsInProgress > 0);
    --WriteAndZeroRequestsInProgress;

    DrainActorCompanion.ProcessDrainRequests(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareZeroBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TZeroBlocks& args)
{
    Y_UNUSED(ctx);

    // writes are usually blind but we still need our index structures to be
    // properly initialized
    TPartitionDatabase db(tx.DB);
    if (!State->InitIndex(db, args.WriteRange)) {
        return false;
    }

    return true;
}

void TPartitionActor::ExecuteZeroBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TZeroBlocks& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    args.CommitId = State->GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return;
    }

    // mark overwritten blocks
    State->AddFreshBlockUpdate(db, {args.CommitId, args.WriteRange});
    State->MarkMergedBlocksDeleted(db, args.WriteRange, args.CommitId);
}

void TPartitionActor::CompleteZeroBlocks(
    const TActorContext& ctx,
    TTxPartition::TZeroBlocks& args)
{
    if (args.CommitId == InvalidCommitId) {
        RebootPartitionOnCommitIdOverflow(ctx, "ZeroBlocks");
        Y_DEBUG_ABORT_UNLESS(WriteAndZeroRequestsInProgress > 0);
        --WriteAndZeroRequestsInProgress;
        return;
    }

    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Complete zero blocks @%lu",
        TabletID(),
        args.CommitId);

    auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "ZeroBlocks",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    NProto::TPartitionStats stats;
    {
        auto execTime = CyclesToDurationSafe(args.RequestInfo->GetExecCycles());
        auto waitTime = CyclesToDurationSafe(args.RequestInfo->GetWaitCycles());

        auto& counters = *stats.MutableUserWriteCounters();
        counters.SetRequestsCount(1);
        counters.SetBlocksCount(args.WriteRange.Size());
        counters.SetExecTime(execTime.MicroSeconds());
        counters.SetWaitTime(waitTime.MicroSeconds());
    }
    UpdateStats(stats);

    UpdateCPUUsageStat(ctx, timer.Finish());

    auto time = CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    ui64 requestBytes = static_cast<ui64>(State->GetBlockSize()) * args.WriteRange.Size();
    PartCounters->RequestCounters.ZeroBlocks.AddRequest(time, requestBytes);

    if (Executor()->GetStats().IsAnyChannelYellowMove) {
        ScheduleYellowStateUpdate(ctx);
    }

    Y_DEBUG_ABORT_UNLESS(WriteAndZeroRequestsInProgress > 0);
    --WriteAndZeroRequestsInProgress;

    DrainActorCompanion.ProcessDrainRequests(ctx);
    EnqueueCompactionIfNeeded(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
