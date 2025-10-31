#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TZeroBlocksActor final
    : public TActorBootstrapped<TZeroBlocksActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TActorId Tablet;
    const ui64 CommitId;

    TVector<TAddMergedBlob> MergedBlobs;
    ui32 BlocksCount = 0;

    bool SafeToUseOrbit = true;

public:
    TZeroBlocksActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        ui64 commitId,
        TVector<TAddMergedBlob> mergedBlobs);

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
        TVector<TAddMergedBlob> mergedBlobs)
    : RequestInfo(std::move(requestInfo))
    , Tablet(tablet)
    , CommitId(commitId)
    , MergedBlobs(std::move(mergedBlobs))
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
    for (const auto& blob: MergedBlobs) {
        BlocksCount += blob.BlockRange.Size();
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvAddBlobsRequest>(
        RequestInfo->CallContext,
        CommitId,
        TVector<TAddMixedBlob>(),
        std::move(MergedBlobs),
        TVector<TAddFreshBlob>(),
        ADD_WRITE_RESULT
    );

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

    request->ExecCycles = RequestInfo->GetExecCycles();
    request->TotalCycles = RequestInfo->GetTotalCycles();

    request->CommitId = CommitId;

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

void TZeroBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto respose = std::make_unique<TEvService::TEvZeroBlocksResponse>(
        MakeError(E_REJECTED, "tablet is shutting down"));

    ReplyAndDie(ctx, std::move(respose));
}

void TZeroBlocksActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    SafeToUseOrbit = true;

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    ReplyAndDie(ctx, std::make_unique<TEvService::TEvZeroBlocksResponse>());
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

}   // namespace

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

    TBlockRange64 writeRange;

    auto ok = InitReadWriteBlockRange(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        &writeRange
    );

    if (!ok) {
        auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>(
            MakeError(E_ARGUMENT, TStringBuilder()
                << "invalid block range ["
                << "index: " << msg->Record.GetStartIndex()
                << ", count: " << msg->Record.GetBlocksCount()
                << "]"));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "ZeroBlocks",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    ui64 commitId = State->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        requestInfo->CancelRequest(ctx);
        RebootPartitionOnCommitIdOverflow(ctx, "ZeroBlocks");
        return;
    }

    ++WriteAndZeroRequestsInProgress;

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start zero blocks @%lu (range: %s)",
        LogTitle.GetWithTime().c_str(),
        commitId,
        DescribeRange(writeRange).c_str());

    State->GetCommitQueue().AcquireBarrier(commitId);

    const auto requestSize = writeRange.Size() * State->GetBlockSize();
    const auto writeBlobThreshold =
        GetWriteBlobThreshold(*Config, PartitionConfig.GetStorageMediaKind());
    if (requestSize < writeBlobThreshold) {
        // small writes will be accumulated in FreshBlocks table
        AddTransaction<TEvService::TZeroBlocksMethod>(*requestInfo);

        auto tx = CreateTx<TZeroBlocks>(
            requestInfo,
            commitId,
            ConvertRangeSafe(writeRange));

        // start execution
        State->IncrementFreshBlocksInFlight(writeRange.Size());
        ExecuteTx(ctx, std::move(tx));
    } else {
        // large writes could skip FreshBlocks table completely
        TVector<TAddMergedBlob> requests(
            Reserve(1 + writeRange.Size() / State->GetMaxBlocksInBlob()));

        ui32 blobIndex = 0;
        for (ui64 blockIndex: xrange(writeRange, State->GetMaxBlocksInBlob())) {
            auto range = TBlockRange32::MakeClosedIntervalWithLimit(
                blockIndex,
                blockIndex + State->GetMaxBlocksInBlob() - 1,
                writeRange.End);

            auto blobId = State->GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                commitId,
                0,  // deletion marker
                blobIndex++);

            requests.emplace_back(
                blobId,
                range,
                TBlockMask(), // skipMask
                TVector<ui32>() /* checksums */);
        }

        Y_ABORT_UNLESS(requests);
        auto actor = NCloud::Register<TZeroBlocksActor>(
            ctx,
            requestInfo,
            SelfId(),
            commitId,
            std::move(requests));

        Actors.Insert(actor);
    }
}

void TPartitionActor::HandleZeroBlocksCompleted(
    const TEvPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui64 commitId = msg->CommitId;
    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete zero blocks @%lu",
        LogTitle.GetWithTime().c_str(),
        commitId);

    UpdateStats(msg->Stats);

    ui64 blocksCount = msg->Stats.GetUserWriteCounters().GetBlocksCount();
    ui64 requestBytes = blocksCount * State->GetBlockSize();

    UpdateCPUUsageStat(ctx.Now(), msg->ExecCycles);

    auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ZeroBlocks.AddRequest(time, requestBytes);

    State->GetCommitQueue().ReleaseBarrier(commitId);

    Actors.Erase(ev->Sender);

    Y_DEBUG_ABORT_UNLESS(WriteAndZeroRequestsInProgress > 0);
    --WriteAndZeroRequestsInProgress;

    DrainActorCompanion.ProcessDrainRequests(ctx);
    ProcessCommitQueue(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareZeroBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TZeroBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    // we really want to keep the writes blind
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

    ui64 commitId = args.CommitId;

    State->ZeroFreshBlocks(db, args.WriteRange, commitId);

    // update counters
    State->DecrementFreshBlocksInFlight(args.WriteRange.Size());

    db.WriteMeta(State->GetMeta());

    State->UnsetUsedBlocks(db, args.WriteRange);
}

void TPartitionActor::CompleteZeroBlocks(
    const TActorContext& ctx,
    TTxPartition::TZeroBlocks& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    ui64 commitId = args.CommitId;
    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete ZeroBlocks transaction @%lu",
        LogTitle.GetWithTime().c_str(),
        commitId);

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

    ui64 requestBytes = static_cast<ui64>(args.WriteRange.Size()) * State->GetBlockSize();

    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());

    auto time = CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.ZeroBlocks.AddRequest(time, requestBytes);

    State->GetCommitQueue().ReleaseBarrier(args.CommitId);

    Y_DEBUG_ABORT_UNLESS(WriteAndZeroRequestsInProgress > 0);
    --WriteAndZeroRequestsInProgress;

    EnqueueFlushIfNeeded(ctx);
    DrainActorCompanion.ProcessDrainRequests(ctx);
    ProcessCommitQueue(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
