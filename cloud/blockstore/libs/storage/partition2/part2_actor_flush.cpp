#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/common/alloc.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t FreshBlockUpdatesSizeThreshold = 32 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TFlushActor final
    : public TActorBootstrapped<TFlushActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    const TActorId Tablet;
    TVector<TWriteBlob> Requests;
    TVector<TBlockRange64> AffectedRanges;
    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

    size_t RequestsCompleted = 0;
    size_t BlockCount = 0;

    TVector<TCallContextPtr> ForkedCallContexts;

public:
    TFlushActor(
        TRequestInfoPtr requestInfo,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        const TActorId& tablet,
        TVector<TWriteBlob> requests);

    void Bootstrap(const TActorContext& ctx);

private:
    void WriteBlobs(const TActorContext& ctx);
    void AddBlobs(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvPartitionPrivate::TEvFlushResponse> response);

private:
    STFUNC(StateWork);

    void HandleWriteBlobResponse(
        const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddBlobsResponse(
        const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TFlushActor::TFlushActor(
        TRequestInfoPtr requestInfo,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        const TActorId& tablet,
        TVector<TWriteBlob> requests)
    : RequestInfo(std::move(requestInfo))
    , BlockDigestGenerator(blockDigestGenerator)
    , Tablet(tablet)
    , Requests(std::move(requests))
{}

void TFlushActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "Flush",
        RequestInfo->CallContext->RequestId);

    TBlockRange64Builder rangeBuilder(AffectedRanges);
    for (const auto& request: Requests) {
        auto blockContent = request.BlobContent.Get().GetBlocks().begin();

        Y_DEBUG_ABORT_UNLESS(request.BlobContent.Get().GetBlocks().size()
            == request.Blocks.size());

        for (const auto& block: request.Blocks) {
            rangeBuilder.OnBlock(block.BlockIndex);

            auto digest = BlockDigestGenerator->ComputeDigest(
                block.BlockIndex,
                *blockContent);

            if (digest.Defined()) {
                AffectedBlockInfos.push_back({block.BlockIndex, *digest});
            }

            ++blockContent;
        }
    }

    WriteBlobs(ctx);
}

void TFlushActor::WriteBlobs(const TActorContext& ctx)
{
    for (auto& req: Requests) {
        BlockCount += req.Blocks.size();

        auto request = std::make_unique<TEvPartitionPrivate::TEvWriteBlobRequest>(
            req.BlobId,
            req.BlobContent.GetGuardedSgList(),
            true);  // async

        if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                RequestInfo->CallContext->LWOrbit,
                "TEvPartitionPrivate::TEvWriteBlobRequest",
                RequestInfo->CallContext->RequestId);
        }

        ForkedCallContexts.emplace_back(request->CallContext);

        NCloud::Send(
            ctx,
            Tablet,
            std::move(request));
    }
}

void TFlushActor::AddBlobs(const TActorContext& ctx)
{
    TVector<TAddBlob> blobs(Reserve(Requests.size()));
    for (auto& req: Requests) {
        blobs.emplace_back(req.BlobId, std::move(req.Blocks));
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvAddBlobsRequest>(
        RequestInfo->CallContext,
        ADD_FLUSH_RESULT,
        std::move(blobs));

    NCloud::Send(
        ctx,
        Tablet,
        std::move(request));
}

void TFlushActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvFlushCompleted>(error);
    request->ExecCycles = RequestInfo->GetExecCycles();
    request->TotalCycles = RequestInfo->GetTotalCycles();

    {
        auto execTime = CyclesToDurationSafe(RequestInfo->GetExecCycles());
        auto waitTime = CyclesToDurationSafe(RequestInfo->GetWaitCycles());

        auto& counters = *request->Stats.MutableSysWriteCounters();
        counters.SetRequestsCount(Requests.size());
        counters.SetBlocksCount(BlockCount);
        counters.SetExecTime(execTime.MicroSeconds());
        counters.SetWaitTime(waitTime.MicroSeconds());
    }

    request->AffectedRanges = std::move(AffectedRanges);
    request->AffectedBlockInfos = std::move(AffectedBlockInfos);

    NCloud::Send(ctx, Tablet, std::move(request));
}

bool TFlushActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvPartitionPrivate::TEvFlushResponse>(error));
        return true;
    }
    return false;
}

void TFlushActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvPartitionPrivate::TEvFlushResponse> response)
{
    NotifyCompleted(ctx, response->GetError());

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "Flush",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TFlushActor::HandleWriteBlobResponse(
    const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    RequestInfo->AddExecCycles(msg->ExecCycles);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    Y_ABORT_UNLESS(RequestsCompleted < Requests.size());
    if (++RequestsCompleted < Requests.size()) {
        return;
    }

    for (auto context: ForkedCallContexts) {
        RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
    }

    AddBlobs(ctx);
}

void TFlushActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto respose = std::make_unique<TEvPartitionPrivate::TEvFlushResponse>(
        MakeError(E_REJECTED, "tablet is shutting down"));

    ReplyAndDie(ctx, std::move(respose));
}

void TFlushActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    RequestInfo->AddExecCycles(msg->ExecCycles);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    ReplyAndDie(ctx, std::make_unique<TEvPartitionPrivate::TEvFlushResponse>());
}

STFUNC(TFlushActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvPartitionPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvPartitionPrivate::TEvAddBlobsResponse, HandleAddBlobsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TFlushBlocksVisitor final
    : public IFreshBlockVisitor
{
private:
    const ui32 BlockSize;
    const ui32 MaxBlobRangeSize;
    const ui32 MaxBlocksInBlob;
    const ui64 MaxCommitId;

    TVector<TWriteBlob> Blobs;
    TVector<TBlock> Blocks;
    TBlockBuffer BlobContent { TProfilingAllocator::Instance() };

public:
    TFlushBlocksVisitor(
            ui32 blockSize,
            ui32 maxBlobRangeSize,
            ui32 maxBlocksInBlob,
            ui64 maxCommitId)
        : BlockSize(blockSize)
        , MaxBlobRangeSize(maxBlobRangeSize)
        , MaxBlocksInBlob(maxBlocksInBlob)
        , MaxCommitId(maxCommitId)
    {}

    void Visit(const TBlock& block, TStringBuf blockContent) override
    {
        if (block.MinCommitId > MaxCommitId) {
            return;
        }

        if (Blocks) {
            // NBS-299: we do not want to mix blocks that are too far from each other
            ui32 firstBlockIndex = Blocks.front().BlockIndex;
            Y_ABORT_UNLESS(firstBlockIndex <= block.BlockIndex);
            if (block.BlockIndex - firstBlockIndex
                    > MaxBlobRangeSize / BlockSize)
            {
                Flush();
            }
        }

        Blocks.push_back(block);
        BlobContent.AddBlock({blockContent.data(), blockContent.size()});

        if (Blocks.size() == MaxBlocksInBlob) {
            Flush();
        }
    }

    TVector<TWriteBlob> Finish()
    {
        if (Blocks) {
            Flush();
        }
        return std::move(Blobs);
    }

private:
    void Flush()
    {
        Blobs.emplace_back(
            TPartialBlobId(), // to be filled later
            std::move(Blocks),
            std::move(BlobContent));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueFlushIfNeeded(const TActorContext& ctx)
{
    if (State->GetFlushStatus() != EOperationStatus::Idle) {
        // already enqueued
        return;
    }

    const auto freshBlockUpdatesSize = State->GetFreshBlockUpdateCount();
    const auto dataSize = State->GetFreshBlockCount() * State->GetBlockSize();
    const bool shouldFlush = !State->IsLoadStateFinished()
        || dataSize >= Config->GetFlushThreshold()
        || freshBlockUpdatesSize >= FreshBlockUpdatesSizeThreshold;

    if (!shouldFlush) {
        return;
    }

    State->SetFlushStatus(EOperationStatus::Enqueued);

    auto request = std::make_unique<TEvPartitionPrivate::TEvFlushRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()));

    NCloud::Send(
        ctx,
        SelfId(),
        std::move(request));
}

void TPartitionActor::HandleFlush(
    const TEvPartitionPrivate::TEvFlushRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvPartitionPrivate::TFlushMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        BackgroundTaskStarted_Partition,
        requestInfo->CallContext->LWOrbit,
        "Flush",
        static_cast<ui32>(PartitionConfig.GetStorageMediaKind()),
        requestInfo->CallContext->RequestId,
        PartitionConfig.GetDiskId());

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvPartitionPrivate::TEvFlushResponse>(
            MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "Flush",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetFlushStatus() == EOperationStatus::Started ||
        State->GetFlushStatus() == EOperationStatus::Delayed)
    {
        replyError(ctx, *requestInfo, E_TRY_AGAIN, "flush already in progress");
        return;
    }

    ui64 blockCount = State->GetFreshBlockCount();
    if (!blockCount) {
        State->SetFlushStatus(EOperationStatus::Idle);

        replyError(ctx, *requestInfo, S_ALREADY, "nothing to flush");
        return;
    }

    const ui64 commitId = State->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        requestInfo->CancelRequest(ctx);
        RebootPartitionOnCommitIdOverflow(ctx, "Flush");
        return;
    }

    State->AcquireCollectBarrier(commitId);
    State->ConstructFlushContext(std::move(requestInfo), commitId);

    if (State->HasFreshBlocksInFlightUntil(commitId)) {
        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Delaying flush @%lu",
            TabletID(),
            commitId);

        State->SetFlushStatus(EOperationStatus::Delayed);
        return;
    }

    StartFlush(ctx);
}

void TPartitionActor::ResumeDelayedFlushIfNeeded(const TActorContext& ctx)
{
    if (State->GetFlushStatus() != EOperationStatus::Delayed) {
        // no delayed flush is present
        return;
    }

    if (const auto& flushCtx = State->GetFlushContext();
        State->HasFreshBlocksInFlightUntil(flushCtx.CommitId))
    {
        // we still have to wait
        return;
    }

    StartFlush(ctx);
}

void TPartitionActor::StartFlush(const TActorContext& ctx)
{
    auto& flushCtx = State->GetFlushContext();

    const ui64 commitId = flushCtx.CommitId;
    Y_DEBUG_ABORT_UNLESS(!State->HasFreshBlocksInFlightUntil(commitId));

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Starting flush @%lu",
        TabletID(),
        commitId);

    State->SetFlushStatus(EOperationStatus::Started);

    auto blobs = [&] {
        TFlushBlocksVisitor visitor(
            State->GetBlockSize(),
            Config->GetMaxBlobRangeSize(),
            State->GetMaxBlocksInBlob(),
            commitId);

        State->FindFreshBlocks(visitor);

        return visitor.Finish();
    }();

    Y_ABORT_UNLESS(blobs);

    ui32 blobIndex = 0;
    for (auto& blob: blobs) {
        Y_ABORT_UNLESS(IsSorted(blob.Blocks.begin(), blob.Blocks.end()));

        blob.BlobId = State->GenerateBlobId(
            EChannelDataKind::Mixed,
            EChannelPermission::UserWritesAllowed,
            commitId,
            blob.BlobContent.Get().GetBytesCount(),
            blobIndex++);
    }

    auto actor = NCloud::Register<TFlushActor>(
        ctx,
        std::move(flushCtx.RequestInfo),
        BlockDigestGenerator,
        SelfId(),
        std::move(blobs));

    Actors.insert(actor);
}

void TPartitionActor::HandleFlushCompleted(
    const TEvPartitionPrivate::TEvFlushCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 commitId = State->GetFlushContext().CommitId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Flush completed @%lu",
        TabletID(),
        commitId);

    UpdateStats(msg->Stats);

    UpdateCPUUsageStat(ctx, msg->ExecCycles);

    const auto d = CyclesToDurationSafe(msg->TotalCycles);
    const auto ts = ctx.Now() - d;
    PartCounters->RequestCounters.Flush.AddRequest(d.MicroSeconds());

    State->ResetFlushContext();
    State->ReleaseCollectBarrier(commitId);
    State->SetFlushStatus(EOperationStatus::Idle);
    State->SetTrimFreshLogToCommitId(commitId);

    Actors.erase(ev->Sender);

    EnqueueFlushIfNeeded(ctx);
    EnqueueTrimFreshLogIfNeeded(ctx);

    {
        IProfileLog::TSysReadWriteRequest request;
        request.RequestType = ESysRequestType::Flush;
        request.Duration = d;
        request.Ranges = std::move(msg->AffectedRanges);

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ts;
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }

    LogBlockInfos(
        ctx,
        ESysRequestType::Flush,
        std::move(msg->AffectedBlockInfos),
        commitId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
