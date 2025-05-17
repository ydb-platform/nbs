#include "part_actor.h"

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using TBlobMark = TEvPartitionPrivate::TScanDiskBatchResponse::TBlobMark;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MaxUniqueId = std::numeric_limits<ui64>::max();

////////////////////////////////////////////////////////////////////////////////

TString StringifyScanDiskBatchTx(const TTxPartition::TScanDiskBatch& args)
{
    return TStringBuilder()
        << " StartBlobId: ["
        <<  args.StartBlobId.CommitId() << ':' << args.StartBlobId.UniqueId() << ']'
        << " BlobCountToVisit: " << args.BlobCountToVisit
        << " FinalBlobId: ["
        << args.FinalBlobId.CommitId() << ':' << args.FinalBlobId.UniqueId() << ']'
        << " VisitCount: " << args.VisitCount
        << " LastVisitedBlobId: ["
        << args.LastVisitedBlobId.CommitId() << ':' << args.LastVisitedBlobId.UniqueId() << ']'
        << " BlobsToReadInCurrentBatchSize: " << args.BlobsToReadInCurrentBatch.size();
}

////////////////////////////////////////////////////////////////////////////////

class TScanDiskVisitor final
    : public IBlobsIndexVisitor
{
private:
    const TTabletStorageInfo& TabletInfo;
    TTxPartition::TScanDiskBatch& Args;

public:
    TScanDiskVisitor(
            const TTabletStorageInfo& tabletInfo,
            TTxPartition::TScanDiskBatch& args)
        : TabletInfo(tabletInfo)
        , Args(args)
    {}

public:
    bool Visit(
        ui64 commitId,
        ui64 blobId,
        const NProto::TBlobMeta& blobMeta,
        const TStringBuf blockMask) override
    {
        Y_UNUSED(blobMeta);
        Y_UNUSED(blockMask);

        ++Args.VisitCount;
        Args.LastVisitedBlobId = MakePartialBlobId(commitId, blobId);

        if (!IsDeletionMarker(Args.LastVisitedBlobId)) {
            const auto group = TabletInfo.GroupFor(
                Args.LastVisitedBlobId.Channel(),
                Args.LastVisitedBlobId.Generation());
            Args.BlobsToReadInCurrentBatch.emplace_back(
                MakeBlobId(TabletInfo.TabletID, Args.LastVisitedBlobId),
                group);
        }

        return Args.VisitCount < Args.BlobCountToVisit;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TScanDiskActor final
    : public TActorBootstrapped<TScanDiskActor>
{
private:
    const TActorId Tablet;
    const ui32 BlobsPerBatch = 0;
    const TPartialBlobId FinalBlobId;
    const TDuration RetryTimeout;

    TPartialBlobId BlobIdToRead;

    TVector<TBlobMark> RequestsInCurrentBatch;
    ui32 ReadBlobResponsesCounter = 0;

    TGuardedBuffer<TBlockBuffer> GuardedBuffer;

    TVector<TLogoBlobID> BrokenBlobs;

public:
    TScanDiskActor(
        const TActorId& tablet,
        ui32 blobsPerBatch,
        ui64 finalCommitId,
        TDuration retryTimeout,
        TBlockBuffer blockBuffer);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendScanDiskBatchRequest(const TActorContext& ctx);

    void NotifyCompleted(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void SendReadBlobRequest(const TActorContext& ctx, ui32 requestIndex);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleScanDiskBatchResponse(
        const TEvPartitionPrivate::TEvScanDiskBatchResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadBlobResponse(
        const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TScanDiskActor::TScanDiskActor(
        const TActorId& tablet,
        ui32 blobsPerBatch,
        ui64 finalCommitId,
        TDuration retryTimeout,
        TBlockBuffer blockBuffer)
    : Tablet(tablet)
    , BlobsPerBatch(blobsPerBatch)
    , FinalBlobId(MakePartialBlobId(finalCommitId, Max()))
    , RetryTimeout(retryTimeout)
    , GuardedBuffer(std::move(blockBuffer))
{}

void TScanDiskActor::Bootstrap(const TActorContext &ctx)
{
    SendScanDiskBatchRequest(ctx);
    Become(&TThis::StateWork);
}

void TScanDiskActor::SendScanDiskBatchRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvScanDiskBatchRequest>(
        MakeIntrusive<TCallContext>(),
        BlobIdToRead,
        BlobsPerBatch,
        FinalBlobId);

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TScanDiskActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    using TEvent = TEvPartitionPrivate::TEvScanDiskCompleted;
    auto response = std::make_unique<TEvent>(
            error,
            std::move(BrokenBlobs));

    NCloud::Send(ctx, Tablet, std::move(response));
    Die(ctx);
}

void TScanDiskActor::SendReadBlobRequest(
    const TActorContext& ctx,
    ui32 requestIndex)
{
    const auto& dstSglist = GuardedBuffer.Get().GetBlocks();
    TSgList subset{dstSglist[requestIndex]};
    auto subSgList = GuardedBuffer.CreateGuardedSgList(std::move(subset));

    const auto& blobMark = RequestsInCurrentBatch[requestIndex];
    TVector<ui16> blobOffsets{0};

    auto request = std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
        blobMark.BlobId,
        MakeBlobStorageProxyID(blobMark.BSGroupId),
        std::move(blobOffsets),
        std::move(subSgList),
        blobMark.BSGroupId,
        false,           // async
        TInstant::Max(), // deadline
        false            // shouldCalculateChecksums
    );

    NCloud::Send(
        ctx,
        Tablet,
        std::move(request),
        requestIndex);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TScanDiskActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvPartitionPrivate::TEvScanDiskBatchResponse,
            HandleScanDiskBatchResponse);
        HFunc(
            TEvPartitionCommonPrivate::TEvReadBlobResponse,
            HandleReadBlobResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TScanDiskActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    SendScanDiskBatchRequest(ctx);
}

void TScanDiskActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto error = MakeError(E_REJECTED, "tablet is shutting down");

    NotifyCompleted(ctx, error);
}

void TScanDiskActor::HandleScanDiskBatchResponse(
    const TEvPartitionPrivate::TEvScanDiskBatchResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        if (GetErrorKind(msg->Error) == EErrorKind::ErrorRetriable) {
            ctx.Schedule(RetryTimeout, new TEvents::TEvWakeup());
        } else {
            NotifyCompleted(ctx, msg->Error);
        }
        return;
    }

    if (msg->IsScanCompleted) {
        NotifyCompleted(ctx);
        return;
    }

    BlobIdToRead = NextBlobId(msg->LastVisitedBlobId, MaxUniqueId);

    if (msg->BlobsInBatch.empty()) {
        SendScanDiskBatchRequest(ctx);
        return;
    }
    ReadBlobResponsesCounter = 0;

    RequestsInCurrentBatch = std::move(msg->BlobsInBatch);
    for (ui32 requestIndex = 0; requestIndex < RequestsInCurrentBatch.size(); ++requestIndex) {
        SendReadBlobRequest(ctx, requestIndex);
    }
}

void TScanDiskActor::HandleReadBlobResponse(
    const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ++ReadBlobResponsesCounter;

    if (FAILED(msg->GetStatus())) {
        const ui32 requestIndex = ev->Cookie;
        BrokenBlobs.push_back(RequestsInCurrentBatch[requestIndex].BlobId);
    }

    if (ReadBlobResponsesCounter == RequestsInCurrentBatch.size()) {
        SendScanDiskBatchRequest(ctx);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo_ScanDisk(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    using namespace NMonitoringUtils;

    const auto& batchSizeParam = params.Get("BatchSize");
    ui32 batchSize = 0;
    NProto::TError result;
    if (batchSizeParam) {
        if (!TryFromString(batchSizeParam, batchSize)) {
            result = MakeError(E_ARGUMENT, "BatchSize value is not an integer");
        }
    }

    if (!HasError(result)) {
        result = DoHandleScanDisk(ctx, batchSize);
    }

    auto alertType = EAlertLevel::SUCCESS;
    if (HasError(result)) {
        alertType = EAlertLevel::DANGER;
    }

    SendHttpResponse(ctx, *requestInfo, result.GetMessage(), alertType);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleScanDisk(
    const TEvVolume::TEvScanDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError result = DoHandleScanDisk(ctx, msg->Record.GetBatchSize());

    auto response =
        std::make_unique<TEvVolume::TEvScanDiskResponse>(std::move(result));
    NCloud::Reply(ctx, *ev, std::move(response));
}

NProto::TError TPartitionActor::DoHandleScanDisk(
    const TActorContext& ctx,
    ui32 blobsPerBatch)
{
    if (State->IsScanDiskStarted()) {
        return MakeError(S_ALREADY, "Scan disk is already running");
    }

    if (!blobsPerBatch) {
        return MakeError(E_ARGUMENT, "Batch size is 0");
    }

    State->StartScanDisk();

    blobsPerBatch = Min(
        static_cast<ui64>(blobsPerBatch),
        State->GetMixedBlobsCount() + State->GetMergedBlobsCount());

    const auto actorId = NCloud::Register(
        ctx,
        CreateScanDiskActor(
            SelfId(),
            blobsPerBatch,
            State->GetLastCommitId(),
            Config->GetCompactionRetryTimeout(),
            CreateScanDiskBlockBuffer(blobsPerBatch)));

    Actors.Insert(actorId);

    return MakeError(S_OK, "Scan disk has been started");
}

void TPartitionActor::HandleGetScanDiskStatus(
    const TEvVolume::TEvGetScanDiskStatusRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvVolume::TEvGetScanDiskStatusResponse>();
    NProto::TError result;

    if (State) {
        if (!State->HasScanDiskProgress()) {
            result = MakeError(E_NOT_FOUND, "No operation found");
        } else {
            auto& progress = *response->Record.MutableProgress();
            const auto p = State->GetScanDiskProgress();

            progress.SetProcessed(p.ProcessedBlobs);
            progress.SetTotal(p.TotalBlobs);
            progress.SetIsCompleted(p.IsCompleted);

            for (size_t i = 0; i < p.BrokenBlobs.size(); ++i) {
                auto& brokenBlob = *progress.AddBrokenBlobs();
                brokenBlob.SetRawX1(p.BrokenBlobs[i].GetRaw()[0]);
                brokenBlob.SetRawX2(p.BrokenBlobs[i].GetRaw()[1]);
                brokenBlob.SetRawX3(p.BrokenBlobs[i].GetRaw()[2]);
            }
        }
    } else {
        result = MakeError(E_REJECTED, "tablet is shutting down");
    };

    *response->Record.MutableError() = std::move(result);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TPartitionActor::HandleScanDiskBatch(
    const TEvPartitionPrivate::TEvScanDiskBatchRequest::TPtr& ev,
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
        "ScanDisk",
        requestInfo->CallContext->RequestId);

    State->UpdateScanDiskProgress();

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response =
            std::make_unique<TEvPartitionPrivate::TEvScanDiskBatchResponse>(
                MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "ScanDisk",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetCommitQueue().GetMinCommitId() <= msg->BlobId.CommitId()) {
        replyError(
            ctx,
            *requestInfo,
            E_REJECTED,
            "There are pending write commits");
        return;
    }

    if (State->GetCleanupState().Status == EOperationStatus::Started) {
        replyError(ctx, *requestInfo, E_REJECTED, "Cleanup is running");
        return;
    }

    const auto [gen, step] = ParseCommitId(msg->BlobId.CommitId());

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Start scan batch (starting %u:%u)",
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
        gen,
        step);

    AddTransaction<TEvPartitionPrivate::TScanDiskBatchMethod>(
        *requestInfo,
        ETransactionType::ScanDiskBatch);

    ExecuteTx(ctx, CreateTx<TScanDiskBatch>(
        requestInfo,
        msg->BlobId,
        msg->Count,
        msg->FinalBlobId));
}

void TPartitionActor::HandleScanDiskCompleted(
    const TEvPartitionPrivate::TEvScanDiskCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    State->SetBrokenBlobs(std::move(msg->BrokenBlobs));

    if (State && State->IsScanDiskStarted()) {
        State->CompleteScanDisk();
        EnqueueCleanupIfNeeded(ctx);
    }

    Actors.Erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareScanDiskBatch(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TScanDiskBatch& args)
{
    TRequestScope timer(*args.RequestInfo);

    TPartitionDatabase db(tx.DB);

    TScanDiskVisitor visitor(*Info(), args);

    const auto progress = db.FindBlocksInBlobsIndex(
        visitor,
        args.StartBlobId,
        args.FinalBlobId,
        args.BlobCountToVisit);
    const auto ready =
        progress != TPartitionDatabase::EBlobIndexScanProgress::NotReady;

    if (ready) {
        State->UpdateScanDiskBlobsToBeProcessed(args.VisitCount);
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] PrepareScanDiskBatch completed (%u) %s",
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
        static_cast<ui32>(progress),
        StringifyScanDiskBatchTx(args).c_str());

    return ready;
}

void TPartitionActor::ExecuteScanDiskBatch(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TScanDiskBatch& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteScanDiskBatch(
    const TActorContext& ctx,
    TTxPartition::TScanDiskBatch& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());

    const bool isScanCompleted = args.VisitCount == 0;

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvPartitionPrivate::TEvScanDiskBatchResponse>(
            std::move(args.BlobsToReadInCurrentBatch),
            args.LastVisitedBlobId,
            isScanCompleted));
}

////////////////////////////////////////////////////////////////////////////////

TBlockBuffer TPartitionActor::CreateScanDiskBlockBuffer(ui32 blobsPerBatch)
{
    TBlockBuffer blobBuffer(TProfilingAllocator::Instance());

    for (ui32 i = 0; i < blobsPerBatch; ++i) {
        blobBuffer.AddBlock(State->GetBlockSize(), 1);
    }

    return blobBuffer;
}

IActorPtr TPartitionActor::CreateScanDiskActor(
    TActorId tablet,
    ui64 blobsPerBatch,
    ui64 finalCommitId,
    TDuration retryTimeout,
    TBlockBuffer blockBuffer)
{
    return std::make_unique<TScanDiskActor>(
        std::move(tablet),
        blobsPerBatch,
        finalCommitId,
        retryTimeout,
        std::move(blockBuffer));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
