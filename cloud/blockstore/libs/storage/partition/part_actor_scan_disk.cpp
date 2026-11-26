#include "part_actor.h"

#include <cloud/blockstore/libs/storage/partition/model/blob_liveness.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/string/split.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using TBlobMark = TEvPartitionPrivate::TScanDiskReadBatchResponse::TBlobMark;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MaxUniqueId = std::numeric_limits<ui64>::max();

////////////////////////////////////////////////////////////////////////////////

TString StringifyScanDiskBatchTx(const TTxPartition::TScanDiskBatch& args)
{
    return TStringBuilder()
           << " StartBlobId: [" << args.StartBlobId.CommitId() << ':'
           << args.StartBlobId.UniqueId() << ']'
           << " BlobCountToVisit: " << args.BlobCountToVisit
           << " FinalBlobId: [" << args.FinalBlobId.CommitId() << ':'
           << args.FinalBlobId.UniqueId() << ']'
           << " VisitCount: " << args.VisitCount << " LastVisitedBlobId: ["
           << args.LastVisitedBlobId.CommitId() << ':'
           << args.LastVisitedBlobId.UniqueId() << ']'
           << " BlobsToReadInCurrentBatchSize: "
           << args.BlobsToReadInCurrentBatch.size()
           << " DeadBlobCount: " << args.DeadBlobCount
           << " ConfirmedBlobsSize: " << args.ConfirmedBlobs.size();
}

TString FormatScanDiskCursor(const TPartialBlobId& blobId)
{
    return TStringBuilder() << blobId.CommitId() << ':' << blobId.UniqueId();
}

bool TryParseScanDiskCursor(TStringBuf cursor, TPartialBlobId& blobId)
{
    ui64 commitId = 0;
    ui64 uniqueId = 0;
    if (!StringSplitter(cursor).Split(':').TryCollectInto(&commitId, &uniqueId))
    {
        return false;
    }

    blobId = MakePartialBlobId(commitId, uniqueId);
    return true;
}

bool HasBlockChecksums(const NProto::TBlobMeta& blobMeta)
{
    return AnyOf(
        blobMeta.GetBlockChecksums(),
        [](ui32 checksum) { return checksum != 0; });
}

TBlobMark MakeBlobMark(
    const TTabletStorageInfo& tabletInfo,
    const TPartialBlobId& blobId,
    bool hasChecksums)
{
    return {
        MakeBlobId(tabletInfo.TabletID, blobId),
        tabletInfo.GroupFor(blobId.Channel(), blobId.Generation()),
        hasChecksums};
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
        Y_UNUSED(blockMask);

        ++Args.VisitCount;
        Args.LastVisitedBlobId = MakePartialBlobId(commitId, blobId);

        if (!IsDeletionMarker(Args.LastVisitedBlobId)) {
            if (Args.BatchRequest) {
                Args.VisitedBlobs.push_back(
                    {Args.LastVisitedBlobId, HasBlockChecksums(blobMeta)});
                AppendBlobBlocks(blobMeta, Args.VisitedBlobBlocks);
            } else {
                Args.BlobsToReadInCurrentBatch.push_back(MakeBlobMark(
                    TabletInfo,
                    Args.LastVisitedBlobId,
                    false));   // hasChecksums
            }
        }

        return Args.VisitCount < Args.BlobCountToVisit;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBlockVersionsVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
    , public IMixedBlocksIndexVisitor
{
private:
    TVector<TBlockVersion>& Versions;

public:
    explicit TBlockVersionsVisitor(TVector<TBlockVersion>& versions)
        : Versions(versions)
    {}

    bool Visit(const TFreshBlock& block) override
    {
        Versions.push_back(
            {block.Meta.BlockIndex, block.Meta.CommitId, block.BlobId});
        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_UNUSED(blobOffset);
        Versions.push_back({blockIndex, commitId, blobId});
        return true;
    }

    bool VisitBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui8 compactionRangeCount) override
    {
        Y_UNUSED(blobOffset);
        Y_UNUSED(compactionRangeCount);
        Versions.push_back({blockIndex, commitId, blobId});
        return true;
    }
};

// Only saves checks, so it judges liveness at the current commit: a dead blob
// stays dead, and an unhealthy verdict is confirmed against the blob index
// anyway.
bool SelectLiveBlobs(
    TPartitionState& state,
    const TTabletStorageInfo& tabletInfo,
    TPartitionDatabase& db,
    TTxPartition::TScanDiskBatch& args)
{
    const ui64 readerCommitId = state.GetLastCommitId();

    Sort(args.VisitedBlobBlocks);

    TVector<TBlockRange32> ranges;
    TBlockRange32Builder rangeBuilder(ranges);
    for (ui32 blockIndex: args.VisitedBlobBlocks) {
        rangeBuilder.OnBlock(blockIndex);
    }

    TVector<TBlockVersion> versions;
    TBlockVersionsVisitor visitor(versions);
    TIndexOnlyBlobsVisitor blobsVisitor;

    bool ready = true;
    for (const auto& range: ranges) {
        state.FindFreshBlocks(visitor, range, readerCommitId);
        ready &= db.FindMixedBlocks(
            visitor,
            range,
            true   // precharge
        );
        ready &= db.FindMergedBlocks(
            visitor,
            blobsVisitor,
            range,
            true,   // precharge
            state.GetMaxBlocksInBlob());
    }

    if (!ready) {
        return false;
    }

    TVector<ui64> checkpointCommitIds;
    state.GetCheckpoints().GetCommitIds(checkpointCommitIds);
    state.GetCheckpointsInFlight()->GetCommitIds(checkpointCommitIds);

    const auto liveBlobs = FindLiveBlobs(
        std::move(versions),
        readerCommitId,
        std::move(checkpointCommitIds));

    for (const auto& blob: args.VisitedBlobs) {
        if (liveBlobs.contains(blob.BlobId)) {
            args.BlobsToReadInCurrentBatch.push_back(
                MakeBlobMark(tabletInfo, blob.BlobId, blob.HasChecksums));
        } else {
            ++args.DeadBlobCount;
        }
    }

    return true;
}

bool FindBlobsToConfirm(
    TPartitionDatabase& db,
    TTxPartition::TScanDiskBatch& args)
{
    bool ready = true;
    for (const auto& blobId: args.BatchRequest->BlobsToConfirm) {
        TMaybe<NProto::TBlobMeta> blobMeta;
        if (!db.ReadBlobMeta(blobId, blobMeta)) {
            ready = false;
        } else if (blobMeta) {
            args.ConfirmedBlobs.push_back(blobId);
        }
    }
    return ready;
}

std::unique_ptr<TEvVolume::TEvScanDiskBatchResponse> MakeScanDiskBatchResponse(
    ui64 tabletId,
    const TTxPartition::TScanDiskBatch& args)
{
    const auto& batchRequest = *args.BatchRequest;

    auto response = std::make_unique<TEvVolume::TEvScanDiskBatchResponse>();
    auto& record = response->Record;

    for (const auto& blobMark: args.BlobsToReadInCurrentBatch) {
        auto& blob = *record.AddBlobs();
        LogoBlobIDFromLogoBlobID(blobMark.BlobId, blob.MutableBlobId());
        blob.SetBSGroupId(blobMark.BSGroupId);
        blob.SetHasChecksums(blobMark.HasChecksums);
    }

    TPartialBlobId nextBlobId = args.StartBlobId;
    if (args.BlobCountToVisit) {
        const auto lastListedBlobId =
            args.ReachedFinalBlobId ? args.FinalBlobId : args.LastVisitedBlobId;
        nextBlobId = Max(nextBlobId, NextBlobId(lastListedBlobId, MaxUniqueId));
        record.SetIsCompleted(
            args.ReachedFinalBlobId &&
            !batchRequest.ListingBoundedByPendingCommits);
    }
    record.SetNextCursor(FormatScanDiskCursor(nextBlobId));

    record.SetDeadBlobs(args.DeadBlobCount);
    record.SetScanCommitId(batchRequest.ScanCommitId);

    for (const auto& blobId: args.ConfirmedBlobs) {
        LogoBlobIDFromLogoBlobID(
            MakeBlobId(tabletId, blobId),
            record.AddConfirmedBlobs());
    }

    return response;
}

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
    void SendScanDiskReadBatchRequest(const TActorContext& ctx);

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

    void HandleScanDiskReadBatchResponse(
        const TEvPartitionPrivate::TEvScanDiskReadBatchResponse::TPtr& ev,
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
    SendScanDiskReadBatchRequest(ctx);
    Become(&TThis::StateWork);
}

void TScanDiskActor::SendScanDiskReadBatchRequest(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvPartitionPrivate::TEvScanDiskReadBatchRequest>(
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
            TEvPartitionPrivate::TEvScanDiskReadBatchResponse,
            HandleScanDiskReadBatchResponse);
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

    SendScanDiskReadBatchRequest(ctx);
}

void TScanDiskActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto error = MakeError(E_REJECTED, "tablet is shutting down");

    NotifyCompleted(ctx, error);
}

void TScanDiskActor::HandleScanDiskReadBatchResponse(
    const TEvPartitionPrivate::TEvScanDiskReadBatchResponse::TPtr& ev,
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
        SendScanDiskReadBatchRequest(ctx);
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
        SendScanDiskReadBatchRequest(ctx);
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

void TPartitionActor::HandleScanDiskReadBatch(
    const TEvPartitionPrivate::TEvScanDiskReadBatchRequest::TPtr& ev,
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
            std::make_unique<TEvPartitionPrivate::TEvScanDiskReadBatchResponse>(
                MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "ScanDisk",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetCommitQueue()->GetMinCommitId() <= msg->BlobId.CommitId()) {
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

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start scan batch (starting %u:%u)",
        LogTitle.GetWithTime().c_str(),
        gen,
        step);

    AddTransaction<TEvPartitionPrivate::TScanDiskReadBatchMethod>(*requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TScanDiskBatch>(
            requestInfo,
            msg->BlobId,
            msg->Count,
            msg->FinalBlobId));
}

void TPartitionActor::HandleScanDiskBatch(
    const TEvVolume::TEvScanDiskBatchRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvVolume::TScanDiskBatchMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ScanDiskBatch",
        requestInfo->CallContext->RequestId);

    auto replyError = [&](NProto::TError error)
    {
        auto response = std::make_unique<TEvVolume::TEvScanDiskBatchResponse>(
            std::move(error));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "ScanDiskBatch",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    };

    const auto& record = msg->Record;

    auto startBlobId = MakePartialBlobId(record.GetMinCommitId(), 0);
    if (record.GetCursor() &&
        !TryParseScanDiskCursor(record.GetCursor(), startBlobId))
    {
        replyError(MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "invalid cursor: " << record.GetCursor().Quote()));
        return;
    }

    // An omitted ScanCommitId reads as 0, and a request written by hand
    // without it must not scan nothing.
    const ui64 requestedScanCommitId =
        record.GetScanCommitId() ? record.GetScanCommitId() : Max<ui64>();
    const ui64 scanCommitId =
        Min(requestedScanCommitId, State->GetLastCommitId());

    // Writes, flushes and compactions in flight add their blobs to the index
    // later. The cursor must not pass them, or the scan would miss the blobs.
    const ui64 minPendingCommitId = State->GetCommitQueue()->GetMinCommitId();
    const bool boundedByPendingCommits = minPendingCommitId <= scanCommitId;
    const ui64 listingCommitId =
        boundedByPendingCommits ? minPendingCommitId - 1 : scanCommitId;

    if (record.GetCount() && boundedByPendingCommits &&
        startBlobId.CommitId() > listingCommitId)
    {
        replyError(MakeError(E_REJECTED, "There are pending commits"));
        return;
    }

    TTxPartition::TScanDiskBatch::TBatchRequest batchRequest{
        .ScanCommitId = scanCommitId,
        .ListingBoundedByPendingCommits = boundedByPendingCommits,
    };
    for (const auto& protoBlobId: record.GetBlobsToConfirm()) {
        const auto blobId = LogoBlobIDFromLogoBlobID(protoBlobId);
        // Blobs of other tablets, of a base disk for example, can't be in
        // this blob index.
        if (blobId.TabletID() == TabletID()) {
            batchRequest.BlobsToConfirm.push_back(MakePartialBlobId(blobId));
        }
    }

    AddTransaction<TEvVolume::TScanDiskBatchMethod>(*requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TScanDiskBatch>(
            requestInfo,
            startBlobId,
            record.GetCount(),
            MakePartialBlobId(listingCommitId, MaxUniqueId),
            std::move(batchRequest)));
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

    using EProgress = TPartitionDatabase::EBlobIndexScanProgress;

    // A batch request with Count 0 lists nothing. The read-only mode always
    // lists.
    const bool listBlobs = !args.BatchRequest || args.BlobCountToVisit;

    auto progress = EProgress::Completed;
    bool ready = true;

    if (listBlobs && args.StartBlobId <= args.FinalBlobId) {
        TScanDiskVisitor visitor(*Info(), args);

        progress = db.FindBlocksInBlobsIndex(
            visitor,
            args.StartBlobId,
            args.FinalBlobId,
            args.BlobCountToVisit);
        ready = progress != EProgress::NotReady;
    }

    args.ReachedFinalBlobId = progress == EProgress::Completed &&
                              args.VisitCount < args.BlobCountToVisit;

    if (ready && args.BatchRequest) {
        ready = SelectLiveBlobs(*State, *Info(), db, args);
        ready &= FindBlobsToConfirm(db, args);
    }

    if (ready && !args.BatchRequest) {
        State->UpdateScanDiskBlobsToBeProcessed(args.VisitCount);
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s PrepareScanDiskBatch completed (%u) %s",
        LogTitle.GetWithTime().c_str(),
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

    if (args.BatchRequest) {
        LWTRACK(
            ResponseSent_Partition,
            args.RequestInfo->CallContext->LWOrbit,
            "ScanDiskBatch",
            args.RequestInfo->CallContext->RequestId);

        NCloud::Reply(
            ctx,
            *args.RequestInfo,
            MakeScanDiskBatchResponse(TabletID(), args));
        return;
    }

    const bool isScanCompleted = args.VisitCount == 0;

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvPartitionPrivate::TEvScanDiskReadBatchResponse>(
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
