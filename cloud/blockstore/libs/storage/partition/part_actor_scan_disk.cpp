#include "part_actor.h"

#include <cloud/blockstore/libs/storage/partition/model/blob_liveness.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using TBlobMark = TEvPartitionPrivate::TScanDiskBatchResponse::TBlobMark;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MaxUniqueId = std::numeric_limits<ui64>::max();

constexpr ui32 DefaultMaxInFlight = 4;
constexpr auto DefaultCheckTimeout = TDuration::Seconds(60);
constexpr ui32 DefaultMaxRetries = 5;
constexpr auto DefaultRetryTimeout = TDuration::Seconds(1);
constexpr ui32 DefaultMaxReportedBlobs = 10000;
constexpr ui32 DefaultMaxDataInfoSize = 1024;

constexpr auto MaxCheckRetryDelay = TDuration::Minutes(1);

constexpr ui64 RetryCheckWakeupTag = 1;

////////////////////////////////////////////////////////////////////////////////

ui32 ValueOrDefault(ui32 value, ui32 defaultValue)
{
    return value ? value : defaultValue;
}

TDuration MillisecondsOrDefault(ui32 milliseconds, TDuration defaultValue)
{
    return milliseconds ? TDuration::MilliSeconds(milliseconds) : defaultValue;
}

TScanDiskSettings MakeScanDiskSettings(const NProto::TScanDiskRequest& request)
{
    return {
        .BatchSize = request.GetBatchSize(),
        .CheckIntegrity = request.GetCheckIntegrity(),
        .MaxInFlight =
            ValueOrDefault(request.GetMaxInFlight(), DefaultMaxInFlight),
        .CheckTimeout = MillisecondsOrDefault(
            request.GetCheckTimeoutMs(),
            DefaultCheckTimeout),
        .MaxRetries =
            ValueOrDefault(request.GetMaxRetries(), DefaultMaxRetries),
        .RetryTimeout = MillisecondsOrDefault(
            request.GetRetryTimeoutMs(),
            DefaultRetryTimeout),
        .MaxReportedBlobs = ValueOrDefault(
            request.GetMaxReportedBlobs(),
            DefaultMaxReportedBlobs),
        .MaxDataInfoSize = ValueOrDefault(
            request.GetMaxDataInfoSize(),
            DefaultMaxDataInfoSize),
    };
}

////////////////////////////////////////////////////////////////////////////////

NProto::TScanDiskBlobResult::EVerdict ToProtoVerdict(
    EScanDiskBlobVerdict verdict)
{
    switch (verdict) {
        case EScanDiskBlobVerdict::Broken:
            return NProto::TScanDiskBlobResult::BROKEN;
        case EScanDiskBlobVerdict::Degraded:
            return NProto::TScanDiskBlobResult::DEGRADED;
        case EScanDiskBlobVerdict::Unchecked:
            return NProto::TScanDiskBlobResult::UNCHECKED;
    }
}

void FillScanDiskProgress(
    const TScanDiskProgress& progress,
    NProto::TScanDiskProgress& proto)
{
    const auto& results = progress.Results;

    proto.SetProcessed(progress.ProcessedBlobs);
    proto.SetTotal(progress.TotalBlobs);
    proto.SetIsCompleted(progress.IsCompleted);

    for (const auto& blobId: results.BrokenBlobs) {
        LogoBlobIDFromLogoBlobID(blobId, proto.AddBrokenBlobs());
    }

    if (!progress.CheckIntegrity) {
        proto.SetCancelled(progress.Cancelled);
        return;
    }

    proto.SetCheckIntegrity(true);
    proto.SetScanCommitId(progress.ScanCommitId);
    proto.SetDeadBlobs(results.DeadBlobs);
    proto.SetCheckedBlobs(results.CheckedBlobs);
    proto.SetReplicatingBlobs(results.ReplicatingBlobs);
    proto.SetDegradedBlobs(results.DegradedBlobs);
    proto.SetUncheckedBlobs(results.UncheckedBlobs);
    proto.SetDroppedBlobResults(results.DroppedBlobResults);
    proto.SetCancelled(progress.Cancelled);

    for (const auto& blobResult: results.BlobResults) {
        auto& protoResult = *proto.AddBlobResults();
        LogoBlobIDFromLogoBlobID(
            blobResult.BlobId,
            protoResult.MutableBlobId());
        protoResult.SetBSGroupId(blobResult.BSGroupId);
        protoResult.SetVerdict(ToProtoVerdict(blobResult.Verdict));
        protoResult.SetStatus(blobResult.Status);
        protoResult.SetPlacementStatus(blobResult.PlacementStatus);
        protoResult.SetDataStatus(blobResult.DataStatus);
        protoResult.SetErrorReason(blobResult.ErrorReason);
        protoResult.SetDataInfo(blobResult.DataInfo);
    }
}

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
           << " DeadBlobCount: " << args.DeadBlobCount;
}

TBlobMark MakeBlobMark(
    const TTabletStorageInfo& tabletInfo,
    const TPartialBlobId& blobId)
{
    return {
        MakeBlobId(tabletInfo.TabletID, blobId),
        tabletInfo.GroupFor(blobId.Channel(), blobId.Generation())};
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
            if (Args.SkipDeadBlobs) {
                Args.VisitedBlobs.push_back({Args.LastVisitedBlobId, blobMeta});
            } else {
                Args.BlobsToReadInCurrentBatch.push_back(
                    MakeBlobMark(TabletInfo, Args.LastVisitedBlobId));
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

// Runs the index lookups readers use over every block of the visited blobs.
// Writes in flight are not in the index yet, so they can only make a dead
// blob look live.
bool SelectLiveBlobs(
    TPartitionState& state,
    const TTabletStorageInfo& tabletInfo,
    TPartitionDatabase& db,
    TTxPartition::TScanDiskBatch& args)
{
    TVector<ui32> blocks;
    for (const auto& blob: args.VisitedBlobs) {
        AppendBlobBlocks(blob.BlobMeta, blocks);
    }
    SortUnique(blocks);

    TVector<TBlockVersion> versions;
    if (!blocks.empty()) {
        TBlockVersionsVisitor visitor(versions);

        for (const auto& range: GroupContiguousBlocks(blocks)) {
            state.FindFreshBlocks(visitor, range, Max<ui64>());
        }

        bool ready = db.FindMixedBlocks(visitor, blocks, Max<ui64>());
        ready &= db.FindMergedBlocks(
            visitor,
            blocks,
            state.GetMaxBlocksInBlob(),
            Max<ui64>());

        if (!ready) {
            return false;
        }
    }

    TVector<ui64> checkpointCommitIds;
    state.GetCheckpoints().GetCommitIds(checkpointCommitIds);
    state.GetCheckpointsInFlight()->GetCommitIds(checkpointCommitIds);

    const auto liveBlobs =
        FindLiveBlobs(std::move(versions), std::move(checkpointCommitIds));

    for (const auto& blob: args.VisitedBlobs) {
        if (liveBlobs.contains(blob.BlobId)) {
            args.BlobsToReadInCurrentBatch.push_back(
                MakeBlobMark(tabletInfo, blob.BlobId));
        } else {
            ++args.DeadBlobCount;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

enum class ECheckVerdict
{
    Healthy,
    Replicating,
    Degraded,
    Broken,
    NoVerdict,
};

ECheckVerdict ClassifyCheckIntegrityResult(
    const TEvBlobStorage::TEvCheckIntegrityResult& result)
{
    using TResult = TEvBlobStorage::TEvCheckIntegrityResult;

    // Error replies may keep the default PS_OK and DS_OK.
    if (result.Status != NKikimrProto::OK) {
        return ECheckVerdict::NoVerdict;
    }

    if (result.PlacementStatus == TResult::PS_BLOB_IS_LOST ||
        result.DataStatus == TResult::DS_ERROR)
    {
        return ECheckVerdict::Broken;
    }

    if (result.DataStatus == TResult::DS_UNKNOWN) {
        return ECheckVerdict::NoVerdict;
    }

    switch (result.PlacementStatus) {
        case TResult::PS_OK:
            return ECheckVerdict::Healthy;
        case TResult::PS_REPLICATION_IN_PROGRESS:
            return ECheckVerdict::Replicating;
        case TResult::PS_BLOB_IS_RECOVERABLE:
            return ECheckVerdict::Degraded;
        default:
            return ECheckVerdict::NoVerdict;
    }
}

TScanDiskBlobResult MakeBlobResult(
    const TEvBlobStorage::TEvCheckIntegrityResult& result,
    ui32 maxDataInfoSize)
{
    using TResult = TEvBlobStorage::TEvCheckIntegrityResult;

    return {
        .Status = TString(NKikimrProto::EReplyStatus_Name(result.Status)),
        .PlacementStatus =
            TResult::PlacementStatusToString(result.PlacementStatus),
        .DataStatus = TResult::DataStatusToString(result.DataStatus),
        .ErrorReason = result.ErrorReason,
        .DataInfo = result.DataInfo.substr(0, maxDataInfoSize),
    };
}

TScanDiskBlobResult MakeUndeliveredBlobResult()
{
    return {
        .Status = TString(NKikimrProto::EReplyStatus_Name(NKikimrProto::ERROR)),
        .ErrorReason = "CheckIntegrity was not delivered to the group proxy",
    };
}

TStringBuf DescribeVerdict(EScanDiskBlobVerdict verdict)
{
    switch (verdict) {
        case EScanDiskBlobVerdict::Broken:
            return "broken";
        case EScanDiskBlobVerdict::Degraded:
            return "degraded";
        case EScanDiskBlobVerdict::Unchecked:
            return "unchecked";
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TBlobCheck
{
    ui32 Attempts = 0;
    TBackoffDelayProvider RetryDelay;
};

////////////////////////////////////////////////////////////////////////////////

class TScanDiskActor final
    : public TActorBootstrapped<TScanDiskActor>
{
private:
    const TActorId Tablet;
    const TScanDiskSettings Settings;
    const ui32 BlobsPerBatch = 0;
    const TPartialBlobId FinalBlobId;
    const TDuration RetryTimeout;

    TPartialBlobId BlobIdToRead;

    TVector<TBlobMark> RequestsInCurrentBatch;
    ui32 ReadBlobResponsesCounter = 0;

    TGuardedBuffer<TBlockBuffer> GuardedBuffer;

    TVector<TBlobCheck> Checks;
    TDeque<ui32> ChecksToSend;
    ui32 ChecksInFlight = 0;
    ui32 FinishedChecks = 0;

    TScanDiskResults Results;

public:
    TScanDiskActor(
        const TActorId& tablet,
        const TScanDiskSettings& settings,
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

    void StartChecks(const TActorContext& ctx);
    void SendChecks(const TActorContext& ctx);
    void SendCheck(const TActorContext& ctx, ui32 requestIndex);

    void OnCheckReply(
        const TActorContext& ctx,
        ui32 requestIndex,
        ECheckVerdict verdict,
        TScanDiskBlobResult blobResult);

    void FinishCheck(
        const TActorContext& ctx,
        ui32 requestIndex,
        ECheckVerdict verdict,
        TScanDiskBlobResult blobResult);

    void ReportBlob(
        const TActorContext& ctx,
        ui32 requestIndex,
        EScanDiskBlobVerdict verdict,
        TScanDiskBlobResult blobResult);

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

    void HandleCheckIntegrityResult(
        const TEvBlobStorage::TEvCheckIntegrityResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleUndelivered(
        const TEvents::TEvUndelivered::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TScanDiskActor::TScanDiskActor(
    const TActorId& tablet,
    const TScanDiskSettings& settings,
    ui32 blobsPerBatch,
    ui64 finalCommitId,
    TDuration retryTimeout,
    TBlockBuffer blockBuffer)
    : Tablet(tablet)
    , Settings(settings)
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
    auto response = std::make_unique<TEvent>(error, std::move(Results));

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

void TScanDiskActor::StartChecks(const TActorContext& ctx)
{
    const ui32 blobCount = RequestsInCurrentBatch.size();

    Checks.clear();
    Checks.reserve(blobCount);
    ChecksToSend.clear();
    for (ui32 requestIndex = 0; requestIndex < blobCount; ++requestIndex) {
        Checks.push_back(
            {.RetryDelay = TBackoffDelayProvider(
                 Settings.RetryTimeout,
                 MaxCheckRetryDelay)});
        ChecksToSend.push_back(requestIndex);
    }
    FinishedChecks = 0;

    SendChecks(ctx);
}

void TScanDiskActor::SendChecks(const TActorContext& ctx)
{
    while (ChecksInFlight < Settings.MaxInFlight && !ChecksToSend.empty()) {
        const ui32 requestIndex = ChecksToSend.front();
        ChecksToSend.pop_front();
        SendCheck(ctx, requestIndex);
    }
}

void TScanDiskActor::SendCheck(const TActorContext& ctx, ui32 requestIndex)
{
    const auto& blobMark = RequestsInCurrentBatch[requestIndex];

    ++Checks[requestIndex].Attempts;
    ++ChecksInFlight;

    auto request = std::make_unique<TEvBlobStorage::TEvCheckIntegrity>(
        blobMark.BlobId,
        ctx.Now() + Settings.CheckTimeout,
        NKikimrBlobStorage::LowRead);

    SendToBSProxy(
        ctx,
        MakeBlobStorageProxyID(blobMark.BSGroupId),
        request.release(),
        requestIndex);
}

void TScanDiskActor::OnCheckReply(
    const TActorContext& ctx,
    ui32 requestIndex,
    ECheckVerdict verdict,
    TScanDiskBlobResult blobResult)
{
    Y_DEBUG_ABORT_UNLESS(ChecksInFlight > 0);
    --ChecksInFlight;

    auto& check = Checks[requestIndex];
    if (verdict == ECheckVerdict::NoVerdict &&
        check.Attempts <= Settings.MaxRetries)
    {
        ctx.Schedule(
            check.RetryDelay.GetDelayAndIncrease(),
            std::make_unique<IEventHandle>(
                SelfId(),
                SelfId(),
                new TEvents::TEvWakeup(RetryCheckWakeupTag),
                0,   // flags
                requestIndex));
    } else {
        FinishCheck(ctx, requestIndex, verdict, std::move(blobResult));
    }

    SendChecks(ctx);

    if (FinishedChecks == RequestsInCurrentBatch.size()) {
        SendScanDiskBatchRequest(ctx);
    }
}

void TScanDiskActor::FinishCheck(
    const TActorContext& ctx,
    ui32 requestIndex,
    ECheckVerdict verdict,
    TScanDiskBlobResult blobResult)
{
    ++FinishedChecks;

    switch (verdict) {
        case ECheckVerdict::Healthy:
            ++Results.CheckedBlobs;
            break;
        case ECheckVerdict::Replicating:
            ++Results.CheckedBlobs;
            ++Results.ReplicatingBlobs;
            break;
        case ECheckVerdict::Degraded:
            ++Results.CheckedBlobs;
            ++Results.DegradedBlobs;
            ReportBlob(
                ctx,
                requestIndex,
                EScanDiskBlobVerdict::Degraded,
                std::move(blobResult));
            break;
        case ECheckVerdict::Broken:
            ++Results.CheckedBlobs;
            Results.BrokenBlobs.push_back(
                RequestsInCurrentBatch[requestIndex].BlobId);
            ReportBlob(
                ctx,
                requestIndex,
                EScanDiskBlobVerdict::Broken,
                std::move(blobResult));
            break;
        case ECheckVerdict::NoVerdict:
            ++Results.UncheckedBlobs;
            ReportBlob(
                ctx,
                requestIndex,
                EScanDiskBlobVerdict::Unchecked,
                std::move(blobResult));
            break;
    }
}

void TScanDiskActor::ReportBlob(
    const TActorContext& ctx,
    ui32 requestIndex,
    EScanDiskBlobVerdict verdict,
    TScanDiskBlobResult blobResult)
{
    const auto& blobMark = RequestsInCurrentBatch[requestIndex];

    // One dead disk makes every blob it held degraded, so only broken blobs
    // are warnings.
    LOG_LOG(
        ctx,
        verdict == EScanDiskBlobVerdict::Broken ? NLog::PRI_WARN
                                                : NLog::PRI_INFO,
        TBlockStoreComponents::PARTITION_WORKER,
        "ScanDisk: blob %s in group %u is %s: Status# %s PlacementStatus# %s "
        "DataStatus# %s ErrorReason# %s",
        blobMark.BlobId.ToString().c_str(),
        blobMark.BSGroupId,
        TString(DescribeVerdict(verdict)).c_str(),
        blobResult.Status.c_str(),
        blobResult.PlacementStatus.c_str(),
        blobResult.DataStatus.c_str(),
        blobResult.ErrorReason.c_str());

    if (Results.BlobResults.size() >= Settings.MaxReportedBlobs) {
        ++Results.DroppedBlobResults;
        return;
    }

    blobResult.BlobId = blobMark.BlobId;
    blobResult.BSGroupId = blobMark.BSGroupId;
    blobResult.Verdict = verdict;
    Results.BlobResults.push_back(std::move(blobResult));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TScanDiskActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvUndelivered, HandleUndelivered);
        HFunc(
            TEvPartitionPrivate::TEvScanDiskBatchResponse,
            HandleScanDiskBatchResponse);
        HFunc(
            TEvPartitionCommonPrivate::TEvReadBlobResponse,
            HandleReadBlobResponse);
        HFunc(
            TEvBlobStorage::TEvCheckIntegrityResult,
            HandleCheckIntegrityResult);

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
    if (ev->Get()->Tag == RetryCheckWakeupTag) {
        ChecksToSend.push_back(ev->Cookie);
        SendChecks(ctx);
        return;
    }

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
    auto* msg = ev->Get();

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
    Results.DeadBlobs += msg->DeadBlobCount;

    if (msg->BlobsInBatch.empty()) {
        SendScanDiskBatchRequest(ctx);
        return;
    }

    RequestsInCurrentBatch = std::move(msg->BlobsInBatch);

    if (Settings.CheckIntegrity) {
        StartChecks(ctx);
        return;
    }

    ReadBlobResponsesCounter = 0;
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
        Results.BrokenBlobs.push_back(
            RequestsInCurrentBatch[requestIndex].BlobId);
    }

    if (ReadBlobResponsesCounter == RequestsInCurrentBatch.size()) {
        SendScanDiskBatchRequest(ctx);
    }
}

void TScanDiskActor::HandleCheckIntegrityResult(
    const TEvBlobStorage::TEvCheckIntegrityResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    OnCheckReply(
        ctx,
        ev->Cookie,
        ClassifyCheckIntegrityResult(*msg),
        MakeBlobResult(*msg, Settings.MaxDataInfoSize));
}

void TScanDiskActor::HandleUndelivered(
    const TEvents::TEvUndelivered::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Get()->SourceType != TEvBlobStorage::EvCheckIntegrity) {
        return;
    }

    OnCheckReply(
        ctx,
        ev->Cookie,
        ECheckVerdict::NoVerdict,
        MakeUndeliveredBlobResult());
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
        result =
            DoHandleScanDisk(ctx, TScanDiskSettings{.BatchSize = batchSize});
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
    const auto& record = ev->Get()->Record;

    auto result = record.GetCancel()
                      ? CancelScanDisk(ctx)
                      : DoHandleScanDisk(ctx, MakeScanDiskSettings(record));

    auto response =
        std::make_unique<TEvVolume::TEvScanDiskResponse>(std::move(result));
    NCloud::Reply(ctx, *ev, std::move(response));
}

NProto::TError TPartitionActor::DoHandleScanDisk(
    const TActorContext& ctx,
    const TScanDiskSettings& settings)
{
    if (State->IsScanDiskStarted()) {
        return MakeError(S_ALREADY, "Scan disk is already running");
    }

    if (!settings.BatchSize) {
        return MakeError(E_ARGUMENT, "Batch size is 0");
    }

    const ui64 scanCommitId = State->GetLastCommitId();

    State->StartScanDisk(settings.CheckIntegrity, scanCommitId);

    // Keeps every blob live at or after scan start until its check is done,
    // while cleanup goes on deleting blobs that died earlier.
    if (settings.CheckIntegrity) {
        State->GetCleanupQueue().AcquireBarrier(scanCommitId);
    }

    const ui32 blobsPerBatch =
        Min(static_cast<ui64>(settings.BatchSize),
            State->GetMixedBlobsCount() + State->GetMergedBlobsCount());

    const ui32 readBufferBlockCount =
        settings.CheckIntegrity ? 0 : blobsPerBatch;

    ScanDiskActorId = NCloud::Register(
        ctx,
        CreateScanDiskActor(
            SelfId(),
            settings,
            blobsPerBatch,
            scanCommitId,
            Config->GetCompactionRetryTimeout(),
            CreateScanDiskBlockBuffer(readBufferBlockCount)));

    Actors.Insert(ScanDiskActorId);

    return MakeError(S_OK, "Scan disk has been started");
}

NProto::TError TPartitionActor::CancelScanDisk(const TActorContext& ctx)
{
    if (!State->IsScanDiskStarted()) {
        return MakeError(S_FALSE, "Scan disk is not running");
    }

    if (State->IsScanDiskCancelRequested()) {
        return MakeError(S_ALREADY, "Scan disk is already being cancelled");
    }

    State->RequestScanDiskCancel();
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, ScanDiskActorId);

    return MakeError(S_OK, "Scan disk is being cancelled");
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
            const auto& progress = State->GetScanDiskProgress();
            FillScanDiskProgress(progress, *response->Record.MutableProgress());

            // A scan that stopped on an error must not look complete.
            if (HasError(progress.Error)) {
                result = progress.Error;
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

    if (State->GetCommitQueue()->GetMinCommitId() <= msg->BlobId.CommitId()) {
        replyError(
            ctx,
            *requestInfo,
            E_REJECTED,
            "There are pending write commits");
        return;
    }

    const bool skipDeadBlobs = State->IsScanDiskStarted() &&
                               State->GetScanDiskProgress().CheckIntegrity;

    // Without the liveness filter a running cleanup could delete a blob
    // between the batch and its read.
    if (!skipDeadBlobs &&
        State->GetCleanupState().Status == EOperationStatus::Started)
    {
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

    AddTransaction<TEvPartitionPrivate::TScanDiskBatchMethod>(*requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TScanDiskBatch>(
            requestInfo,
            msg->BlobId,
            msg->Count,
            msg->FinalBlobId,
            skipDeadBlobs));
}

void TPartitionActor::HandleScanDiskCompleted(
    const TEvPartitionPrivate::TEvScanDiskCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    Actors.Erase(ev->Sender);

    if (!State->IsScanDiskStarted()) {
        return;
    }

    ScanDiskActorId = {};

    const auto& progress = State->GetScanDiskProgress();
    if (progress.CheckIntegrity) {
        State->GetCleanupQueue().ReleaseBarrier(progress.ScanCommitId);
    }

    // Cancel poisons the scan actor, which reports that as an error.
    auto error = State->IsScanDiskCancelRequested() ? NProto::TError()
                                                    : std::move(msg->Error);

    State->CompleteScanDisk(std::move(msg->Results), std::move(error));
    EnqueueCleanupIfNeeded(ctx);
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
    bool ready =
        progress != TPartitionDatabase::EBlobIndexScanProgress::NotReady;

    if (ready && args.SkipDeadBlobs) {
        ready = SelectLiveBlobs(*State, *Info(), db, args);
    }

    if (ready) {
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

    const bool isScanCompleted = args.VisitCount == 0;

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvPartitionPrivate::TEvScanDiskBatchResponse>(
            std::move(args.BlobsToReadInCurrentBatch),
            args.LastVisitedBlobId,
            isScanCompleted,
            args.DeadBlobCount));
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
    const TScanDiskSettings& settings,
    ui64 blobsPerBatch,
    ui64 finalCommitId,
    TDuration retryTimeout,
    TBlockBuffer blockBuffer)
{
    return std::make_unique<TScanDiskActor>(
        std::move(tablet),
        settings,
        blobsPerBatch,
        finalCommitId,
        retryTimeout,
        std::move(blockBuffer));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
