#include "scan_disk_batch_actor.h"

#include "service_events_private.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/logoblob.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash_set.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultScanDiskBatchMaxInFlight = 4;
constexpr auto DefaultScanDiskBatchDeadline = TDuration::Seconds(25);
constexpr ui32 DefaultMaxDataInfoSize = 1024;

using TBlobResult = NPrivateProto::TScanDiskBlobResult;
using EVerdict = TBlobResult::EVerdict;

////////////////////////////////////////////////////////////////////////////////

EVerdict ClassifyCheckIntegrityResult(
    const TEvBlobStorage::TEvCheckIntegrityResult& result)
{
    using TResult = TEvBlobStorage::TEvCheckIntegrityResult;

    // Error replies may keep the default PS_OK and DS_OK.
    if (result.Status != NKikimrProto::OK) {
        return TBlobResult::UNCHECKED;
    }

    if (result.PlacementStatus == TResult::PS_BLOB_IS_LOST ||
        result.DataStatus == TResult::DS_ERROR)
    {
        return TBlobResult::BROKEN;
    }

    if (result.DataStatus == TResult::DS_UNKNOWN) {
        return TBlobResult::UNCHECKED;
    }

    switch (result.PlacementStatus) {
        case TResult::PS_OK:
            return TBlobResult::HEALTHY;
        case TResult::PS_REPLICATION_IN_PROGRESS:
            return TBlobResult::REPLICATING;
        case TResult::PS_BLOB_IS_RECOVERABLE:
            return TBlobResult::DEGRADED;
        default:
            return TBlobResult::UNCHECKED;
    }
}

void FillCheckIntegrityResult(
    const TEvBlobStorage::TEvCheckIntegrityResult& result,
    ui32 maxDataInfoSize,
    TBlobResult& blobResult)
{
    using TResult = TEvBlobStorage::TEvCheckIntegrityResult;

    blobResult.SetStatus(NKikimrProto::EReplyStatus_Name(result.Status));
    blobResult.SetPlacementStatus(
        TResult::PlacementStatusToString(result.PlacementStatus));
    blobResult.SetDataStatus(TResult::DataStatusToString(result.DataStatus));
    blobResult.SetErrorReason(result.ErrorReason);
    blobResult.SetDataInfo(result.DataInfo.substr(0, maxDataInfoSize));
}

void CopyLogoBlobId(const TLogoBlobID& src, NPrivateProto::TLogoBlobID& dst)
{
    dst.SetRawX1(src.GetRaw()[0]);
    dst.SetRawX2(src.GetRaw()[1]);
    dst.SetRawX3(src.GetRaw()[2]);
}

TLogoBlobID MakeLogoBlobId(const NPrivateProto::TLogoBlobID& blobId)
{
    return TLogoBlobID(blobId.GetRawX1(), blobId.GetRawX2(), blobId.GetRawX3());
}

bool IsReadable(EVerdict verdict)
{
    return verdict == TBlobResult::HEALTHY ||
           verdict == TBlobResult::REPLICATING ||
           verdict == TBlobResult::DEGRADED;
}

bool NeedsConfirmation(EVerdict verdict)
{
    return verdict != TBlobResult::HEALTHY &&
           verdict != TBlobResult::REPLICATING;
}

bool IsReported(EVerdict verdict)
{
    return verdict == TBlobResult::DEGRADED || verdict == TBlobResult::BROKEN ||
           verdict == TBlobResult::CHECKSUM_MISMATCH;
}

////////////////////////////////////////////////////////////////////////////////

struct TBlobCheck
{
    TLogoBlobID BlobId;
    ui32 BSGroupId = 0;
    bool HasChecksums = false;

    bool CheckSent = false;
    bool CheckReplied = false;
    // The verdict of the integrity check, kept when the checksums change the
    // final verdict.
    EVerdict IntegrityVerdict = TBlobResult::UNCHECKED;
    bool ChecksumsVerified = false;
    // The disk itself deleted the blob, so its result is dropped.
    bool Deleted = false;

    TBlobResult Result;
};

////////////////////////////////////////////////////////////////////////////////

// Serves one batch of a scan: lists the batch, checks it and confirms the
// unhealthy verdicts. Keeps nothing afterwards: the scan actor owns the
// cursors, the retries and the results.
class TScanDiskBatchActor final: public TActorBootstrapped<TScanDiskBatchActor>
{
private:
    enum EPhase
    {
        Listing,
        Checking,
        VerifyingChecksums,
        Confirming,
    };

    const TActorId Owner;
    const NPrivateProto::TScanDiskBatchRequest Request;

    ui32 MaxInFlight = 0;
    ui32 MaxDataInfoSize = 0;
    TInstant Deadline;
    TInstant ChecksDeadline;
    TInstant ChecksumsDeadline;

    EPhase Phase = Listing;

    TVector<TBlobCheck> Blobs;
    ui32 NextCheck = 0;
    ui32 ChecksInFlight = 0;
    ui32 ChecksReplied = 0;

    TVector<ui32> BlobsToVerify;
    TVector<ui32> BlobsToConfirm;

    NPrivateProto::TScanDiskBatchResponse Response;

public:
    TScanDiskBatchActor(
        TActorId owner,
        NPrivateProto::TScanDiskBatchRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    TString CheckRequest() const;

    void AddBlob(const TLogoBlobID& blobId, ui32 groupId, bool hasChecksums);

    void ScheduleWakeup(const TActorContext& ctx, TInstant deadline);

    void SendListingRequest(const TActorContext& ctx);

    void StartChecks(const TActorContext& ctx);
    void SendChecks(const TActorContext& ctx);
    void OnCheckReply(const TActorContext& ctx, ui32 blobIndex);
    void FinishChecks(const TActorContext& ctx);

    void StartChecksumVerification(const TActorContext& ctx);
    void StartConfirmation(const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx);
    void ReplyErrorAndDie(const TActorContext& ctx, NProto::TError error);

    template <typename T>
    std::unique_ptr<T> CreateVolumeRequest() const;

private:
    STFUNC(StateWork);

    void HandleScanDiskBatchResponse(
        const TEvVolume::TEvScanDiskBatchResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCheckIntegrityResult(
        const TEvBlobStorage::TEvCheckIntegrityResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleUndelivered(
        const TEvents::TEvUndelivered::TPtr& ev,
        const TActorContext& ctx);

    void HandleVerifyBlobChecksumsResponse(
        const TEvVolume::TEvVerifyBlobChecksumsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TScanDiskBatchActor::TScanDiskBatchActor(
    TActorId owner,
    NPrivateProto::TScanDiskBatchRequest request)
    : Owner(owner)
    , Request(std::move(request))
{}

void TScanDiskBatchActor::Bootstrap(const TActorContext& ctx)
{
    if (auto error = CheckRequest()) {
        ReplyErrorAndDie(ctx, MakeError(E_ARGUMENT, std::move(error)));
        return;
    }

    MaxInFlight = Request.GetMaxInFlight() ? Request.GetMaxInFlight()
                                           : DefaultScanDiskBatchMaxInFlight;
    MaxDataInfoSize = Request.GetMaxDataInfoSize()
                          ? Request.GetMaxDataInfoSize()
                          : DefaultMaxDataInfoSize;

    Become(&TThis::StateWork);

    const auto timeout = Request.GetDeadlineMs()
                             ? TDuration::MilliSeconds(Request.GetDeadlineMs())
                             : DefaultScanDiskBatchDeadline;
    Deadline = ctx.Now() + timeout;

    for (const auto& blob: Request.GetBlobsToRecheck()) {
        AddBlob(
            MakeLogoBlobId(blob.GetBlobId()),
            blob.GetBSGroupId(),
            blob.GetHasChecksums());
    }

    Response.SetNextCursor(Request.GetCursor());
    Response.SetScanCommitId(Request.GetScanCommitId());

    if (!Request.GetCount()) {
        StartChecks(ctx);
        return;
    }

    SendListingRequest(ctx);
}

TString TScanDiskBatchActor::CheckRequest() const
{
    if (!Request.GetDiskId()) {
        return "DiskId should be supplied";
    }

    for (const auto& blob: Request.GetBlobsToRecheck()) {
        if (!MakeLogoBlobId(blob.GetBlobId())) {
            return "BlobsToRecheck should have blob ids";
        }
    }

    return {};
}

void TScanDiskBatchActor::AddBlob(
    const TLogoBlobID& blobId,
    ui32 groupId,
    bool hasChecksums)
{
    auto& blob = Blobs.emplace_back();
    blob.BlobId = blobId;
    blob.BSGroupId = groupId;
    blob.HasChecksums = hasChecksums;

    CopyLogoBlobId(blobId, *blob.Result.MutableBlobId());
    blob.Result.SetBSGroupId(groupId);
    blob.Result.SetVerdict(TBlobResult::UNCHECKED);
}

void TScanDiskBatchActor::ScheduleWakeup(
    const TActorContext& ctx,
    TInstant deadline)
{
    ctx.Schedule(deadline - ctx.Now(), new TEvents::TEvWakeup(Phase));
}

template <typename T>
std::unique_ptr<T> TScanDiskBatchActor::CreateVolumeRequest() const
{
    auto request = std::make_unique<T>();
    request->Record.SetDiskId(Request.GetDiskId());
    request->Record.SetPartitionIndex(Request.GetPartitionIndex());
    return request;
}

void TScanDiskBatchActor::SendListingRequest(const TActorContext& ctx)
{
    auto request = CreateVolumeRequest<TEvVolume::TEvScanDiskBatchRequest>();
    auto& record = request->Record;
    record.SetCursor(Request.GetCursor());
    record.SetMinCommitId(Request.GetMinCommitId());
    record.SetCount(Request.GetCount());
    record.SetScanCommitId(Request.GetScanCommitId());

    Phase = Listing;
    ScheduleWakeup(ctx, Deadline);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TScanDiskBatchActor::StartChecks(const TActorContext& ctx)
{
    Phase = Checking;

    // The checks leave time for the checksum reads, and both leave time for
    // the confirmation.
    const auto now = ctx.Now();
    const auto timeLeft = Deadline - now;
    ChecksumsDeadline = now + timeLeft * 3 / 4;
    ChecksDeadline =
        Request.GetVerifyChecksums() ? now + timeLeft / 2 : ChecksumsDeadline;

    if (Blobs.empty()) {
        ReplyAndDie(ctx);
        return;
    }

    ScheduleWakeup(ctx, ChecksDeadline);
    SendChecks(ctx);
}

void TScanDiskBatchActor::SendChecks(const TActorContext& ctx)
{
    while (ChecksInFlight < MaxInFlight && NextCheck < Blobs.size()) {
        const ui32 blobIndex = NextCheck++;
        auto& blob = Blobs[blobIndex];

        blob.CheckSent = true;
        ++ChecksInFlight;

        SendToBSProxy(
            ctx,
            blob.BSGroupId,
            new TEvBlobStorage::TEvCheckIntegrity(
                blob.BlobId,
                ChecksDeadline,
                NKikimrBlobStorage::LowRead),
            blobIndex);
    }
}

void TScanDiskBatchActor::OnCheckReply(const TActorContext& ctx, ui32 blobIndex)
{
    Blobs[blobIndex].CheckReplied = true;
    --ChecksInFlight;
    ++ChecksReplied;

    if (ChecksReplied == Blobs.size()) {
        FinishChecks(ctx);
        return;
    }

    SendChecks(ctx);
}

void TScanDiskBatchActor::FinishChecks(const TActorContext& ctx)
{
    for (auto& blob: Blobs) {
        if (blob.CheckReplied) {
            continue;
        }

        *blob.Result.MutableError() = MakeError(
            E_TIMEOUT,
            blob.CheckSent ? "no CheckIntegrity reply before the deadline"
                           : "CheckIntegrity not sent before the deadline");
    }

    StartChecksumVerification(ctx);
}

void TScanDiskBatchActor::StartChecksumVerification(const TActorContext& ctx)
{
    if (!Request.GetVerifyChecksums()) {
        StartConfirmation(ctx);
        return;
    }

    for (ui32 blobIndex = 0; blobIndex < Blobs.size(); ++blobIndex) {
        auto& blob = Blobs[blobIndex];
        if (blob.HasChecksums && IsReadable(blob.Result.GetVerdict())) {
            blob.Result.SetChecksumsNotVerified(true);
            BlobsToVerify.push_back(blobIndex);
        }
    }

    const auto timeLeft = ChecksumsDeadline - ctx.Now();
    if (BlobsToVerify.empty() || timeLeft == TDuration::Zero()) {
        StartConfirmation(ctx);
        return;
    }

    auto request =
        CreateVolumeRequest<TEvVolume::TEvVerifyBlobChecksumsRequest>();
    auto& record = request->Record;
    for (ui32 blobIndex: BlobsToVerify) {
        LogoBlobIDFromLogoBlobID(Blobs[blobIndex].BlobId, record.AddBlobIds());
    }
    record.SetMaxInFlight(MaxInFlight);
    // The reply has to travel back before ChecksumsDeadline.
    record.SetDeadlineMs(Max<ui64>(1, (timeLeft * 4 / 5).MilliSeconds()));

    Phase = VerifyingChecksums;
    ScheduleWakeup(ctx, ChecksumsDeadline);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TScanDiskBatchActor::StartConfirmation(const TActorContext& ctx)
{
    for (ui32 blobIndex = 0; blobIndex < Blobs.size(); ++blobIndex) {
        auto& blob = Blobs[blobIndex];
        if (!blob.Deleted && NeedsConfirmation(blob.Result.GetVerdict())) {
            blob.Result.SetUnconfirmed(true);
            BlobsToConfirm.push_back(blobIndex);
        }
    }

    if (BlobsToConfirm.empty()) {
        ReplyAndDie(ctx);
        return;
    }

    auto request = CreateVolumeRequest<TEvVolume::TEvScanDiskBatchRequest>();
    for (ui32 blobIndex: BlobsToConfirm) {
        LogoBlobIDFromLogoBlobID(
            Blobs[blobIndex].BlobId,
            request->Record.AddBlobsToConfirm());
    }

    Phase = Confirming;
    ScheduleWakeup(ctx, Deadline);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TScanDiskBatchActor::ReplyAndDie(const TActorContext& ctx)
{
    for (auto& blob: Blobs) {
        if (blob.Deleted) {
            Response.SetDeletedDuringScan(Response.GetDeletedDuringScan() + 1);
            continue;
        }

        auto& result = blob.Result;
        const auto verdict = result.GetVerdict();

        if (verdict == TBlobResult::UNCHECKED ||
            verdict == TBlobResult::READ_FAILED ||
            result.GetChecksumsNotVerified() || result.GetUnconfirmed())
        {
            auto& blobToRetry = *Response.AddBlobsToRetry();
            *blobToRetry.MutableBlobId() = result.GetBlobId();
            blobToRetry.SetBSGroupId(blob.BSGroupId);
            blobToRetry.SetHasChecksums(blob.HasChecksums);
            *blobToRetry.MutableResult() = std::move(result);
            continue;
        }

        Response.SetCheckedBlobs(Response.GetCheckedBlobs() + 1);
        switch (blob.IntegrityVerdict) {
            case TBlobResult::REPLICATING:
                Response.SetReplicatingBlobs(
                    Response.GetReplicatingBlobs() + 1);
                break;
            case TBlobResult::DEGRADED:
                Response.SetDegradedBlobs(Response.GetDegradedBlobs() + 1);
                break;
            case TBlobResult::BROKEN:
                Response.SetBrokenBlobs(Response.GetBrokenBlobs() + 1);
                break;
            default:
                break;
        }
        if (blob.ChecksumsVerified) {
            Response.SetChecksumVerifiedBlobs(
                Response.GetChecksumVerifiedBlobs() + 1);
        }
        if (verdict == TBlobResult::CHECKSUM_MISMATCH) {
            Response.SetChecksumMismatchBlobs(
                Response.GetChecksumMismatchBlobs() + 1);
        }

        if (!IsReported(verdict)) {
            continue;
        }

        // One dead disk makes every blob it held degraded, so only the other
        // verdicts are warnings.
        LOG_LOG(
            ctx,
            verdict == TBlobResult::DEGRADED ? NLog::PRI_INFO : NLog::PRI_WARN,
            TBlockStoreComponents::SERVICE,
            "ScanDiskBatch: blob %s in group %u of disk %s is %s: Status# %s "
            "PlacementStatus# %s DataStatus# %s ErrorReason# %s",
            blob.BlobId.ToString().c_str(),
            blob.BSGroupId,
            Request.GetDiskId().c_str(),
            TBlobResult::EVerdict_Name(verdict).c_str(),
            result.GetStatus().c_str(),
            result.GetPlacementStatus().c_str(),
            result.GetDataStatus().c_str(),
            result.GetErrorReason().c_str());

        *Response.AddBlobResults() = std::move(result);
    }

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvServicePrivate::TEvScanDiskBatchResponse>(
            std::move(Response)));
    Die(ctx);
}

void TScanDiskBatchActor::ReplyErrorAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvServicePrivate::TEvScanDiskBatchResponse>(
            std::move(error)));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TScanDiskBatchActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvScanDiskBatchResponse, HandleScanDiskBatchResponse);
        HFunc(
            TEvBlobStorage::TEvCheckIntegrityResult,
            HandleCheckIntegrityResult);
        HFunc(TEvents::TEvUndelivered, HandleUndelivered);
        HFunc(
            TEvVolume::TEvVerifyBlobChecksumsResponse,
            HandleVerifyBlobChecksumsResponse);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TScanDiskBatchActor::HandleScanDiskBatchResponse(
    const TEvVolume::TEvScanDiskBatchResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto& record = ev->Get()->Record;

    if (Phase == Listing) {
        // Only listing can fail the batch. It only reads, so the scan actor
        // just resends it.
        if (HasError(record.GetError())) {
            ReplyErrorAndDie(ctx, record.GetError());
            return;
        }

        Response.SetNextCursor(record.GetNextCursor());
        Response.SetIsCompleted(record.GetIsCompleted());
        Response.SetScanCommitId(record.GetScanCommitId());
        Response.SetDeadBlobs(record.GetDeadBlobs());

        for (const auto& blob: record.GetBlobs()) {
            AddBlob(
                LogoBlobIDFromLogoBlobID(blob.GetBlobId()),
                blob.GetBSGroupId(),
                blob.GetHasChecksums());
        }

        StartChecks(ctx);
        return;
    }

    if (Phase != Confirming) {
        return;
    }

    if (HasError(record.GetError())) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "ScanDiskBatch: failed to confirm blobs of disk %s: %s",
            Request.GetDiskId().c_str(),
            FormatError(record.GetError()).c_str());

        for (ui32 blobIndex: BlobsToConfirm) {
            *Blobs[blobIndex].Result.MutableError() = record.GetError();
        }
        ReplyAndDie(ctx);
        return;
    }

    THashSet<TLogoBlobID> confirmedBlobs;
    for (const auto& blobId: record.GetConfirmedBlobs()) {
        confirmedBlobs.insert(LogoBlobIDFromLogoBlobID(blobId));
    }

    for (ui32 blobIndex: BlobsToConfirm) {
        auto& blob = Blobs[blobIndex];
        // Cleanup deletes a row from the blob index before garbage collection
        // can let the disks drop the blob, so a blob still in the index was
        // supposed to be stored.
        if (confirmedBlobs.contains(blob.BlobId)) {
            blob.Result.SetUnconfirmed(false);
        } else {
            blob.Deleted = true;
        }
    }

    ReplyAndDie(ctx);
}

void TScanDiskBatchActor::HandleCheckIntegrityResult(
    const TEvBlobStorage::TEvCheckIntegrityResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui32 blobIndex = ev->Cookie;
    if (Phase != Checking || blobIndex >= Blobs.size() ||
        Blobs[blobIndex].CheckReplied)
    {
        return;
    }

    const auto* msg = ev->Get();
    auto& blob = Blobs[blobIndex];

    blob.IntegrityVerdict = ClassifyCheckIntegrityResult(*msg);
    blob.Result.SetVerdict(blob.IntegrityVerdict);
    FillCheckIntegrityResult(*msg, MaxDataInfoSize, blob.Result);

    OnCheckReply(ctx, blobIndex);
}

void TScanDiskBatchActor::HandleUndelivered(
    const TEvents::TEvUndelivered::TPtr& ev,
    const TActorContext& ctx)
{
    const ui32 blobIndex = ev->Cookie;
    if (ev->Get()->SourceType != TEvBlobStorage::EvCheckIntegrity ||
        Phase != Checking || blobIndex >= Blobs.size() ||
        Blobs[blobIndex].CheckReplied)
    {
        return;
    }

    *Blobs[blobIndex].Result.MutableError() = MakeError(
        E_REJECTED,
        "CheckIntegrity was not delivered to the group proxy");

    OnCheckReply(ctx, blobIndex);
}

void TScanDiskBatchActor::HandleVerifyBlobChecksumsResponse(
    const TEvVolume::TEvVerifyBlobChecksumsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (Phase != VerifyingChecksums) {
        return;
    }

    const auto& record = ev->Get()->Record;

    if (HasError(record.GetError()) ||
        record.ResultsSize() != BlobsToVerify.size())
    {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "ScanDiskBatch: failed to verify checksums of disk %s: %s",
            Request.GetDiskId().c_str(),
            FormatError(record.GetError()).c_str());

        StartConfirmation(ctx);
        return;
    }

    using TChecksumsResult = NProto::TBlobChecksumsResult;

    for (ui32 i = 0; i < BlobsToVerify.size(); ++i) {
        const auto& checksumsResult = record.GetResults(i);
        auto& blob = Blobs[BlobsToVerify[i]];
        auto& result = blob.Result;

        switch (checksumsResult.GetStatus()) {
            case TChecksumsResult::VERIFIED:
                result.SetChecksumsNotVerified(false);
                blob.ChecksumsVerified = true;
                break;
            case TChecksumsResult::MISMATCH:
                // All parts agree, so blobstorage can't repair the blob.
                result.SetChecksumsNotVerified(false);
                result.SetVerdict(TBlobResult::CHECKSUM_MISMATCH);
                result.SetMismatchedBlockCount(
                    checksumsResult.GetMismatchedBlockCount());
                for (const auto& mismatch: checksumsResult.GetMismatches()) {
                    auto& dst = *result.AddMismatches();
                    dst.SetBlockIndex(mismatch.GetBlockIndex());
                    dst.SetBlobOffset(mismatch.GetBlobOffset());
                    dst.SetExpectedChecksum(mismatch.GetExpectedChecksum());
                    dst.SetActualChecksum(mismatch.GetActualChecksum());
                }
                break;
            case TChecksumsResult::READ_FAILED:
                result.SetChecksumsNotVerified(false);
                result.SetVerdict(TBlobResult::READ_FAILED);
                *result.MutableError() = checksumsResult.GetError();
                break;
            case TChecksumsResult::NOT_IN_INDEX:
                blob.Deleted = true;
                break;
            default:
                break;
        }
    }

    StartConfirmation(ctx);
}

void TScanDiskBatchActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Get()->Tag != Phase) {
        return;
    }

    switch (Phase) {
        case Listing:
            ReplyErrorAndDie(
                ctx,
                MakeError(
                    E_TIMEOUT,
                    "no ScanDiskBatch reply before the deadline"));
            break;
        case Checking:
            FinishChecks(ctx);
            break;
        case VerifyingChecksums:
            StartConfirmation(ctx);
            break;
        case Confirming:
            for (ui32 blobIndex: BlobsToConfirm) {
                *Blobs[blobIndex].Result.MutableError() =
                    MakeError(E_TIMEOUT, "no confirmation before the deadline");
            }
            ReplyAndDie(ctx);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateScanDiskBatchActor(
    TActorId owner,
    NPrivateProto::TScanDiskBatchRequest request)
{
    return std::make_unique<TScanDiskBatchActor>(owner, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
