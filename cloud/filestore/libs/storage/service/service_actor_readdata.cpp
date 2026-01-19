#include "service_actor.h"
#include "rope_utils.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/probes.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/model/sparse_segment.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsTwoStageReadEnabled(const NProto::TFileStore& fs)
{
    const auto isHdd = fs.GetStorageMediaKind() == NProto::STORAGE_MEDIA_HYBRID
        || fs.GetStorageMediaKind() == NProto::STORAGE_MEDIA_HDD;
    const auto disabledAsHdd = isHdd &&
        fs.GetFeatures().GetTwoStageReadDisabledForHDD();
    return !disabledAsHdd && fs.GetFeatures().GetTwoStageReadEnabled();
}

////////////////////////////////////////////////////////////////////////////////

class TReadDataActor final: public TActorBootstrapped<TReadDataActor>
{
private:
    // Original request
    NProto::TReadDataRequest ReadRequest;

    // Filesystem-specific params
    const TString LogTag;
    const ui32 BlockSize;
    const bool ReadBlobDisabled;

    // Response data
    const TByteRange OriginByteRange;
    const TByteRange AlignedByteRange;
    std::unique_ptr<TString> BlockBuffer;
    TRope TargetBuffers;
    NProtoPrivate::TDescribeDataResponse DescribeResponse;
    ui32 RemainingBlobsToRead = 0;
    bool ReadDataFallbackEnabled = false;
    TSparseSegment ZeroIntervals;

    // Stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;
    ITraceSerializerPtr TraceSerializer;
    TInFlightRequestStoragePtr InFlightRequests; // holding this IntrusivePtr
                                                 // to guarantee that
                                                 // MainInFlightRequest is not
                                                 // deallocated
    const TActorId Sender;
    const ui64 Cookie;
    TCallContextPtr CallContext; // invalid after Bootstrap()
    TChecksumCalcInfo ChecksumCalcInfo; // invalid after Bootstrap()
    const TInstant StartTime;
    const ui64 RequestCookie;
    TString ClientId; // invalid after Bootstrap()
    TInFlightRequest* MainInFlightRequest;
    std::optional<TInFlightRequest> InFlightRequest;
    TShardStatePtr ShardState;
    const NCloud::NProto::EStorageMediaKind MediaKind;
    const bool UseTwoStageRead;

public:
    TReadDataActor(
        NProto::TReadDataRequest readRequest,
        TString logTag,
        ui32 blockSize,
        bool readBlobDisabled,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        TInFlightRequestStoragePtr inFlightRequests,
        NActors::TActorId sender,
        ui64 cookie,
        TCallContextPtr callContext,
        TChecksumCalcInfo checksumCalcInfo,
        TInstant startTime,
        ui64 requestCookie,
        TString clientId,
        TShardStatePtr shardState,
        NCloud::NProto::EStorageMediaKind mediaKind,
        bool useTwoStageRead);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeData(const TActorContext& ctx);

    void HandleDescribeDataResponse(
        const TEvIndexTablet::TEvDescribeDataResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReadBlobsIfNeeded(const TActorContext& ctx);

    void HandleReadBlobResponse(
        const TEvBlobStorage::TEvGetResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReadData(const TActorContext& ctx, const TString& fallbackReason);

    void HandleReadDataResponse(
        const TEvService::TEvReadDataResponse::TPtr& ev,
        const TActorContext& ctx);

    void MoveBufferToIovecsIfNeeded(
        const TActorContext& ctx,
        NProto::TReadDataResponse& response);

    void SendResponseAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvReadDataResponse> response);
    void ReplyTwoStageAndDie(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TReadDataActor::TReadDataActor(
        NProto::TReadDataRequest readRequest,
        TString logTag,
        ui32 blockSize,
        bool readBlobDisabled,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        TInFlightRequestStoragePtr inFlightRequests,
        NActors::TActorId sender,
        ui64 cookie,
        TCallContextPtr callContext,
        TChecksumCalcInfo checksumCalcInfo,
        TInstant startTime,
        ui64 requestCookie,
        TString clientId,
        TShardStatePtr shardState,
        NCloud::NProto::EStorageMediaKind mediaKind,
        bool useTwoStageRead)
    : ReadRequest(std::move(readRequest))
    , LogTag(std::move(logTag))
    , BlockSize(blockSize)
    , ReadBlobDisabled(readBlobDisabled)
    , OriginByteRange(
        ReadRequest.GetOffset(),
        ReadRequest.GetLength(),
        BlockSize)
    , AlignedByteRange(OriginByteRange.AlignedSuperRange())
    , BlockBuffer(std::make_unique<TString>())
    , ZeroIntervals(TDefaultAllocator::Instance(), 0, OriginByteRange.Length)
    , RequestStats(std::move(requestStats))
    , ProfileLog(std::move(profileLog))
    , TraceSerializer(std::move(traceSerializer))
    , InFlightRequests(std::move(inFlightRequests))
    , Sender(sender)
    , Cookie(cookie)
    , CallContext(std::move(callContext))
    , ChecksumCalcInfo(std::move(checksumCalcInfo))
    , StartTime(startTime)
    , RequestCookie(requestCookie)
    , ClientId(std::move(clientId))
    , ShardState(std::move(shardState))
    , MediaKind(mediaKind)
    , UseTwoStageRead(useTwoStageRead)
{
}

void TReadDataActor::Bootstrap(const TActorContext& ctx)
{
    if (ReadRequest.GetIovecs().empty()) {
        // BlockBuffer should not be initialized in constructor, because
        // creating a block buffer leads to memory allocation (and
        // initialization) which is heavy and we would like to execute that on a
        // separate thread (instead of this actor's parent thread)
        BlockBuffer->ReserveAndResize(ReadRequest.GetLength());
        TargetBuffers = CreateRope(BlockBuffer->begin(), BlockBuffer->size());
    } else {
        TargetBuffers = CreateRope(ReadRequest.GetIovecs());
    }

    // Registering InFlightRequest here for the same reason - it's quite
    // expensive so we don't want to do it in TStorageServiceActor
    MainInFlightRequest = InFlightRequests->Register(
        Sender,
        Cookie,
        std::move(CallContext),
        MediaKind,
        std::move(ChecksumCalcInfo),
        RequestStats,
        StartTime,
        RequestCookie);

    InitProfileLogRequestInfo(
        MainInFlightRequest->AccessProfileLogRequest(),
        ReadRequest);
    MainInFlightRequest->AccessProfileLogRequest().SetClientId(
        std::move(ClientId));

    if (UseTwoStageRead) {
        DescribeData(ctx);
    } else {
        ReadData(ctx, {} /* fallbackReason */);
    }
    Become(&TThis::StateWork);
}

void TReadDataActor::DescribeData(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_ServiceWorker,
        MainInFlightRequest->CallContext,
        "DescribeData");

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "%s executing DescribeData for node: %lu, "
        "handle: %lu, offset: %lu, length: %lu",
        LogTag.c_str(),
        ReadRequest.GetNodeId(),
        ReadRequest.GetHandle(),
        ReadRequest.GetOffset(),
        ReadRequest.GetLength());

    auto request = std::make_unique<TEvIndexTablet::TEvDescribeDataRequest>();

    request->Record.MutableHeaders()->CopyFrom(ReadRequest.GetHeaders());
    request->Record.SetFileSystemId(ReadRequest.GetFileSystemId());
    request->Record.SetNodeId(ReadRequest.GetNodeId());
    request->Record.SetHandle(ReadRequest.GetHandle());
    request->Record.SetOffset(ReadRequest.GetOffset());
    request->Record.SetLength(ReadRequest.GetLength());

    auto describeCallContext = MakeIntrusive<TCallContext>(
        MainInFlightRequest->CallContext->FileSystemId,
        MainInFlightRequest->CallContext->RequestId);
    describeCallContext->SetRequestStartedCycles(GetCycleCount());
    describeCallContext->RequestType = EFileStoreRequest::DescribeData;
    InFlightRequest.emplace(
        Sender,
        Cookie,
        std::move(describeCallContext),
        ProfileLog,
        MediaKind,
        RequestStats);

    InFlightRequest->Start(ctx.Now());
    InitProfileLogRequestInfo(
        InFlightRequest->AccessProfileLogRequest(),
        request->Record);
    TraceSerializer->BuildTraceRequest(
        *request->Record.MutableHeaders()->MutableInternal()->MutableTrace(),
        MainInFlightRequest->CallContext->LWOrbit);

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

////////////////////////////////////////////////////////////////////////////////

TString DescribeResponseDebugString(
    NProtoPrivate::TDescribeDataResponse response)
{
    // we need to clear user data first
    for (auto& freshRange: *response.MutableFreshDataRanges()) {
        freshRange.SetContent(
            Sprintf("Content size: %lu", freshRange.GetContent().size()));
    }

    return response.DebugString();
}

////////////////////////////////////////////////////////////////////////////////

void ApplyFreshDataRange(
    const TActorContext& ctx,
    const NProtoPrivate::TFreshDataRange& sourceFreshData,
    TRope& targetBuffer,
    TByteRange targetByteRange,
    ui32 blockSize,
    ui64 offset,
    ui64 length,
    const NProtoPrivate::TDescribeDataResponse& describeResponse,
    TSparseSegment& zeroIntervals)
{
    if (sourceFreshData.GetContent().empty()) {
        return;
    }
    TByteRange sourceByteRange(
        sourceFreshData.GetOffset(),
        sourceFreshData.GetContent().size(),
        blockSize);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "common byte range found: source: %s, target: %s, original request: "
        "[%lu, %lu), response: %s",
        sourceByteRange.Describe().c_str(),
        targetByteRange.Describe().c_str(),
        offset,
        length,
        DescribeResponseDebugString(describeResponse).Quote().c_str());

    auto commonRange = sourceByteRange.Intersect(targetByteRange);

    if (commonRange.Length == 0) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "common range is empty: source: %s, target: %s",
            sourceByteRange.Describe().c_str(),
            targetByteRange.Describe().c_str());
        return;
    }

    const ui64 relOffset = commonRange.Offset - targetByteRange.Offset;
    TRopeUtils::Memcpy(
        targetBuffer.Begin() + relOffset,
        sourceFreshData.GetContent().data() +
            (commonRange.Offset - sourceByteRange.Offset),
        commonRange.Length);
    zeroIntervals.PunchHole(relOffset, relOffset + commonRange.Length);
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::HandleDescribeDataResponse(
    const TEvIndexTablet::TEvDescribeDataResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    TABLET_VERIFY(InFlightRequest);

    FinalizeProfileLogRequestInfo(
        InFlightRequest->AccessProfileLogRequest(),
        msg->Record);
    InFlightRequest->Complete(ctx.Now(), error);
    HandleServiceTraceInfo(
        "DescribeData",
        ctx,
        TraceSerializer,
        MainInFlightRequest->CallContext,
        msg->Record);

    if (FAILED(msg->GetStatus())) {
        if (error.GetCode() != E_FS_THROTTLED) {
            ReadData(ctx, FormatError(error));
        } else {
            HandleError(ctx, error);
        }
        return;
    }

    const auto& backendInfo = msg->Record.GetHeaders().GetBackendInfo();
    ShardState->SetIsOverloaded(backendInfo.GetIsOverloaded());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "%s DescribeData succeeded %lu freshdata + %lu blobpieces"
        ", backend-info: %s",
        LogTag.c_str(),
        msg->Record.FreshDataRangesSize(),
        msg->Record.BlobPiecesSize(),
        backendInfo.ShortUtf8DebugString().Quote().c_str());

    DescribeResponse.CopyFrom(msg->Record);
    ReadBlobsIfNeeded(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::ReadBlobsIfNeeded(const TActorContext& ctx)
{
    if (DescribeResponse.GetFakeResponse() || ReadBlobDisabled) {
        if (ReadBlobDisabled) {
            ReportFakeBlobWasRead();
            ReplyTwoStageAndDie(ctx);
            return;
        }

        ReportUnexpectedFakeDescribeDataResponse(LogTag);

        // It is better to hang IO, otherwise, returning a fatal error or
        // success could leave the filesystem in a broken or corrupted state
        auto error = MakeError(
            E_REJECTED,
            "misconfiguration: fake DescribeData received when "
            "ReadBlobDisabled=false");
        HandleError(ctx, std::move(error));
        return;
    }

    RemainingBlobsToRead = DescribeResponse.GetBlobPieces().size();
    if (RemainingBlobsToRead == 0) {
        ReplyTwoStageAndDie(ctx);
        return;
    }

    FILESTORE_TRACK(
        RequestReceived_ServiceWorker,
        MainInFlightRequest->CallContext,
        "ReadBlobs");

    auto readBlobCallContext = MakeIntrusive<TCallContext>(
        MainInFlightRequest->CallContext->FileSystemId,
        MainInFlightRequest->CallContext->RequestId);
    readBlobCallContext->SetRequestStartedCycles(GetCycleCount());
    readBlobCallContext->RequestType = EFileStoreRequest::ReadBlob;
    ui32 blobPieceId = 0;

    InFlightRequest.emplace(
        Sender,
        Cookie,
        std::move(readBlobCallContext),
        ProfileLog,
        MediaKind,
        RequestStats);
    InFlightRequest->Start(ctx.Now());

    for (const auto& blobPiece: DescribeResponse.GetBlobPieces()) {
        NKikimr::TLogoBlobID blobId =
            LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Processing blob piece: %s, size: %lu",
            blobPiece.DebugString().Quote().c_str(),
            blobId.BlobSize());
        NKikimr::TActorId proxy =
            MakeBlobStorageProxyID(blobPiece.GetBSGroupId());
        using TEvGetQuery = TEvBlobStorage::TEvGet::TQuery;
        TArrayHolder<TEvGetQuery> queries(
            new TEvGetQuery[blobPiece.RangesSize()]);
        for (size_t i = 0; i < blobPiece.RangesSize(); ++i) {
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "Adding query for blobId: %s, offset: %lu, length: %lu, "
                "blobsize: %lu",
                blobId.ToString().c_str(),
                blobPiece.GetRanges(i).GetBlobOffset(),
                blobPiece.GetRanges(i).GetLength(),
                blobId.BlobSize());
            queries[i].Set(
                blobId,
                blobPiece.GetRanges(i).GetBlobOffset(),
                blobPiece.GetRanges(i).GetLength());
        }
        auto request = std::make_unique<TEvBlobStorage::TEvGet>(
            queries,
            blobPiece.RangesSize(),
            TInstant::Max(),
            NKikimrBlobStorage::FastRead);

        if (!MainInFlightRequest->CallContext->LWOrbit.Fork(request->Orbit)) {
            FILESTORE_TRACK(
                ForkFailed,
                MainInFlightRequest->CallContext,
                "TEvBlobStorage::TEvGet");
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Sending ReadBlob request, size: %lu, blobId: %s",
            blobPiece.RangesSize(),
            blobId.ToString().c_str());

        SendToBSProxy(ctx, proxy, request.release(), blobPieceId++);
    }
}

void TReadDataActor::HandleReadBlobResponse(
    const TEvBlobStorage::TEvGetResult::TPtr& ev,
    const TActorContext& ctx)
{
    if (ReadDataFallbackEnabled) {
        // we don't need this response anymore

        return;
    }

    const auto* msg = ev->Get();
    MainInFlightRequest->CallContext->LWOrbit.Join(msg->Orbit);

    if (msg->Status != NKikimrProto::OK) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s TEvBlobStorage::TEvGet failed: response %s",
            LogTag.c_str(),
            msg->Print(false).c_str());

        const NProto::TError error(
            MakeError(MAKE_KIKIMR_ERROR(msg->Status), msg->ErrorReason));

        InFlightRequest->Complete(ctx.Now(), error);

        const auto errorReason = FormatError(error);
        ReadData(ctx, errorReason);
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "%s ReadBlobResponse count: %lu, status: %lu, cookie: %lu",
        LogTag.c_str(),
        msg->ResponseSz,
        (ui64)(msg->Status),
        ev->Cookie);

    TABLET_VERIFY(ev->Cookie < DescribeResponse.BlobPiecesSize());
    const auto& blobPiece = DescribeResponse.GetBlobPieces(ev->Cookie);

    for (size_t i = 0; i < msg->ResponseSz; ++i) {
        TABLET_VERIFY(i < blobPiece.RangesSize());

        const auto& blobPiece = DescribeResponse.GetBlobPieces(ev->Cookie);
        const auto& blobRange = blobPiece.GetRanges(i);
        const auto& response = msg->Responses[i];
        if (response.Status != NKikimrProto::OK) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s TEvBlobStorage::TEvGet query failed:"
                " status %s, response %s",
                LogTag.c_str(),
                NKikimrProto::EReplyStatus_Name(response.Status).c_str(),
                msg->Print(false).c_str());

            const auto error =
                MakeError(MAKE_KIKIMR_ERROR(response.Status), "read error");
            InFlightRequest->Complete(ctx.Now(), error);
            ReadData(ctx, FormatError(error));
            return;
        }

        const auto blobId = LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());

        STORAGE_CHECK_PRECONDITION(response.Id == blobId);
        STORAGE_CHECK_PRECONDITION(!response.Buffer.empty());
        if (response.Id != blobId || response.Buffer.empty()) {
            const auto error = FormatError(MakeError(
                E_FAIL,
                Sprintf(
                    "invalid response received: "
                    "expected blobId: %s, response blobId: %s, buffer size: "
                    "%lu",
                    blobId.ToString().c_str(),
                    response.Id.ToString().c_str(),
                    response.Buffer.size())));
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s ReadBlob error: %s",
                LogTag.c_str(),
                error.c_str());
            InFlightRequest->Complete(ctx.Now(), MakeError(E_FAIL, error));
            ReadData(ctx, error);

            return;
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "ReadBlobResponse: blobId: %s, offset: %lu, length: %lu, size: "
            "%lu, target: %s",
            blobPiece.GetBlobId().DebugString().Quote().c_str(),
            blobRange.GetBlobOffset(),
            blobRange.GetLength(),
            response.Buffer.size(),
            AlignedByteRange.Describe().c_str());
        Y_ABORT_UNLESS(
            blobRange.GetLength() == response.Buffer.size(),
            "Blob range length mismatch: all requested ranges: %s, response: "
            "#%lu, size is %lu",
            DescribeResponse.DebugString().Quote().c_str(),
            i,
            response.Buffer.size());
        TABLET_VERIFY(blobRange.GetOffset() >= AlignedByteRange.Offset);

        const auto blobByteRange =
            TByteRange{blobRange.GetOffset(), blobRange.GetLength(), BlockSize};
        const auto commonRange = OriginByteRange.Intersect(blobByteRange);
        if (commonRange.Length != 0) {
            const auto relOffset = commonRange.Offset - OriginByteRange.Offset;
            auto dataIter = response.Buffer.begin();
            dataIter += commonRange.Offset - blobByteRange.Offset;
            TRopeUtils::Memcpy(
                TargetBuffers.Begin() + relOffset,
                dataIter,
                commonRange.Length);
            ZeroIntervals.PunchHole(relOffset, relOffset + commonRange.Length);
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "common range is empty: origin range: %s, blob range: %s",
                OriginByteRange.Describe().c_str(),
                blobByteRange.Describe().c_str());
        }
    }

    --RemainingBlobsToRead;
    if (RemainingBlobsToRead == 0) {
        InFlightRequest->Complete(ctx.Now(), {});

        ReplyTwoStageAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::ReadData(
    const TActorContext& ctx,
    const TString& fallbackReason)
{
    FILESTORE_TRACK(
        RequestReceived_ServiceWorker,
        MainInFlightRequest->CallContext,
        "ReadData");

    ReadDataFallbackEnabled = true;

    if (fallbackReason) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s falling back to ReadData: "
            "node: %lu, handle: %lu, offset: %lu, length: %lu. Message: %s",
            LogTag.c_str(),
            ReadRequest.GetNodeId(),
            ReadRequest.GetHandle(),
            ReadRequest.GetOffset(),
            ReadRequest.GetLength(),
            fallbackReason.Quote().c_str());
    }

    auto request = std::make_unique<TEvService::TEvReadDataRequest>();
    request->Record = std::move(ReadRequest);
    request->Record.MutableHeaders()->SetThrottlingDisabled(true);
    TraceSerializer->BuildTraceRequest(
        *request->Record.MutableHeaders()->MutableInternal()->MutableTrace(),
        MainInFlightRequest->CallContext->LWOrbit);

    // Original iovecs should be preserved in this request and pruned during
    // this forwarding on the tablet side
    ReadRequest.MutableIovecs()->Swap(request->Record.MutableIovecs());

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TReadDataActor::HandleReadDataResponse(
    const TEvService::TEvReadDataResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    HandleServiceTraceInfo(
        "ReadData",
        ctx,
        TraceSerializer,
        MainInFlightRequest->CallContext,
        msg->Record);

    if (FAILED(msg->GetStatus())) {
        HandleError(ctx, msg->GetError());
        return;
    }

    const auto& backendInfo = msg->Record.GetHeaders().GetBackendInfo();
    ShardState->SetIsOverloaded(backendInfo.GetIsOverloaded());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "ReadData succeeded %lu data, backend-info: %s",
        msg->Record.GetBuffer().size(),
        backendInfo.ShortUtf8DebugString().Quote().c_str());

    auto response = std::make_unique<TEvService::TEvReadDataResponse>();
    response->Record = std::move(msg->Record);

    MoveBufferToIovecsIfNeeded(ctx, response->Record);

    SendResponseAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::MoveBufferToIovecsIfNeeded(
    const TActorContext& ctx,
    NProto::TReadDataResponse& response)
{
    if (ReadRequest.GetIovecs().empty()) {
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "%s copying data to target iovecs",
        LogTag.c_str());
    auto currentOffset = response.GetBufferOffset();
    for (const auto& iovec: ReadRequest.GetIovecs()) {
        if (currentOffset >= response.GetBuffer().size()) {
            break;
        }
        auto dataToWrite =
            Min(iovec.GetLength(), response.GetBuffer().size() - currentOffset);
        if (dataToWrite > 0) {
            char* targetData = reinterpret_cast<char*>(iovec.GetBase());
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s copying %lu bytes to iovec at offset %lu to the target "
                "address "
                "%p",
                LogTag.c_str(),
                dataToWrite,
                currentOffset,
                targetData);
            memcpy(
                targetData,
                response.GetBuffer().data() + currentOffset,
                dataToWrite);
            currentOffset += dataToWrite;
        }
    }
    response.SetLength(
        response.GetBuffer().size() - response.GetBufferOffset());
    response.ClearBuffer();
    response.SetBufferOffset(0);
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::ReplyTwoStageAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvReadDataResponse>();

    // we apply fresh data ranges to the buffer only after all blobs are
    // read and applied
    for (const auto& freshDataRange: DescribeResponse.GetFreshDataRanges())
    {
        ui64 offset = freshDataRange.GetOffset();
        const TString& content = freshDataRange.GetContent();

        ApplyFreshDataRange(
            ctx,
            freshDataRange,
            TargetBuffers,
            OriginByteRange,
            BlockSize,
            ReadRequest.GetOffset(),
            ReadRequest.GetLength(),
            DescribeResponse,
            ZeroIntervals);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s processed fresh data range size: %lu, offset: %lu",
            LogTag.c_str(),
            content.size(),
            offset);
    }

    for (const auto& zeroInterval: ZeroIntervals) {
        TRopeUtils::Memset(
            TargetBuffers.Begin() + zeroInterval.Start,
            0,
            zeroInterval.End - zeroInterval.Start);
    }

    const auto end = Min(DescribeResponse.GetFileSize(), OriginByteRange.End());
    if (end <= OriginByteRange.Offset) {
        BlockBuffer->clear();
    } else {
        const ui64 length = end - OriginByteRange.Offset;
        if (ReadRequest.GetIovecs().empty()) {
            BlockBuffer->ReserveAndResize(length);
            response->Record.set_allocated_buffer(BlockBuffer.release());
        } else {
            response->Record.SetLength(length);
        }
    }

    SendResponseAndDie(ctx, std::move(response));
}

void TReadDataActor::SendResponseAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvReadDataResponse> response)
{
    FILESTORE_TRACK(
        ResponseSent_ServiceWorker,
        MainInFlightRequest->CallContext,
        "ReadData");

    CompleteRequestImpl<TEvService::TReadDataMethod>(
        ctx,
        TraceSerializer,
        response->Record,
        MainInFlightRequest,
        *InFlightRequests,
        RequestCookie);

    ctx.Send(Sender, response.release(), 0 /* flags */, Cookie);

    Die(ctx);
}

void TReadDataActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    SendResponseAndDie(
        ctx,
        std::make_unique<TEvService::TEvReadDataResponse>(error));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TReadDataActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDescribeDataResponse,
            HandleDescribeDataResponse);

        HFunc(TEvService::TEvReadDataResponse, HandleReadDataResponse);

        HFunc(TEvBlobStorage::TEvGetResult, HandleReadBlobResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleReadData(
    const TEvService::TEvReadDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    TInstant startTime = ctx.Now();
    auto* msg = ev->Get();

    FILESTORE_TRACK(RequestReceived_Service, msg->CallContext, "ReadData");

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<TEvService::TEvReadDataResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }
    const NProto::TFileStore& filestore = session->FileStore;

    // In handleless IO mode, if the handle is not set, we use the nodeId to
    // infer the shard number
    const ui32 shardNo = ExtractShardNoSafe(
        filestore,
        filestore.GetFeatures().GetAllowHandlelessIO() &&
                msg->Record.GetHandle() == InvalidHandle
            ? msg->Record.GetNodeId()
            : msg->Record.GetHandle());

    auto [fsId, error] = SelectShard(
        ctx,
        sessionId,
        seqNo,
        msg->Record.GetHeaders().GetDisableMultiTabletForwarding(),
        TEvService::TReadDataMethod::Name,
        msg->CallContext->RequestId,
        filestore,
        shardNo);

    if (HasError(error)) {
        auto response = std::make_unique<TEvService::TEvReadDataResponse>(
            std::move(error));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    if (fsId) {
        msg->Record.SetFileSystemId(fsId);
    }

    if (msg->Record.IovecsSize() > 0) {
        ui64 totalIovecLength = 0;
        for (const auto& iovec: msg->Record.GetIovecs()) {
            totalIovecLength += iovec.GetLength();
        }
        if (totalIovecLength < msg->Record.GetLength()) {
            auto response = std::make_unique<TEvService::TEvReadDataResponse>(
                ErrorInvalidArgument(Sprintf("Total iovec length %lu is less than requested read length %lu",
                    totalIovecLength, msg->Record.GetLength())));
            return NCloud::Reply(ctx, *ev, std::move(response));
        }
    }

    const bool isShardNoValid = shardNo > 0 && !fsId.empty();
    auto shardState = isShardNoValid
        ? session->AccessShardState(shardNo - 1)
        : session->AccessMainTabletState();

    const ui32 twoStageReadThreshold =
        filestore.GetFeatures().GetTwoStageReadThreshold()
        ? filestore.GetFeatures().GetTwoStageReadThreshold()
        : StorageConfig->GetTwoStageReadThreshold();

    //
    // For large requests we conservatively decide not to use tablet-side
    // network for the data in order not to overload the tablet-side NIC.
    //
    // For small requests we use tablet-side reads only if the tablet doesn't
    // consider itself to be overloaded.
    //
    // Later on we can start taking NIC usage into account for the IsOverloaded
    // flag but right now we don't expect it to take network usage into account.
    //

    const bool useTwoStageRead = IsTwoStageReadEnabled(filestore)
        && (msg->Record.GetLength() >= twoStageReadThreshold
            || shardState->GetIsOverloaded());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "read data %s, use-two-stage-read: %d, shard-is-overloaded: %d",
        msg->Record.DebugString().Quote().c_str(),
        useTwoStageRead,
        shardState->GetIsOverloaded());

    TChecksumCalcInfo checksumCalcInfo;
    const bool blockChecksumsEnabled =
        filestore.GetFeatures().GetBlockChecksumsInProfileLogEnabled()
        || StorageConfig->GetBlockChecksumsInProfileLogEnabled();
    if (blockChecksumsEnabled) {
        checksumCalcInfo = TChecksumCalcInfo(
            filestore.GetBlockSize(),
            msg->Record.GetIovecs());
    }

    auto actor = std::make_unique<TReadDataActor>(
        std::move(msg->Record),
        filestore.GetFileSystemId(),
        filestore.GetBlockSize(),
        filestore.GetFeatures().GetReadBlobDisabled(),
        session->RequestStats,
        ProfileLog,
        TraceSerializer,
        InFlightRequests,
        ev->Sender,
        ev->Cookie,
        std::move(msg->CallContext),
        std::move(checksumCalcInfo),
        startTime,
        GenerateRequestCookie(),
        session->ClientId,
        std::move(shardState),
        session->MediaKind,
        useTwoStageRead);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
