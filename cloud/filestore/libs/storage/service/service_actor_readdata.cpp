#include "service_actor.h"

#include "protobuf_utils.h"
#include "rope_utils.h"
#include "verify.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/blob_id.h>
#include <cloud/filestore/libs/storage/core/probes.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/model/sparse_segment.h>

#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/libs/protobuf/src/google/protobuf/io/coded_stream.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/event_local.h>

#include <memory>
#include <optional>

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

struct TEvStartReadData final
    : public TEventLocal<TEvStartReadData, TEvServicePrivate::EvStartReadData>
{
    NProto::TReadDataRequest ReadRequest;
    TString LogTag;
    ui32 BlockSize;
    bool ReadBlobDisabled;
    IRequestStatsPtr RequestStats;
    TActorId Sender;
    ui64 Cookie;
    TCallContextPtr CallContext;
    TChecksumCalcInfo ChecksumCalcInfo;
    TInstant StartTime;
    ui64 RequestCookie;
    TString ClientId;
    TShardStatePtr ShardState;
    NCloud::NProto::EStorageMediaKind MediaKind;
    bool UseTwoStageRead;
    bool UseCustomReadDataResponseParser;
    bool ZeroCopyReadEnabled;

    TEvStartReadData(
        NProto::TReadDataRequest readRequest,
        TString logTag,
        ui32 blockSize,
        bool readBlobDisabled,
        IRequestStatsPtr requestStats,
        TActorId sender,
        ui64 cookie,
        TCallContextPtr callContext,
        TChecksumCalcInfo checksumCalcInfo,
        TInstant startTime,
        ui64 requestCookie,
        TString clientId,
        TShardStatePtr shardState,
        NCloud::NProto::EStorageMediaKind mediaKind,
        bool useTwoStageRead,
        bool useCustomReadDataResponseParser,
        bool zeroCopyReadEnabled)
        : ReadRequest(std::move(readRequest))
        , LogTag(std::move(logTag))
        , BlockSize(blockSize)
        , ReadBlobDisabled(readBlobDisabled)
        , RequestStats(std::move(requestStats))
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
        , UseCustomReadDataResponseParser(useCustomReadDataResponseParser)
        , ZeroCopyReadEnabled(zeroCopyReadEnabled)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TReadDataActor final: public TActorBootstrapped<TReadDataActor>
{
private:
    struct TRequestState
    {
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
        const TActorId Sender;
        const ui64 Cookie;
        TCallContextPtr CallContext;          // invalid after StartRequest()
        TChecksumCalcInfo ChecksumCalcInfo;   // invalid after StartRequest()
        const TInstant StartTime;
        const ui64 RequestCookie;
        TString ClientId;   // invalid after StartRequest()
        TInFlightRequest* MainInFlightRequest = nullptr;
        std::optional<TInFlightRequest> InFlightRequest;
        TShardStatePtr ShardState;
        const NCloud::NProto::EStorageMediaKind MediaKind;
        const bool UseTwoStageRead;
        const bool UseCustomReadDataResponseParser;
        const bool ZeroCopyReadEnabled;

        TRequestState(
            NProto::TReadDataRequest readRequest,
            TString logTag,
            ui32 blockSize,
            bool readBlobDisabled,
            IRequestStatsPtr requestStats,
            NActors::TActorId sender,
            ui64 cookie,
            TCallContextPtr callContext,
            TChecksumCalcInfo checksumCalcInfo,
            TInstant startTime,
            ui64 requestCookie,
            TString clientId,
            TShardStatePtr shardState,
            NCloud::NProto::EStorageMediaKind mediaKind,
            bool useTwoStageRead,
            bool useCustomReadDataResponseParser,
            bool zeroCopyReadEnabled);
    };

    const IProfileLogPtr ProfileLog;
    const ITraceSerializerPtr TraceSerializer;
    // Keeps the storage containing MainInFlightRequest alive.
    const TInFlightRequestStoragePtr InFlightRequests;

    std::optional<TRequestState> RequestState;

public:
    TReadDataActor(
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        TInFlightRequestStoragePtr inFlightRequests);

    void Initialize(
        NProto::TReadDataRequest readRequest,
        TString logTag,
        ui32 blockSize,
        bool readBlobDisabled,
        IRequestStatsPtr requestStats,
        NActors::TActorId sender,
        ui64 cookie,
        TCallContextPtr callContext,
        TChecksumCalcInfo checksumCalcInfo,
        TInstant startTime,
        ui64 requestCookie,
        TString clientId,
        TShardStatePtr shardState,
        NCloud::NProto::EStorageMediaKind mediaKind,
        bool useTwoStageRead,
        bool useCustomReadDataResponseParser,
        bool zeroCopyReadEnabled);

    void Bootstrap(const TActorContext& ctx);

private:
    void StartRequest(const TActorContext& ctx);

    STFUNC(StateIdle);
    STFUNC(StateWork);

    void HandleStartReadData(
        const TEvStartReadData::TPtr& ev,
        const TActorContext& ctx);

    void Cleanup();

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

    NProto::TError ProcessExternalPayload(
        const TRope& payload,
        NProto::TReadDataResponse& readDataResponse);

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

TReadDataActor::TRequestState::TRequestState(
        NProto::TReadDataRequest readRequest,
        TString logTag,
        ui32 blockSize,
        bool readBlobDisabled,
        IRequestStatsPtr requestStats,
        NActors::TActorId sender,
        ui64 cookie,
        TCallContextPtr callContext,
        TChecksumCalcInfo checksumCalcInfo,
        TInstant startTime,
        ui64 requestCookie,
        TString clientId,
        TShardStatePtr shardState,
        NCloud::NProto::EStorageMediaKind mediaKind,
        bool useTwoStageRead,
        bool useCustomReadDataResponseParser,
        bool zeroCopyReadEnabled)
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
    , UseCustomReadDataResponseParser(useCustomReadDataResponseParser)
    // Zero-copy read optimization is only applicable when iovecs are provided.
    , ZeroCopyReadEnabled(
          zeroCopyReadEnabled && !ReadRequest.GetIovecs().empty())
{
}

TReadDataActor::TReadDataActor(
    IProfileLogPtr profileLog,
    ITraceSerializerPtr traceSerializer,
    TInFlightRequestStoragePtr inFlightRequests)
    : ProfileLog(std::move(profileLog))
    , TraceSerializer(std::move(traceSerializer))
    , InFlightRequests(std::move(inFlightRequests))
{}

void TReadDataActor::Initialize(
    NProto::TReadDataRequest readRequest,
    TString logTag,
    ui32 blockSize,
    bool readBlobDisabled,
    IRequestStatsPtr requestStats,
    NActors::TActorId sender,
    ui64 cookie,
    TCallContextPtr callContext,
    TChecksumCalcInfo checksumCalcInfo,
    TInstant startTime,
    ui64 requestCookie,
    TString clientId,
    TShardStatePtr shardState,
    NCloud::NProto::EStorageMediaKind mediaKind,
    bool useTwoStageRead,
    bool useCustomReadDataResponseParser,
    bool zeroCopyReadEnabled)
{
    Y_ABORT_UNLESS(!RequestState);
    RequestState.emplace(
        std::move(readRequest),
        std::move(logTag),
        blockSize,
        readBlobDisabled,
        std::move(requestStats),
        sender,
        cookie,
        std::move(callContext),
        std::move(checksumCalcInfo),
        startTime,
        requestCookie,
        std::move(clientId),
        std::move(shardState),
        mediaKind,
        useTwoStageRead,
        useCustomReadDataResponseParser,
        zeroCopyReadEnabled);
}

void TReadDataActor::Cleanup()
{
    RequestState.reset();
}

void TReadDataActor::Bootstrap(const TActorContext& ctx)
{
    if (RequestState) {
        StartRequest(ctx);
    } else {
        Become(&TThis::StateIdle);
    }
}

void TReadDataActor::HandleStartReadData(
    const TEvStartReadData::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    Initialize(
        std::move(msg->ReadRequest),
        std::move(msg->LogTag),
        msg->BlockSize,
        msg->ReadBlobDisabled,
        std::move(msg->RequestStats),
        msg->Sender,
        msg->Cookie,
        std::move(msg->CallContext),
        std::move(msg->ChecksumCalcInfo),
        msg->StartTime,
        msg->RequestCookie,
        std::move(msg->ClientId),
        std::move(msg->ShardState),
        msg->MediaKind,
        msg->UseTwoStageRead,
        msg->UseCustomReadDataResponseParser,
        msg->ZeroCopyReadEnabled);

    StartRequest(ctx);
}

void TReadDataActor::StartRequest(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(RequestState);
    Become(&TThis::StateWork);
    auto& state = *RequestState;

    if (state.UseTwoStageRead) {
        if (!state.ZeroCopyReadEnabled) {
            // BlockBuffer should not be initialized in constructor, because
            // creating a block buffer leads to memory allocation (and
            // initialization) which is heavy and we would like to execute that
            // on a separate thread (instead of this actor's parent thread)
            state.BlockBuffer->ReserveAndResize(state.ReadRequest.GetLength());
            state.TargetBuffers = CreateRope(
                state.BlockBuffer->begin(),
                state.BlockBuffer->size());
        } else {
            state.TargetBuffers = CreateRope(state.ReadRequest.GetIovecs());
        }
    }

    // Registering InFlightRequest here for the same reason - it's quite
    // expensive so we don't want to do it in TStorageServiceActor
    state.MainInFlightRequest = InFlightRequests->Register(
        state.Sender,
        state.Cookie,
        std::move(state.CallContext),
        state.MediaKind,
        std::move(state.ChecksumCalcInfo),
        state.RequestStats,
        state.StartTime,
        state.RequestCookie);

    InitProfileLogRequestInfo(
        state.MainInFlightRequest->AccessProfileLogRequest(),
        state.ReadRequest);
    state.MainInFlightRequest->AccessProfileLogRequest().SetClientId(
        std::move(state.ClientId));

    if (state.UseTwoStageRead) {
        DescribeData(ctx);
    } else {
        ReadData(ctx, {} /* fallbackReason */);
    }
}

void TReadDataActor::DescribeData(const TActorContext& ctx)
{
    auto& state = *RequestState;

    FILESTORE_TRACK(
        RequestReceived_ServiceWorker,
        state.MainInFlightRequest->CallContext,
        "DescribeData");

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "%s executing DescribeData for node: %lu, "
        "handle: %lu, offset: %lu, length: %lu",
        state.LogTag.c_str(),
        state.ReadRequest.GetNodeId(),
        state.ReadRequest.GetHandle(),
        state.ReadRequest.GetOffset(),
        state.ReadRequest.GetLength());

    auto request = std::make_unique<TEvIndexTablet::TEvDescribeDataRequest>();

    request->Record.MutableHeaders()->CopyFrom(state.ReadRequest.GetHeaders());
    request->Record.SetFileSystemId(state.ReadRequest.GetFileSystemId());
    request->Record.SetNodeId(state.ReadRequest.GetNodeId());
    request->Record.SetHandle(state.ReadRequest.GetHandle());
    request->Record.SetOffset(state.ReadRequest.GetOffset());
    request->Record.SetLength(state.ReadRequest.GetLength());

    auto describeCallContext = MakeIntrusive<TCallContext>(
        state.MainInFlightRequest->CallContext->FileSystemId,
        state.MainInFlightRequest->CallContext->RequestId);
    describeCallContext->SetRequestStartedCycles(GetCycleCount());
    describeCallContext->RequestType = EFileStoreRequest::DescribeData;
    if (!state.MainInFlightRequest->CallContext->LWOrbit.Fork(
            describeCallContext->LWOrbit))
    {
        FILESTORE_TRACK(
            ForkFailed,
            state.MainInFlightRequest->CallContext,
            GetFileStoreRequestName(EFileStoreRequest::DescribeData));
    }
    state.InFlightRequest.emplace(
        state.Sender,
        state.Cookie,
        std::move(describeCallContext),
        ProfileLog,
        state.MediaKind,
        state.RequestStats);
    request->CallContext = state.InFlightRequest->CallContext;

    state.InFlightRequest->Start(ctx.Now());
    InitProfileLogRequestInfo(
        state.InFlightRequest->AccessProfileLogRequest(),
        request->Record);
    TraceSerializer->BuildTraceRequest(
        *request->Record.MutableHeaders()->MutableInternal()->MutableTrace(),
        state.MainInFlightRequest->CallContext->LWOrbit);

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
    auto& state = *RequestState;
    const auto& LogTag = state.LogTag;

    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    SERVICE_VERIFY(state.InFlightRequest);

    state.MainInFlightRequest->CallContext->LWOrbit.Join(
        state.InFlightRequest->CallContext->LWOrbit);
    FinalizeProfileLogRequestInfo(
        state.InFlightRequest->AccessProfileLogRequest(),
        msg->Record);
    state.InFlightRequest->Complete(ctx.Now(), error);

    if (FAILED(msg->GetStatus())) {
        if (error.GetCode() != E_FS_THROTTLED) {
            ReadData(ctx, FormatError(error));
        } else {
            HandleError(ctx, error);
        }
        return;
    }

    const auto& backendInfo = msg->Record.GetHeaders().GetBackendInfo();
    state.ShardState->SetIsOverloaded(backendInfo.GetIsOverloaded());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "%s DescribeData succeeded %lu freshdata + %lu blobpieces"
        ", backend-info: %s",
        state.LogTag.c_str(),
        msg->Record.FreshDataRangesSize(),
        msg->Record.BlobPiecesSize(),
        backendInfo.ShortUtf8DebugString().Quote().c_str());

    state.DescribeResponse.CopyFrom(msg->Record);
    ReadBlobsIfNeeded(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::ReadBlobsIfNeeded(const TActorContext& ctx)
{
    auto& state = *RequestState;

    if (state.DescribeResponse.GetFakeResponse() || state.ReadBlobDisabled) {
        if (state.ReadBlobDisabled) {
            ReportFakeBlobWasRead();
            ReplyTwoStageAndDie(ctx);
            return;
        }

        ReportUnexpectedFakeDescribeDataResponse(state.LogTag);

        // It is better to hang IO, otherwise, returning a fatal error or
        // success could leave the filesystem in a broken or corrupted state
        auto error = MakeError(
            E_REJECTED,
            "misconfiguration: fake DescribeData received when "
            "ReadBlobDisabled=false");
        HandleError(ctx, std::move(error));
        return;
    }

    state.RemainingBlobsToRead = state.DescribeResponse.GetBlobPieces().size();
    if (state.RemainingBlobsToRead == 0) {
        ReplyTwoStageAndDie(ctx);
        return;
    }

    FILESTORE_TRACK(
        RequestReceived_ServiceWorker,
        state.MainInFlightRequest->CallContext,
        "ReadBlobs");

    auto readBlobCallContext = MakeIntrusive<TCallContext>(
        state.MainInFlightRequest->CallContext->FileSystemId,
        state.MainInFlightRequest->CallContext->RequestId);
    readBlobCallContext->SetRequestStartedCycles(GetCycleCount());
    readBlobCallContext->RequestType = EFileStoreRequest::ReadBlob;
    ui32 blobPieceId = 0;

    state.InFlightRequest.emplace(
        state.Sender,
        state.Cookie,
        std::move(readBlobCallContext),
        ProfileLog,
        state.MediaKind,
        state.RequestStats);
    state.InFlightRequest->Start(ctx.Now());

    for (const auto& blobPiece: state.DescribeResponse.GetBlobPieces()) {
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

        if (!state.MainInFlightRequest->CallContext->LWOrbit.Fork(
                request->Orbit))
        {
            FILESTORE_TRACK(
                ForkFailed,
                state.MainInFlightRequest->CallContext,
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
    auto& state = *RequestState;
    const auto& LogTag = state.LogTag;

    if (state.ReadDataFallbackEnabled) {
        // we don't need this response anymore

        return;
    }

    const auto* msg = ev->Get();
    state.MainInFlightRequest->CallContext->LWOrbit.Join(msg->Orbit);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "%s ReadBlobResponse count: %lu, status: %lu, cookie: %lu",
        state.LogTag.c_str(),
        msg->ResponseSz,
        (ui64)(msg->Status),
        ev->Cookie);

    if (msg->Status != NKikimrProto::OK) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s TEvBlobStorage::TEvGet failed: response: %s, group: %lu",
            state.LogTag.c_str(),
            msg->Print(false).c_str(),
            ev->Cookie < state.DescribeResponse.BlobPiecesSize()
                ? state.DescribeResponse.GetBlobPieces(ev->Cookie)
                      .GetBSGroupId()
                : 0);

        const NProto::TError error(
            MakeError(MAKE_KIKIMR_ERROR(msg->Status), msg->ErrorReason));

        state.InFlightRequest->Complete(ctx.Now(), error);

        const auto errorReason = FormatError(error);
        ReadData(ctx, errorReason);
        return;
    }

    SERVICE_VERIFY(ev->Cookie < state.DescribeResponse.BlobPiecesSize());
    const auto& blobPiece = state.DescribeResponse.GetBlobPieces(ev->Cookie);

    for (size_t i = 0; i < msg->ResponseSz; ++i) {
        SERVICE_VERIFY(i < blobPiece.RangesSize());

        const auto& blobPiece =
            state.DescribeResponse.GetBlobPieces(ev->Cookie);
        const auto& blobRange = blobPiece.GetRanges(i);
        const auto& response = msg->Responses[i];
        if (response.Status != NKikimrProto::OK) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s TEvBlobStorage::TEvGet query failed:"
                " status %s, response %s",
                state.LogTag.c_str(),
                NKikimrProto::EReplyStatus_Name(response.Status).c_str(),
                msg->Print(false).c_str());

            const auto error =
                MakeError(MAKE_KIKIMR_ERROR(response.Status), "read error");
            state.InFlightRequest->Complete(ctx.Now(), error);
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
                state.LogTag.c_str(),
                error.c_str());
            state.InFlightRequest->Complete(
                ctx.Now(),
                MakeError(E_FAIL, error));
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
            state.AlignedByteRange.Describe().c_str());
        Y_ABORT_UNLESS(
            blobRange.GetLength() == response.Buffer.size(),
            "Blob range length mismatch: all requested ranges: %s, response: "
            "#%lu, size is %lu",
            state.DescribeResponse.DebugString().Quote().c_str(),
            i,
            response.Buffer.size());
        SERVICE_VERIFY(blobRange.GetOffset() >= state.AlignedByteRange.Offset);

        const auto blobByteRange = TByteRange{
            blobRange.GetOffset(),
            blobRange.GetLength(),
            state.BlockSize};
        const auto commonRange = state.OriginByteRange.Intersect(blobByteRange);
        if (commonRange.Length != 0) {
            const auto relOffset =
                commonRange.Offset - state.OriginByteRange.Offset;
            auto dataIter = response.Buffer.begin();
            dataIter += commonRange.Offset - blobByteRange.Offset;
            TRopeUtils::Memcpy(
                state.TargetBuffers.Begin() + relOffset,
                dataIter,
                commonRange.Length);
            state.ZeroIntervals.PunchHole(
                relOffset,
                relOffset + commonRange.Length);
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "common range is empty: origin range: %s, blob range: %s",
                state.OriginByteRange.Describe().c_str(),
                blobByteRange.Describe().c_str());
        }
    }

    --state.RemainingBlobsToRead;
    if (state.RemainingBlobsToRead == 0) {
        state.InFlightRequest->Complete(ctx.Now(), {});

        ReplyTwoStageAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    if (!RequestState) {
        Die(ctx);
        return;
    }

    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::ReadData(
    const TActorContext& ctx,
    const TString& fallbackReason)
{
    auto& state = *RequestState;

    FILESTORE_TRACK(
        RequestReceived_ServiceWorker,
        state.MainInFlightRequest->CallContext,
        "ReadData");

    state.ReadDataFallbackEnabled = true;

    if (fallbackReason) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s falling back to ReadData: "
            "node: %lu, handle: %lu, offset: %lu, length: %lu. Message: %s",
            state.LogTag.c_str(),
            state.ReadRequest.GetNodeId(),
            state.ReadRequest.GetHandle(),
            state.ReadRequest.GetOffset(),
            state.ReadRequest.GetLength(),
            fallbackReason.Quote().c_str());
    }

    auto request = std::make_unique<TEvService::TEvReadDataRequest>();
    request->Record = std::move(state.ReadRequest);
    request->Record.MutableHeaders()->SetThrottlingDisabled(true);
    request->CallContext = state.MainInFlightRequest->CallContext;
    TraceSerializer->BuildTraceRequest(
        *request->Record.MutableHeaders()->MutableInternal()->MutableTrace(),
        state.MainInFlightRequest->CallContext->LWOrbit);

    // Original iovecs should be preserved in this request and pruned during
    // this forwarding on the tablet side
    state.ReadRequest.MutableIovecs()->Swap(request->Record.MutableIovecs());
    // Length should be preserved to validate payload size
    state.ReadRequest.SetLength(request->Record.GetLength());

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

NProto::TError TReadDataActor::ProcessExternalPayload(
    const TRope& payload,
    NProto::TReadDataResponse& readDataResponse)
{
    auto& state = *RequestState;

    ui64 bufferSize = readDataResponse.GetLength();
    if (payload.size() != bufferSize) {
        return MakeError(
            E_BADMSG,
            TStringBuilder()
                << "Payload has an incorrect size. Expected size: "
                << bufferSize << " Actual size: " << payload.size());
    }

    if (readDataResponse.GetBufferOffset() >= bufferSize) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "Incorrect buffer offset. Buffer offset: "
                             << readDataResponse.GetBufferOffset()
                             << " Buffer size : " << bufferSize);
    }

    auto it = payload.begin() + readDataResponse.GetBufferOffset();
    ui64 remainingBufferSize = bufferSize - readDataResponse.GetBufferOffset();
    if (!state.ReadRequest.GetIovecs().empty()) {
        if (remainingBufferSize > state.ReadRequest.GetLength()) {
            return MakeError(
                E_BADMSG,
                TStringBuilder()
                    << "Payload size is more than iovecs size. Expected size: "
                    << state.ReadRequest.GetLength()
                    << " Actual size: " << remainingBufferSize);
        }

        for (auto& iovec: state.ReadRequest.GetIovecs()) {
            ui64 dataToWrite = Min(iovec.GetLength(), remainingBufferSize);
            if (dataToWrite == 0) {
                break;
            }
            TRopeUtils::Memcpy(
                reinterpret_cast<char*>(iovec.GetBase()),
                it,
                dataToWrite);
            remainingBufferSize -= dataToWrite;
            it += dataToWrite;
        }

        if (remainingBufferSize != 0) {
            return MakeError(
                E_BADMSG,
                TStringBuilder()
                    << "Failed to read buffer from payload. "
                       " Expected buffer size: "
                    << bufferSize << " Actual bytes copied from payload: "
                    << bufferSize - remainingBufferSize);
        }
    } else {
        auto& buffer = *readDataResponse.MutableBuffer();
        buffer.ReserveAndResize(remainingBufferSize);
        TRopeUtils::Memcpy(buffer.begin(), it, remainingBufferSize);
    }

    // Set the buffer offset to 0 because the response buffer/iovecs do not
    // contain data before the offset anymore
    readDataResponse.SetBufferOffset(0);

    return {};
}

void TReadDataActor::HandleReadDataResponse(
    const TEvService::TEvReadDataResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto& state = *RequestState;

    auto response = std::make_unique<TEvService::TEvReadDataResponse>();
    bool isResponseParsed = false;
    if (state.UseCustomReadDataResponseParser &&
        !state.ReadRequest.GetIovecs().empty())
    {
        auto buffer = ev->GetChainBuffer();
        // extended format is not used for ReadDataResponse, but we check it
        // just in case to avoid parsing errors
        if (buffer && !buffer->GetSerializationInfo().IsExtendedFormat) {
            auto ret = ParseReadDataResponse(
                *buffer,
                response->Record,
                *state.ReadRequest.MutableIovecs());
            if (!HasError(ret)) {
                isResponseParsed = true;
            } else {
                // report critical event and fallback to the default parser
                ReportReadDataResponseParserFailed(FormatError(ret));
                // Abort execution to detect parser failures in the tests
                Y_DEBUG_ABORT_UNLESS(isResponseParsed);
            }
        }
    }

    if (!isResponseParsed) {
        auto* msg = ev->Get();
        response->Record = std::move(msg->Record);

        ui64 bufferSize = response->Record.GetLength();
        if (response->Record.GetBuffer().empty() && bufferSize != 0) {
            if (msg->GetPayloadCount() != 1) {
                HandleError(
                    ctx,
                    MakeError(
                        E_BADMSG,
                        TStringBuilder()
                            << "Payload is unavailable or message has more "
                               "than one payload. Payload count: "
                            << msg->GetPayloadCount()));
                return;
            }
            auto err =
                ProcessExternalPayload(msg->GetPayload(0), response->Record);
            if (HasError(err)) {
                HandleError(ctx, err);
                return;
            }
        }
    }

    auto& record = response->Record;
    if (HasError(record)) {
        HandleError(ctx, record.GetError());
        return;
    }

    const auto& backendInfo = record.GetHeaders().GetBackendInfo();
    state.ShardState->SetIsOverloaded(backendInfo.GetIsOverloaded());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "ReadData succeeded %lu data, backend-info: %s",
        record.GetBuffer().size(),
        backendInfo.ShortUtf8DebugString().Quote().c_str());

    MoveBufferToIovecsIfNeeded(ctx, record);

    SendResponseAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::MoveBufferToIovecsIfNeeded(
    const TActorContext& ctx,
    NProto::TReadDataResponse& response)
{
    auto& state = *RequestState;

    if (state.ReadRequest.GetIovecs().empty() || response.GetBuffer().empty()) {
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "%s copying data to target iovecs",
        state.LogTag.c_str());
    auto currentOffset = response.GetBufferOffset();
    for (const auto& iovec: state.ReadRequest.GetIovecs()) {
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
                state.LogTag.c_str(),
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
    auto& state = *RequestState;

    auto response = std::make_unique<TEvService::TEvReadDataResponse>();

    // we apply fresh data ranges to the buffer only after all blobs are
    // read and applied
    for (const auto& freshDataRange:
         state.DescribeResponse.GetFreshDataRanges())
    {
        ui64 offset = freshDataRange.GetOffset();
        const TString& content = freshDataRange.GetContent();

        ApplyFreshDataRange(
            ctx,
            freshDataRange,
            state.TargetBuffers,
            state.OriginByteRange,
            state.BlockSize,
            state.ReadRequest.GetOffset(),
            state.ReadRequest.GetLength(),
            state.DescribeResponse,
            state.ZeroIntervals);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s processed fresh data range size: %lu, offset: %lu",
            state.LogTag.c_str(),
            content.size(),
            offset);
    }

    for (const auto& zeroInterval: state.ZeroIntervals) {
        TRopeUtils::Memset(
            state.TargetBuffers.Begin() + zeroInterval.Start,
            0,
            zeroInterval.End - zeroInterval.Start);
    }

    // The actual file size may already be bigger than the returned one (see
    // TDescribeDataResponse::FileSize), it can only be used to clamp the read
    // range.
    const auto end =
        Min(state.DescribeResponse.GetFileSize(), state.OriginByteRange.End());
    if (end <= state.OriginByteRange.Offset) {
        state.BlockBuffer->clear();
    } else {
        const ui64 length = end - state.OriginByteRange.Offset;
        if (!state.ZeroCopyReadEnabled) {
            state.BlockBuffer->ReserveAndResize(length);
            response->Record.set_allocated_buffer(state.BlockBuffer.release());
        } else {
            response->Record.SetLength(length);
        }
    }

    MoveBufferToIovecsIfNeeded(ctx, response->Record);
    SendResponseAndDie(ctx, std::move(response));
}

void TReadDataActor::SendResponseAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvReadDataResponse> response)
{
    auto& state = *RequestState;

    FILESTORE_TRACK(
        ResponseSent_ServiceWorker,
        state.MainInFlightRequest->CallContext,
        "ReadData");

    CompleteRequestImpl<TEvService::TReadDataMethod>(
        ctx,
        response->Record,
        state.MainInFlightRequest,
        *InFlightRequests,
        state.RequestCookie);

    ctx.Send(state.Sender, response.release(), 0 /* flags */, state.Cookie);

    Cleanup();
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

STFUNC(TReadDataActor::StateIdle)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStartReadData, HandleStartReadData);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

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

    auto* session =
        GetAndValidateSession<TEvService::TReadDataMethod>(ctx, ev);
    if (!session) {
        return;
    }

    if (TryHandleControlNamespaceReadData(ctx, ev, session)) {
        return;
    }

    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);
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
        ProfileLog,
        TraceSerializer,
        InFlightRequests);
    actor->Initialize(
        std::move(msg->Record),
        filestore.GetFileSystemId(),
        filestore.GetBlockSize(),
        filestore.GetFeatures().GetReadBlobDisabled(),
        session->RequestStats,
        ev->Sender,
        ev->Cookie,
        std::move(msg->CallContext),
        std::move(checksumCalcInfo),
        startTime,
        GenerateRequestCookie(),
        session->ClientId,
        std::move(shardState),
        session->MediaKind,
        useTwoStageRead,
        // UseCustomReadDataResponseParser is deprecated and not compatible with
        // ExternalReadDataPayload
        filestore.GetFeatures().GetUseCustomReadDataResponseParser() &&
            !filestore.GetFeatures().GetExternalReadDataPayload(),
        filestore.GetFeatures().GetZeroCopyReadEnabled());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
