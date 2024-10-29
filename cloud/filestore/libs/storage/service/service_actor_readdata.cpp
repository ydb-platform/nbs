#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>
#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsTwoStageReadEnabled(const NProto::TFileStore& fs)
{
    const auto isHddLike = fs.GetStorageMediaKind() == NProto::STORAGE_MEDIA_HYBRID
        || fs.GetStorageMediaKind() == NProto::STORAGE_MEDIA_HDD;

    const auto disabledAsHdd = isHddLike &&
        fs.GetFeatures().GetTwoStageReadDisabledForHdd();
    return !disabledAsHdd && fs.GetFeatures().GetTwoStageReadEnabled();
}

////////////////////////////////////////////////////////////////////////////////

class TReadDataActor final: public TActorBootstrapped<TReadDataActor>
{
private:
    // Original request
    const TRequestInfoPtr RequestInfo;
    NProto::TReadDataRequest ReadRequest;

    // Filesystem-specific params
    const TString LogTag;
    const ui32 BlockSize;

    // Response data
    const TByteRange OriginByteRange;
    const TByteRange AlignedByteRange;
    IBlockBufferPtr BlockBuffer;
    NProtoPrivate::TDescribeDataResponse DescribeResponse;
    ui32 RemainingBlobsToRead = 0;
    bool ReadDataFallbackEnabled = false;

    // Stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;
    std::optional<TInFlightRequest> InFlightRequest;
    const NCloud::NProto::EStorageMediaKind MediaKind;

public:
    TReadDataActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadDataRequest readRequest,
        TString logTag,
        ui32 blockSize,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        NCloud::NProto::EStorageMediaKind mediaKind);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeData(const TActorContext& ctx);

    void HandleDescribeDataResponse(
        const TEvIndexTablet::TEvDescribeDataResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReadBlobIfNeeded(const TActorContext& ctx);

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

    void ReplyAndDie(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TReadDataActor::TReadDataActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadDataRequest readRequest,
        TString logTag,
        ui32 blockSize,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        NCloud::NProto::EStorageMediaKind mediaKind)
    : RequestInfo(std::move(requestInfo))
    , ReadRequest(std::move(readRequest))
    , LogTag(std::move(logTag))
    , BlockSize(blockSize)
    , OriginByteRange(
        ReadRequest.GetOffset(),
        ReadRequest.GetLength(),
        BlockSize)
    , AlignedByteRange(OriginByteRange.AlignedSuperRange())
    , BlockBuffer(CreateBlockBuffer(AlignedByteRange))
    , RequestStats(std::move(requestStats))
    , ProfileLog(std::move(profileLog))
    , MediaKind(mediaKind)
{
}

void TReadDataActor::Bootstrap(const TActorContext& ctx)
{
    DescribeData(ctx);
    Become(&TThis::StateWork);
}

void TReadDataActor::DescribeData(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "Executing DescribeData for %lu, %lu, %lu, %lu",
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
        RequestInfo->CallContext->FileSystemId,
        RequestInfo->CallContext->RequestId);
    describeCallContext->SetRequestStartedCycles(GetCycleCount());
    describeCallContext->RequestType = EFileStoreRequest::DescribeData;
    InFlightRequest.emplace(
        TRequestInfo(
            RequestInfo->Sender,
            RequestInfo->Cookie,
            std::move(describeCallContext)),
        ProfileLog,
        MediaKind,
        RequestStats);

    InFlightRequest->Start(ctx.Now());
    InitProfileLogRequestInfo(
        InFlightRequest->ProfileLogRequest,
        request->Record);

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

char* GetDataPtr(
    ui64 offset,
    TByteRange alignedByteRange,
    ui32 blockSize,
    IBlockBuffer& buffer)
{

    const ui64 relOffset = offset - alignedByteRange.Offset;
    const ui32 blockNo = relOffset / blockSize;
    const auto block = buffer.GetBlock(blockNo);
    return const_cast<char*>(block.data()) + relOffset - blockNo * blockSize;
}

////////////////////////////////////////////////////////////////////////////////

void ApplyFreshDataRange(
    const TActorContext& ctx,
    const NProtoPrivate::TFreshDataRange& sourceFreshData,
    IBlockBuffer& targetBuffer,
    TByteRange alignedTargetByteRange,
    ui32 blockSize,
    ui64 offset,
    ui64 length,
    const NProtoPrivate::TDescribeDataResponse& describeResponse)
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
        alignedTargetByteRange.Describe().c_str(),
        offset,
        length,
        DescribeResponseDebugString(describeResponse).Quote().c_str());

    auto commonRange = sourceByteRange.Intersect(alignedTargetByteRange);

    Y_ABORT_UNLESS(sourceByteRange == commonRange);
    if (commonRange.Length == 0) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "common range is empty: source: %s, target: %s",
            sourceByteRange.Describe().c_str(),
            alignedTargetByteRange.Describe().c_str());
        return;
    }
    char* targetData = GetDataPtr(
        commonRange.Offset,
        alignedTargetByteRange,
        blockSize,
        targetBuffer);

    // NB: we assume that underlying target data is a continuous buffer
    // TODO: don't make such an assumption - use GetBlock(i) API
    memcpy(
        targetData,
        sourceFreshData.GetContent().data() +
            (commonRange.Offset - sourceByteRange.Offset),
        commonRange.Length);
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::HandleDescribeDataResponse(
    const TEvIndexTablet::TEvDescribeDataResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TABLET_VERIFY(InFlightRequest);

    InFlightRequest->Complete(ctx.Now(), msg->GetError());
    FinalizeProfileLogRequestInfo(
        InFlightRequest->ProfileLogRequest,
        msg->Record);

    if (FAILED(msg->GetStatus())) {
        ReadData(ctx, FormatError(msg->GetError()));
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "DescribeData succeeded %lu freshdata + %lu blobpieces",
        msg->Record.FreshDataRangesSize(),
        msg->Record.BlobPiecesSize());

    DescribeResponse.CopyFrom(msg->Record);
    ReadBlobIfNeeded(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::ReadBlobIfNeeded(const TActorContext& ctx)
{
    RemainingBlobsToRead = DescribeResponse.GetBlobPieces().size();
    if (RemainingBlobsToRead == 0) {
        ReplyAndDie(ctx);
        return;
    }

    auto readBlobCallContext = MakeIntrusive<TCallContext>(
        RequestInfo->CallContext->FileSystemId,
        RequestInfo->CallContext->RequestId);
    readBlobCallContext->SetRequestStartedCycles(GetCycleCount());
    readBlobCallContext->RequestType = EFileStoreRequest::ReadBlob;
    ui32 blobPieceId = 0;

    InFlightRequest.emplace(
        TRequestInfo(
            RequestInfo->Sender,
            RequestInfo->Cookie,
            std::move(readBlobCallContext)),
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
            blobPiece.DebugString().c_str(),
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

    if (msg->Status != NKikimrProto::OK) {
        const auto errorReason = FormatError(
            MakeError(MAKE_KIKIMR_ERROR(msg->Status), msg->ErrorReason));
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "ReadBlob error: %s",
            errorReason.c_str());
        ReadData(ctx, errorReason);

        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "ReadBlobResponse count: %lu, status: %lu, cookie: %lu",
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
            const auto errorReason = FormatError(
                MakeError(MAKE_KIKIMR_ERROR(response.Status), "read error"));
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "ReadBlob error: %s",
                errorReason.c_str());
            ReadData(ctx, errorReason);

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
                "ReadBlob error: %s",
                error.c_str());
            ReadData(ctx, error);

            return;
        }

        auto dataIter = response.Buffer.begin();
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "ReadBlobResponse: blobId: %s, offset: %lu, length: %lu, size: "
            "%lu, target: %s",
            blobPiece.GetBlobId().DebugString().c_str(),
            blobRange.GetBlobOffset(),
            blobRange.GetLength(),
            response.Buffer.size(),
            AlignedByteRange.Describe().c_str());
        Y_ABORT_UNLESS(
            blobRange.GetLength() == response.Buffer.size(),
            "Blob range length mismatch: all requested ranges: %s, response: "
            "#%lu, size is %lu",
            DescribeResponse.DebugString().c_str(),
            i,
            response.Buffer.size());
        TABLET_VERIFY(blobRange.GetOffset() >= AlignedByteRange.Offset);

        char* targetData = GetDataPtr(
            blobRange.GetOffset(),
            AlignedByteRange,
            BlockSize,
            *BlockBuffer);

        dataIter.ExtractPlainDataAndAdvance(targetData, blobRange.GetLength());
    }

    --RemainingBlobsToRead;
    if (RemainingBlobsToRead == 0) {
        InFlightRequest->Complete(ctx.Now(), {});

        ReplyAndDie(ctx);
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
    ReadDataFallbackEnabled = true;

    LOG_WARN(
        ctx,
        TFileStoreComponents::SERVICE,
        "Falling back to ReadData for %lu, %lu, %lu, %lu. Message: %s",
        ReadRequest.GetNodeId(),
        ReadRequest.GetHandle(),
        ReadRequest.GetOffset(),
        ReadRequest.GetLength(),
        fallbackReason.Quote().c_str());

    auto request = std::make_unique<TEvService::TEvReadDataRequest>();
    request->Record = std::move(ReadRequest);
    request->Record.MutableHeaders()->SetThrottlingDisabled(true);

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TReadDataActor::HandleReadDataResponse(
    const TEvService::TEvReadDataResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        HandleError(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "ReadData succeeded %lu data",
        msg->Record.GetBuffer().size());

    auto response = std::make_unique<TEvService::TEvReadDataResponse>();
    response->Record = std::move(msg->Record);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::ReplyAndDie(const TActorContext& ctx)
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
            *BlockBuffer,
            AlignedByteRange,
            BlockSize,
            ReadRequest.GetOffset(),
            ReadRequest.GetLength(),
            DescribeResponse);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "processed fresh data range size: %lu, offset: %lu",
            content.size(),
            offset);
    }

    CopyFileData(
        LogTag,
        OriginByteRange,
        AlignedByteRange,
        DescribeResponse.GetFileSize(),
        *BlockBuffer,
        response->Record.MutableBuffer());

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TReadDataActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvReadDataResponse>(
        std::move(error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
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
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleReadData(
    const TEvService::TEvReadDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

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

    auto [fsId, error] = SelectShard(
        ctx,
        sessionId,
        seqNo,
        msg->Record.GetHeaders().GetDisableMultiTabletForwarding(),
        TEvService::TReadDataMethod::Name,
        msg->CallContext->RequestId,
        filestore,
        ExtractShardNo(msg->Record.GetHandle()));

    if (HasError(error)) {
        auto response = std::make_unique<TEvService::TEvReadDataResponse>(
            std::move(error));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    if (fsId) {
        msg->Record.SetFileSystemId(fsId);
    }

    if (!IsTwoStageReadEnabled(filestore)) {
        // If two-stage read is disabled, forward the request to the tablet in
        // the same way as all other requests.
        ForwardRequest<TEvService::TReadDataMethod>(ctx, ev);
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "read data %s",
        msg->Record.DebugString().c_str());

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        session->MediaKind,
        session->RequestStats,
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(SelfId(), cookie, msg->CallContext);

    auto actor = std::make_unique<TReadDataActor>(
        std::move(requestInfo),
        std::move(msg->Record),
        filestore.GetFileSystemId(),
        filestore.GetBlockSize(),
        session->RequestStats,
        ProfileLog,
        session->MediaKind);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
