#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/iterator/enumerate.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsThreeStageWriteEnabled(const NProto::TFileStore& fs)
{
    const auto isHdd = fs.GetStorageMediaKind() == NProto::STORAGE_MEDIA_HYBRID
        || fs.GetStorageMediaKind() == NProto::STORAGE_MEDIA_HDD;
    const auto disabledAsHdd = isHdd &&
        fs.GetFeatures().GetThreeStageWriteDisabledForHDD();
    return !disabledAsHdd && fs.GetFeatures().GetThreeStageWriteEnabled();
}

////////////////////////////////////////////////////////////////////////////////

struct TIoVecContiguousChunk: public IContiguousChunk
{
    ui64 Base = 0;
    ui64 Size = 0;

    TIoVecContiguousChunk(ui64 base, ui64 size)
        : Base(base)
        , Size(size)
    {}

    TContiguousSpan GetData() const override
    {
        return TContiguousSpan(reinterpret_cast<const char*>(Base), Size);
    }

    TMutableContiguousSpan GetDataMut() override
    {
        return TMutableContiguousSpan(reinterpret_cast<char*>(Base), Size);
    }

    size_t GetOccupiedMemorySize() const override
    {
        return Size;
    }
};

////////////////////////////////////////////////////////////////////////////////

TRope CreateRopeFromIovecs(const NProto::TWriteDataRequest& request)
{
    const auto& iovecs = request.GetIovecs();
    TRope rope;
    for (const auto& iovec: iovecs) {
        rope.Insert(
            rope.End(),
            TRope(TRcBuf(
                MakeIntrusive<TIoVecContiguousChunk>(
                    iovec.GetBase(),
                    iovec.GetLength()))));
    }
    return rope;
}

/**
 * @brief Copies a slice of data from the iovecs in the request into the buffer
 *        and returns the buffer with data
 *
 * @param request      The write request containing iovecs as the source of data
 * @param byteOffset   The starting offset (in bytes) from the beginning of the
 *                     iovecs
 * @param byteLength   The number of bytes to copy from the offset
 *
 * @return The destination buffer to copy data into
 *
 * @note It is assumed that byteOffset + byteLength does not exceed the total
 *       size of the data in the iovecs.
 */
TString GetBufferFromRope(TRope& rope, ui64 byteOffset, ui64 bufferLength)
{
    TString buffer;
    buffer.ReserveAndResize(bufferLength);
    auto bytesCopied = TRopeUtils::SafeMemcpy(
        &buffer[0],
        rope.Begin() + byteOffset,
        bufferLength);
    Y_ABORT_UNLESS(bytesCopied == bufferLength);
    return buffer;
}

/**
 * @brief Copies a slice of data from the iovecs in the request into the buffer.
 *
 * @param request      The write request containing iovecs as the source of data
 * @param buffer       The destination buffer to copy data into
 * @param byteOffset   The starting offset (in bytes) from the beginning of the
 *                     iovecs
 * @param byteLength   The number of bytes to copy from the offset
 *
 * @note It is assumed that byteOffset + byteLength does not exceed the total
 *       size of the data in the iovecs.
 */
void CopyIovecs(
    const NProto::TWriteDataRequest& request,
    TString& buffer,
    ui64 byteOffset,
    ui64 byteLength)
{
    if (request.GetIovecs().empty()) {
        return;
    }

    auto rope = CreateRopeFromIovecs(request);
    buffer.ReserveAndResize(byteLength);
    auto bytesCopied = TRopeUtils::SafeMemcpy(
        &buffer[0],
        rope.Begin() + byteOffset,
        byteLength);
    Y_ABORT_UNLESS(bytesCopied == byteLength);
}

/**
 * @brief Copies a slice of data from the iovecs in the request into the buffer
 *         in the same request and cleanup iovecs.
 *
 * @param request      The write request containing iovecs as the source of data
 */
void MoveIovecsToBuffer(NProto::TWriteDataRequest& request)
{
    CopyIovecs(
        request,
        *request.MutableBuffer(),
        0,
        CalculateByteCount(request));
    request.MutableIovecs()->Clear();
}

////////////////////////////////////////////////////////////////////////////////

class TWriteDataActor final: public TActorBootstrapped<TWriteDataActor>
{
private:
    // Original request
    NProto::TWriteDataRequest WriteRequest;
    const TByteRange Range;
    const TByteRange BlobRange;
    const TRequestInfoPtr RequestInfo;

    // Filesystem-specific params
    const TString LogTag;

    // generated blob id and associated data
    NProtoPrivate::TGenerateBlobIdsResponse GenerateBlobIdsResponse;

    // WriteData state
    ui32 RemainingBlobsToWrite = 0;
    bool WriteDataFallbackEnabled = false;

    // Stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;
    // Refers to GenerateBlobIds or AddData request, depending on which one is
    // in flight
    TMaybe<TInFlightRequest> InFlightRequest;
    TVector<std::unique_ptr<TInFlightRequest>> InFlightBSRequests;
    TVector<ui32> StorageStatusFlags;
    TVector<double> ApproximateFreeSpaceShares;
    const NCloud::NProto::EStorageMediaKind MediaKind;
    TRope Rope;

public:
    TWriteDataActor(
            NProto::TWriteDataRequest request,
            TByteRange range,
            TRequestInfoPtr requestInfo,
            TString logTag,
            IRequestStatsPtr requestStats,
            IProfileLogPtr profileLog,
            NCloud::NProto::EStorageMediaKind mediaKind)
        : WriteRequest(std::move(request))
        , Range(range)
        , BlobRange(Range.AlignedSubRange())
        , RequestInfo(std::move(requestInfo))
        , LogTag(std::move(logTag))
        , RequestStats(std::move(requestStats))
        , ProfileLog(std::move(profileLog))
        , MediaKind(mediaKind)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Rope = CreateRopeFromIovecs(WriteRequest);

        auto request =
            std::make_unique<TEvIndexTablet::TEvGenerateBlobIdsRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(WriteRequest.GetNodeId());
        request->Record.SetHandle(WriteRequest.GetHandle());
        request->Record.SetOffset(BlobRange.Offset);
        request->Record.SetLength(BlobRange.Length);

        auto genCallContext = MakeIntrusive<TCallContext>(
            RequestInfo->CallContext->FileSystemId,
            RequestInfo->CallContext->RequestId);
        genCallContext->SetRequestStartedCycles(GetCycleCount());
        genCallContext->RequestType = EFileStoreRequest::GenerateBlobIds;
        InFlightRequest.ConstructInPlace(
            TRequestInfo(
                RequestInfo->Sender,
                RequestInfo->Cookie,
                std::move(genCallContext)),
            ProfileLog,
            MediaKind,
            RequestStats);
        InFlightRequest->Start(ctx.Now());
        InitProfileLogRequestInfo(
            InFlightRequest->ProfileLogRequest,
            request->Record);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s WriteDataActor started, data size: %lu, offset: %lu, aligned "
            "size: %lu, aligned offset: %lu",
            LogTag.c_str(),
            CalculateByteCount(WriteRequest),
            WriteRequest.GetOffset(),
            BlobRange.Length,
            BlobRange.Offset);

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());

        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(
                TEvIndexTablet::TEvGenerateBlobIdsResponse,
                HandleGenerateBlobIdsResponse);

            HFunc(TEvBlobStorage::TEvPutResult, HandleWriteBlobResponse);

            HFunc(TEvIndexTablet::TEvAddDataResponse, HandleAddDataResponse);

            HFunc(TEvService::TEvWriteDataResponse, HandleWriteDataResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE_WORKER,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleGenerateBlobIdsResponse(
        const TEvIndexTablet::TEvGenerateBlobIdsResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        const auto& error = msg->GetError();

        TABLET_VERIFY(InFlightRequest);

        InFlightRequest->Complete(ctx.Now(), error);
        FinalizeProfileLogRequestInfo(
            InFlightRequest->ProfileLogRequest,
            msg->Record);

        if (HasError(error)) {
            if (error.GetCode() != E_FS_THROTTLED) {
                WriteData(ctx, error);
            } else {
                HandleError(ctx, error);
            }
            return;
        }

        GenerateBlobIdsResponse.CopyFrom(msg->Record);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s GenerateBlobIds response received: %s",
            LogTag.c_str(),
            GenerateBlobIdsResponse.DebugString().Quote().c_str());

        WriteBlobs(ctx);
    }

    void WriteBlobs(const TActorContext& ctx)
    {
        RemainingBlobsToWrite = GenerateBlobIdsResponse.BlobsSize();
        ui64 offset = BlobRange.Offset - Range.Offset;

        InFlightBSRequests.reserve(RemainingBlobsToWrite);
        StorageStatusFlags.resize(GenerateBlobIdsResponse.BlobsSize());
        ApproximateFreeSpaceShares.resize(GenerateBlobIdsResponse.BlobsSize());

        const auto& iovecs = WriteRequest.GetIovecs();
        auto ropeIt = Rope.Begin();
        if (!Rope.empty()) {
            ropeIt += offset;
        }
        for (const auto& blob: GenerateBlobIdsResponse.GetBlobs()) {
            NKikimr::TLogoBlobID blobId =
                LogoBlobIDFromLogoBlobID(blob.GetBlobId());

            auto writeBlobCallContext = MakeIntrusive<TCallContext>(
                RequestInfo->CallContext->FileSystemId,
                RequestInfo->CallContext->RequestId);
            writeBlobCallContext->SetRequestStartedCycles(GetCycleCount());
            writeBlobCallContext->RequestType = EFileStoreRequest::WriteBlob;
            InFlightBSRequests.emplace_back(std::make_unique<TInFlightRequest>(
                TRequestInfo(
                    RequestInfo->Sender,
                    RequestInfo->Cookie,
                    std::move(writeBlobCallContext)),
                ProfileLog,
                MediaKind,
                RequestStats));
            InFlightBSRequests.back()->Start(ctx.Now());

            std::unique_ptr<TEvBlobStorage::TEvPut> request;

            if (!iovecs.empty()) {
                // TODO(myagkov): Implement TEvPut with TRope as a buffer
                // to remove unnecessary memcpy
                TString putData;
                putData.ReserveAndResize(blobId.BlobSize());
                auto bytesCopied = TRopeUtils::SafeMemcpy(
                    &putData[0],
                    ropeIt,
                    blobId.BlobSize());
                Y_ABORT_UNLESS(bytesCopied == blobId.BlobSize());
                request = std::make_unique<TEvBlobStorage::TEvPut>(
                    blobId,
                    std::move(putData),
                    TInstant::Max(),
                    NKikimrBlobStorage::UserData);
                ropeIt += blobId.BlobSize();
            } else {
                if (GenerateBlobIdsResponse.BlobsSize() == 1 &&
                    Range == BlobRange)
                {
                    // do not copy the buffer if there is only one blob
                    request = std::make_unique<TEvBlobStorage::TEvPut>(
                        blobId,
                        WriteRequest.GetBuffer(),
                        TInstant::Max(),
                        NKikimrBlobStorage::UserData);
                } else {
                    request = std::make_unique<TEvBlobStorage::TEvPut>(
                        blobId,
                        TString(
                            WriteRequest.GetBuffer().data() + offset,
                            blobId.BlobSize()),
                        TInstant::Max(),
                        NKikimrBlobStorage::UserData);
                }
                offset += blobId.BlobSize();
            }

            NKikimr::TActorId proxy =
                MakeBlobStorageProxyID(blob.GetBSGroupId());
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s Sending TEvPut request to blob storage, blobId: %s, proxy: "
                "%s iovecs size: %ul",
                LogTag.c_str(),
                blobId.ToString().c_str(),
                proxy.ToString().c_str(),
                WriteRequest.GetIovecs().size());
            SendToBSProxy(ctx, proxy, request.release(), blobId.Cookie());
        }
    }

    void HandleWriteBlobResponse(
        const TEvBlobStorage::TEvPutResult::TPtr& ev,
        const TActorContext& ctx)
    {
        if (WriteDataFallbackEnabled) {
            return;
        }
        const auto* msg = ev->Get();

        if (msg->Status != NKikimrProto::OK) {
            const auto error =
                MakeError(MAKE_KIKIMR_ERROR(msg->Status), msg->ErrorReason);
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s WriteData error: %s",
                LogTag.c_str(),
                msg->ErrorReason.Quote().c_str());
            // We still may receive some responses, but we do not want to
            // process them
            for (auto& inFlight: InFlightBSRequests) {
                if (inFlight && !inFlight->IsCompleted()) {
                    inFlight->Complete(ctx.Now(), error);
                }
            }
            return WriteData(ctx, error);
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s TEvPutResult response received: %s",
            LogTag.c_str(),
            msg->ToString().c_str());

        ui64 blobIdx = msg->Id.Cookie();
        // It is implicitly expected that cookies are generated in increasing
        // order starting from 0.
        // TODO: replace this TABLET_VERIFY with a critical event + WriteData
        // fallback
        TABLET_VERIFY(
            blobIdx < InFlightBSRequests.size() &&
            InFlightBSRequests[blobIdx] &&
            !InFlightBSRequests[blobIdx]->IsCompleted());
        InFlightBSRequests[blobIdx]->Complete(ctx.Now(), {});
        StorageStatusFlags[blobIdx] = msg->StatusFlags.Raw;
        ApproximateFreeSpaceShares[blobIdx] = msg->ApproximateFreeSpaceShare;

        --RemainingBlobsToWrite;
        if (RemainingBlobsToWrite == 0) {
            AddData(ctx);
        }
    }

    void AddData(const TActorContext& ctx)
    {
        auto request = std::make_unique<TEvIndexTablet::TEvAddDataRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(WriteRequest.GetNodeId());
        request->Record.SetHandle(WriteRequest.GetHandle());
        request->Record.SetOffset(WriteRequest.GetOffset());
        request->Record.SetLength(CalculateByteCount(WriteRequest));
        for (auto& blob: *GenerateBlobIdsResponse.MutableBlobs()) {
            request->Record.AddBlobIds()->Swap(blob.MutableBlobId());
        }
        request->Record.SetCommitId(GenerateBlobIdsResponse.GetCommitId());
        request->Record.MutableStorageStatusFlags()->Reserve(
            StorageStatusFlags.size());
        for (const auto flags: StorageStatusFlags) {
            request->Record.AddStorageStatusFlags(flags);
        }
        request->Record.MutableApproximateFreeSpaceShares()->Reserve(
            ApproximateFreeSpaceShares.size());
        for (const auto share: ApproximateFreeSpaceShares) {
            request->Record.AddApproximateFreeSpaceShares(share);
        }

        if (Range.Offset < BlobRange.Offset) {
            auto& unalignedHead = *request->Record.AddUnalignedDataRanges();
            unalignedHead.SetOffset(Range.Offset);
            const auto length = BlobRange.Offset - Range.Offset;
            if (WriteRequest.GetIovecs().empty()) {
                unalignedHead.SetContent(
                    WriteRequest.GetBuffer().substr(0, length));
            } else {
                unalignedHead.SetContent(GetBufferFromRope(Rope, 0, length));
            }
        }

        if (Range.End() > BlobRange.End()) {
            auto& unalignedTail = *request->Record.AddUnalignedDataRanges();
            unalignedTail.SetOffset(BlobRange.End());
            const auto offset = BlobRange.End() - Range.Offset;
            const auto length = Range.End() - BlobRange.End();
            if (WriteRequest.GetIovecs().empty()) {
                unalignedTail.SetContent(
                    WriteRequest.GetBuffer().substr(offset, length));
            } else {
                unalignedTail.SetContent(
                    GetBufferFromRope(Rope, offset, length));
            }
        }

        auto addCallContext = MakeIntrusive<TCallContext>(
            RequestInfo->CallContext->FileSystemId,
            RequestInfo->CallContext->RequestId);
        addCallContext->SetRequestStartedCycles(GetCycleCount());
        addCallContext->RequestType = EFileStoreRequest::AddData;
        InFlightRequest.ConstructInPlace(
            TRequestInfo(
                RequestInfo->Sender,
                RequestInfo->Cookie,
                std::move(addCallContext)),
            ProfileLog,
            MediaKind,
            RequestStats);
        InFlightRequest->Start(ctx.Now());
        InitProfileLogRequestInfo(
            InFlightRequest->ProfileLogRequest,
            request->Record);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Sending AddData request to tablet: %s",
            LogTag.c_str(),
            request->Record.DebugString().Quote().c_str());

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void HandleAddDataResponse(
        const TEvIndexTablet::TEvAddDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        TABLET_VERIFY(InFlightRequest);
        InFlightRequest->Complete(ctx.Now(), msg->GetError());
        FinalizeProfileLogRequestInfo(
            InFlightRequest->ProfileLogRequest,
            msg->Record);

        if (HasError(msg->GetError())) {
            return WriteData(ctx, msg->GetError());
        }

        auto response = std::make_unique<TEvService::TEvWriteDataResponse>();
        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        Die(ctx);
    }

    /**
     * @brief Fallback to regular write if two-stage write fails for any reason
     */
    void WriteData(const TActorContext& ctx, const NProto::TError& error)
    {
        WriteDataFallbackEnabled = true;

        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Falling back to WriteData for %lu, %lu, %lu (%lu bytes). "
            "Message: %s",
            LogTag.c_str(),
            WriteRequest.GetNodeId(),
            WriteRequest.GetHandle(),
            WriteRequest.GetOffset(),
            CalculateByteCount(WriteRequest),
            FormatError(error).Quote().c_str());

        MoveIovecsToBuffer(WriteRequest);
        auto request = std::make_unique<TEvService::TEvWriteDataRequest>();
        request->Record = std::move(WriteRequest);
        request->Record.MutableHeaders()->SetThrottlingDisabled(true);

        // forward request through tablet proxy
        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void HandleWriteDataResponse(
        const TEvService::TEvWriteDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        if (HasError(msg->GetError())) {
            HandleError(ctx, msg->GetError());
            return;
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s WriteData succeeded",
            LogTag.c_str());

        auto response = std::make_unique<TEvService::TEvWriteDataResponse>();
        response->Record = std::move(msg->Record);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        Die(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx)
    {
        auto response = std::make_unique<TEvService::TEvWriteDataResponse>();
        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        Die(ctx);
    }

    void HandleError(const TActorContext& ctx, const NProto::TError& error)
    {
        auto response =
            std::make_unique<TEvService::TEvWriteDataResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleWriteData(
    const TEvService::TEvWriteDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    TInstant startTime = ctx.Now();
    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<TEvService::TEvWriteDataResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }
    const NProto::TFileStore& filestore = session->FileStore;

    // In handleless IO mode, if the handle is not set, we use the nodeId to
    // infer the shard number
    ui32 shardNo = ExtractShardNo(
        filestore.GetFeatures().GetAllowHandlelessIO() &&
                msg->Record.GetHandle() == InvalidHandle
            ? msg->Record.GetNodeId()
            : msg->Record.GetHandle());

    auto [fsId, error] = SelectShard(
        ctx,
        sessionId,
        seqNo,
        msg->Record.GetHeaders().GetDisableMultiTabletForwarding(),
        TEvService::TWriteDataMethod::Name,
        msg->CallContext->RequestId,
        filestore,
        shardNo);

    if (HasError(error)) {
        auto response = std::make_unique<TEvService::TEvWriteDataResponse>(
            std::move(error));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    if (fsId) {
        msg->Record.SetFileSystemId(fsId);
    }

    if (!msg->Record.GetIovecs().empty() && msg->Record.GetBufferOffset() != 0)
    {
        auto response = std::make_unique<TEvService::TEvWriteDataResponse>(
            ErrorInvalidArgument(
                "The BufferOffset option is not compatible with Iovecs"));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    const auto threeStageWriteAllowed = IsThreeStageWriteEnabled(filestore);

    if (!threeStageWriteAllowed) {
        // If three-stage write is disabled, forward the request to the tablet
        // in the same way as all other requests.
        MoveIovecsToBuffer(msg->Record);
        ForwardRequest<TEvService::TWriteDataMethod>(ctx, ev);
        return;
    }

    if (!filestore.GetFeatures().GetZeroCopyWriteEnabled()) {
        MoveIovecsToBuffer(msg->Record);
    }

    ui32 blockSize = filestore.GetBlockSize();

    const auto bytesCount = CalculateByteCount(msg->Record);
    const TByteRange range(
        msg->Record.GetOffset(),
        bytesCount,
        blockSize);
    const bool threeStageWriteEnabled =
        range.Length >= filestore.GetFeatures().GetThreeStageWriteThreshold() &&
        threeStageWriteAllowed &&
        (range.IsAligned() ||
         StorageConfig->GetUnalignedThreeStageWriteEnabled()) &&
        range.AlignedSubRange().Length > 0;

    if (threeStageWriteEnabled) {
        auto logTag = filestore.GetFileSystemId();
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Using three-stage write for request, size: %lu",
            logTag.c_str(),
            bytesCount);

        auto [cookie, inflight] = CreateInFlightRequest(
            TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            session->MediaKind,
            session->RequestStats,
            startTime);

        InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

        auto requestInfo =
            CreateRequestInfo(SelfId(), cookie, msg->CallContext);

        auto actor = std::make_unique<TWriteDataActor>(
            std::move(msg->Record),
            range,
            std::move(requestInfo),
            std::move(logTag),
            session->RequestStats,
            ProfileLog,
            session->MediaKind);
        NCloud::Register(ctx, std::move(actor));
    } else {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Forwarding WriteData request to tablet");
        MoveIovecsToBuffer(msg->Record);
        return ForwardRequest<TEvService::TWriteDataMethod>(ctx, ev);
    }
}

}   // namespace NCloud::NFileStore::NStorage
