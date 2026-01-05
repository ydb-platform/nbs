#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/probes.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>
#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/iterator/enumerate.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

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

template <bool Owner = false>
class TIovecContiguousChunk: public IContiguousChunk
{
private:
    char* Base = 0;
    ui64 Size = 0;

public:
    TIovecContiguousChunk(ui64 base, ui64 size)
        : Base(reinterpret_cast<char*>(base))
        , Size(size)
    {
        static_assert(sizeof(ui64) == sizeof(char*));
    }

    ~TIovecContiguousChunk() {
        if constexpr(Owner) {
            delete[] Base;
        }
    }

    TContiguousSpan GetData() const override
    {
        return TContiguousSpan(Base, Size);
    }

    TMutableContiguousSpan UnsafeGetDataMut() override
    {
        return TMutableContiguousSpan(Base, Size);
    }

    IContiguousChunk::TPtr Clone() override
    {
        auto copy = new char[Size];
        ::memcpy(copy, Base, Size);

        return MakeIntrusive<TIovecContiguousChunk<true>>(
            reinterpret_cast<ui64>(copy),
            Size);
    }

    size_t GetOccupiedMemorySize() const override
    {
        return Size;
    }
};

////////////////////////////////////////////////////////////////////////////////

TRope CreateRopeFromIovecs(const NProto::TWriteDataRequest& request)
{
    TRope rope;
    for (const auto& iovec: request.GetIovecs()) {
        if (iovec.GetLength() == 0) {
            continue;
        }
        rope.Insert(
            rope.End(),
            TRope(TRcBuf(
                MakeIntrusive<TIovecContiguousChunk<>>(
                    iovec.GetBase(),
                    iovec.GetLength()))));
    }
    return rope;
}

/**
 * @brief Copies a slice of data from the rope in the request into the buffer
 *        and returns the buffer with data
 *
 * @param rope         The rope as the source of data
 * @param buffer       The destination buffer to copy data into
 * @param bytesToSkip  The starting offset (in bytes) from the beginning of the
 *                     iovecs
 * @param bytesToCopy  The number of bytes to copy from the offset
 *
 * @return The number of bytes actually copied. Fewer bytes than `byteLength`
 *         may be copied if `byteOffset + byteLength` exceeds the total size
 *         of the data in the rope.
 */
ui64 CopyBufferFromRope(
    TRope& rope,
    TString& buffer,
    ui64 bytesToSkip,
    ui64 bytesToCopy)
{
    buffer.ReserveAndResize(bytesToCopy);
    auto bytesCopied = TRopeUtils::SafeMemcpy(
        &buffer[0],
        rope.Begin() + bytesToSkip,
        bytesToCopy);
    return bytesCopied;
}

/**
 * @brief Copies a slice of data from the iovecs in the request into the buffer
 *        in the same request and cleanup iovecs.
 *
 * @param request The write request containing iovecs as the source of data
 */
void MoveIovecsToBuffer(NProto::TWriteDataRequest& request)
{
    if (request.GetIovecs().empty()) {
        return;
    }

    auto rope = CreateRopeFromIovecs(request);
    auto* buffer = request.MutableBuffer();
    const auto bytesToCopy = CalculateByteCount(request);
    buffer->ReserveAndResize(bytesToCopy);
    auto bytesCopied =
        TRopeUtils::SafeMemcpy(&(*buffer)[0], rope.Begin(), bytesToCopy);
    request.MutableIovecs()->Clear();
    Y_ABORT_UNLESS(bytesCopied == bytesToCopy);
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

    // Metrics / logging
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;
    ITraceSerializerPtr TraceSerializer;

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
            ITraceSerializerPtr traceSerializer,
            NCloud::NProto::EStorageMediaKind mediaKind)
        : WriteRequest(std::move(request))
        , Range(range)
        , BlobRange(Range.AlignedSubRange())
        , RequestInfo(std::move(requestInfo))
        , LogTag(std::move(logTag))
        , RequestStats(std::move(requestStats))
        , ProfileLog(std::move(profileLog))
        , TraceSerializer(std::move(traceSerializer))
        , MediaKind(mediaKind)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            RequestInfo->CallContext,
            "GenerateBlobIds");

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
        auto* trace =
            request->Record.MutableHeaders()->MutableInternal()->MutableTrace();
        TraceSerializer->BuildTraceRequest(
            *trace,
            RequestInfo->CallContext->LWOrbit);

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
        auto* msg = ev->Get();
        const auto& error = msg->GetError();

        TABLET_VERIFY(InFlightRequest);

        InFlightRequest->Complete(ctx.Now(), error);
        FinalizeProfileLogRequestInfo(
            InFlightRequest->ProfileLogRequest,
            msg->Record);
        HandleServiceTraceInfo(
            "GenerateBlobIds",
            ctx,
            TraceSerializer,
            RequestInfo->CallContext,
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
        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            RequestInfo->CallContext,
            "WriteBlobs");

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
                TABLET_VERIFY(bytesCopied == blobId.BlobSize());
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

            if (!RequestInfo->CallContext->LWOrbit.Fork(request->Orbit)) {
                FILESTORE_TRACK(
                    ForkFailed,
                    RequestInfo->CallContext,
                    "TEvBlobStorage::TEvPut");
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
        RequestInfo->CallContext->LWOrbit.Join(msg->Orbit);

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
        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            RequestInfo->CallContext,
            "AddData");

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
                TString buffer;
                auto bytesCopied = CopyBufferFromRope(Rope, buffer, 0, length);
                TABLET_VERIFY(bytesCopied == length);
                unalignedHead.SetContent(std::move(buffer));
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
                TString buffer;
                auto bytesCopied =
                    CopyBufferFromRope(Rope, buffer, offset, length);
                TABLET_VERIFY(bytesCopied == length);
                unalignedTail.SetContent(std::move(buffer));
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
        auto* trace =
            request->Record.MutableHeaders()->MutableInternal()->MutableTrace();
        TraceSerializer->BuildTraceRequest(
            *trace,
            RequestInfo->CallContext->LWOrbit);

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
        HandleServiceTraceInfo(
            "AddData",
            ctx,
            TraceSerializer,
            RequestInfo->CallContext,
            msg->Record);

        if (HasError(msg->GetError())) {
            WriteData(ctx, msg->GetError());
            return;
        }

        ReplyAndDie(ctx, {} /* record */);
    }

    /**
     * @brief Fallback to regular write if two-stage write fails for any reason
     */
    void WriteData(const TActorContext& ctx, const NProto::TError& error)
    {
        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            RequestInfo->CallContext,
            "WriteData");

        WriteDataFallbackEnabled = true;
        MoveIovecsToBuffer(WriteRequest);

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

        auto request = std::make_unique<TEvService::TEvWriteDataRequest>();
        request->Record = std::move(WriteRequest);
        request->Record.MutableHeaders()->SetThrottlingDisabled(true);
        auto* trace =
            request->Record.MutableHeaders()->MutableInternal()->MutableTrace();
        TraceSerializer->BuildTraceRequest(
            *trace,
            RequestInfo->CallContext->LWOrbit);

        // forward request through tablet proxy
        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void HandleWriteDataResponse(
        const TEvService::TEvWriteDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();
        HandleServiceTraceInfo(
            "WriteData",
            ctx,
            TraceSerializer,
            RequestInfo->CallContext,
            msg->Record);

        if (HasError(msg->GetError())) {
            HandleError(ctx, msg->GetError());
            return;
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s WriteData succeeded",
            LogTag.c_str());

        ReplyAndDie(ctx, std::move(msg->Record));
    }

    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TWriteDataResponse record)
    {
        FILESTORE_TRACK(
            ResponseSent_ServiceWorker,
            RequestInfo->CallContext,
            "WriteData");

        auto response = std::make_unique<TEvService::TEvWriteDataResponse>();
        response->Record = std::move(record);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }

    void HandleError(const TActorContext& ctx, const NProto::TError& error)
    {
        FILESTORE_TRACK(
            ResponseSent_ServiceWorker,
            RequestInfo->CallContext,
            "WriteData");

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

    FILESTORE_TRACK(RequestReceived_Service, msg->CallContext, "WriteData");

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
    const bool blockChecksumsEnabled =
        filestore.GetFeatures().GetBlockChecksumsInProfileLogEnabled()
        || StorageConfig->GetBlockChecksumsInProfileLogEnabled();

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
        inflight->ProfileLogRequest.SetClientId(session->ClientId);
        if (blockChecksumsEnabled) {
            CalculateWriteDataRequestChecksums(
                msg->Record,
                blockSize,
                inflight->ProfileLogRequest);
        }

        auto requestInfo =
            CreateRequestInfo(SelfId(), cookie, msg->CallContext);

        auto actor = std::make_unique<TWriteDataActor>(
            std::move(msg->Record),
            range,
            std::move(requestInfo),
            std::move(logTag),
            session->RequestStats,
            ProfileLog,
            TraceSerializer,
            session->MediaKind);
        NCloud::Register(ctx, std::move(actor));
    } else {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Forwarding WriteData request to tablet");
        MoveIovecsToBuffer(msg->Record);
        ForwardRequest<TEvService::TWriteDataMethod>(ctx, ev);
    }
}

}   // namespace NCloud::NFileStore::NStorage
