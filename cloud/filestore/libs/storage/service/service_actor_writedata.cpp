#include "service_actor.h"

#include "rope_utils.h"
#include "verify.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/iterator/enumerate.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsThreeStageWriteEnabled(const NProto::TFileStore& fs)
{
    const auto isHdd =
        fs.GetStorageMediaKind() == NProto::STORAGE_MEDIA_HYBRID ||
        fs.GetStorageMediaKind() == NProto::STORAGE_MEDIA_HDD;
    const auto disabledAsHdd =
        isHdd && fs.GetFeatures().GetThreeStageWriteDisabledForHDD();
    return !disabledAsHdd && fs.GetFeatures().GetThreeStageWriteEnabled();
}

constexpr ui32 ProxyCriticalRetryThreshold = 10;
static_assert(ProxyCriticalRetryThreshold > 0);

////////////////////////////////////////////////////////////////////////////////

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

    auto rope = CreateRope(request.GetIovecs());
    auto* buffer = request.MutableBuffer();
    const auto bytesToCopy = NFileStore::CalculateByteCount(request);
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
    enum class ETabletRetryRequest
    {
        None,
        ConfirmAddData,
        CancelAddData
    };

    // Original request
    NProto::TWriteDataRequest WriteRequest;
    const TByteRange Range;
    const TByteRange BlobRange;
    const TRequestInfoPtr RequestInfo;

    // Filesystem-specific params
    const TString LogTag;

    // Turns off writes to blob storage for performance testing purposes
    const bool WriteBlobDisabled;

    // generated blob id and associated data
    NProtoPrivate::TGenerateBlobIdsResponse GenerateBlobIdsResponse;

    // WriteData state
    ui32 RemainingBlobsToWrite = 0;
    bool UseUnconfirmedFlow = false;
    NProto::TError WriteBlobError;

    ui32 TabletProxyRetries = 0;
    ETabletRetryRequest TabletRetryRequest = ETabletRetryRequest::None;
    TBackoffDelayProvider TabletProxyRetryDelayProvider{
        TDuration::MilliSeconds(50),
        TDuration::Seconds(1)};

    // Metrics / logging
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;
    ITraceSerializerPtr TraceSerializer;

    // Refers to GenerateBlobIds/AddData/ConfirmAddData/CancelAddData request,
    // depending on which one is in flight
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
        bool writeBlobDisabled,
        bool useUnconfirmedFlow,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        NCloud::NProto::EStorageMediaKind mediaKind)
        : WriteRequest(std::move(request))
        , Range(range)
        , BlobRange(Range.AlignedSubRange())
        , RequestInfo(std::move(requestInfo))
        , LogTag(std::move(logTag))
        , WriteBlobDisabled(writeBlobDisabled)
        , UseUnconfirmedFlow(useUnconfirmedFlow)
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

        Rope = CreateRope(WriteRequest.GetIovecs());

        auto request =
            std::make_unique<TEvIndexTablet::TEvGenerateBlobIdsRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(WriteRequest.GetNodeId());
        request->Record.SetHandle(WriteRequest.GetHandle());
        request->Record.SetOffset(BlobRange.Offset);
        request->Record.SetLength(BlobRange.Length);

        if (UseUnconfirmedFlow) {
            FillUnalignedDataRanges(
                *request->Record.MutableUnalignedDataRanges());
        }

        PrepareTabletRequest(
            ctx,
            EFileStoreRequest::GenerateBlobIds,
            request->Record,
            false /* addWriteRangeInfo */);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s WriteDataActor started, data size: %lu, offset: %lu, aligned "
            "size: %lu, aligned offset: %lu",
            LogTag.c_str(),
            NFileStore::CalculateByteCount(WriteRequest),
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
            HFunc(TEvents::TEvWakeup, HandleWakeup);

            HFunc(
                TEvIndexTablet::TEvGenerateBlobIdsResponse,
                HandleGenerateBlobIdsResponse);

            HFunc(TEvBlobStorage::TEvPutResult, HandleWriteBlobResponse);

            HFunc(TEvIndexTablet::TEvAddDataResponse, HandleAddDataResponse);

            HFunc(
                TEvIndexTablet::TEvConfirmAddDataResponse,
                HandleConfirmAddDataResponse);
            HFunc(
                TEvIndexTablet::TEvCancelAddDataResponse,
                HandleCancelAddDataResponse);

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

        SERVICE_VERIFY(InFlightRequest);

        FinalizeProfileLogRequestInfo(
            InFlightRequest->AccessProfileLogRequest(),
            msg->Record);
        InFlightRequest->Complete(ctx.Now(), error);
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

        SERVICE_VERIFY(
            UseUnconfirmedFlow ||
            !GenerateBlobIdsResponse.GetUnconfirmedFlowEnabled());

        UseUnconfirmedFlow &=
            GenerateBlobIdsResponse.GetUnconfirmedFlowEnabled();

        if (WriteBlobDisabled) {
            // Pretend that the blobs were written and trigger a critical event
            // to alert the on-call engineer, as fake blobs should never be
            // written in production
            ReportFakeBlobWasWritten();
            // Proceed to the final part: adding fake blobs to the index
            if (UseUnconfirmedFlow) {
                ConfirmAddData(ctx);
            } else {
                AddData(ctx);
            }
            return;
        }

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

        IRcBufAllocator* allocator = ctx.ActorSystem()->GetRcBufAllocator();
        if (!allocator) {
            allocator = GetDefaultRcBufAllocator();
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
                auto putData = allocator->AllocRcBuf(blobId.BlobSize(), 0, 0);
                auto bytesCopied = TRopeUtils::SafeMemcpy(
                    &putData[0],
                    ropeIt,
                    blobId.BlobSize());
                SERVICE_VERIFY(bytesCopied == blobId.BlobSize());
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
        if (HasError(WriteBlobError)) {
            return;
        }

        const auto* msg = ev->Get();
        RequestInfo->CallContext->LWOrbit.Join(msg->Orbit);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s TEvPutResult response received: %s",
            LogTag.c_str(),
            msg->ToString().c_str());

        ui64 blobIdx = msg->Id.Cookie();

        if (msg->Status != NKikimrProto::OK) {
            const auto error =
                MakeError(MAKE_KIKIMR_ERROR(msg->Status), msg->ErrorReason);
            WriteBlobError = error;
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s WriteData error: %s, group: %lu",
                LogTag.c_str(),
                msg->ErrorReason.Quote().c_str(),
                blobIdx < GenerateBlobIdsResponse.BlobsSize()
                    ? GenerateBlobIdsResponse.GetBlobs(blobIdx).GetBSGroupId()
                    : 0);
            // We still may receive some responses, but we do not want to
            // process them
            for (auto& inFlight: InFlightBSRequests) {
                if (inFlight && !inFlight->IsCompleted()) {
                    inFlight->Complete(ctx.Now(), error);
                }
            }

            if (UseUnconfirmedFlow) {
                return CancelAddData(ctx);
            }

            return WriteData(ctx, error);
        }

        // It is implicitly expected that cookies are generated in increasing
        // order starting from 0.
        // TODO: replace this SERVICE_VERIFY with a critical event + WriteData
        // fallback
        SERVICE_VERIFY(
            blobIdx < InFlightBSRequests.size() &&
            InFlightBSRequests[blobIdx] &&
            !InFlightBSRequests[blobIdx]->IsCompleted());
        InFlightBSRequests[blobIdx]->Complete(ctx.Now(), {});
        StorageStatusFlags[blobIdx] = msg->StatusFlags.Raw;
        ApproximateFreeSpaceShares[blobIdx] = msg->ApproximateFreeSpaceShare;

        --RemainingBlobsToWrite;
        if (RemainingBlobsToWrite == 0) {
            if (UseUnconfirmedFlow) {
                ConfirmAddData(ctx);
            } else {
                AddData(ctx);
            }
        }
    }

    template <typename TRecord>
    void AddStorageStatusInfo(TRecord& record)
    {
        record.MutableStorageStatusFlags()->Reserve(StorageStatusFlags.size());
        for (const auto flags: StorageStatusFlags) {
            record.AddStorageStatusFlags(flags);
        }
        record.MutableApproximateFreeSpaceShares()->Reserve(
            ApproximateFreeSpaceShares.size());
        for (const auto share: ApproximateFreeSpaceShares) {
            record.AddApproximateFreeSpaceShares(share);
        }
    }

    template <typename TRecord>
    void PrepareTabletRequest(
        const TActorContext& ctx,
        EFileStoreRequest requestType,
        TRecord& record,
        bool addWriteRangeInfo)
    {
        auto callContext = MakeIntrusive<TCallContext>(
            RequestInfo->CallContext->FileSystemId,
            RequestInfo->CallContext->RequestId);
        callContext->SetRequestStartedCycles(GetCycleCount());
        callContext->RequestType = requestType;
        InFlightRequest.ConstructInPlace(
            TRequestInfo(
                RequestInfo->Sender,
                RequestInfo->Cookie,
                std::move(callContext)),
            ProfileLog,
            MediaKind,
            RequestStats);
        InFlightRequest->Start(ctx.Now());
        InitProfileLogRequestInfo(
            InFlightRequest->AccessProfileLogRequest(),
            record);

        if (addWriteRangeInfo) {
            auto* rangeInfo =
                InFlightRequest->AccessProfileLogRequest().AddRanges();
            rangeInfo->SetNodeId(WriteRequest.GetNodeId());
            rangeInfo->SetHandle(WriteRequest.GetHandle());
            rangeInfo->SetOffset(WriteRequest.GetOffset());
            rangeInfo->SetBytes(NFileStore::CalculateByteCount(WriteRequest));
        }

        auto* trace =
            record.MutableHeaders()->MutableInternal()->MutableTrace();
        TraceSerializer->BuildTraceRequest(
            *trace,
            RequestInfo->CallContext->LWOrbit);
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
        request->Record.SetLength(NFileStore::CalculateByteCount(WriteRequest));
        for (const auto& blob: GenerateBlobIdsResponse.GetBlobs()) {
            *request->Record.AddBlobIds() = blob.GetBlobId();
        }
        request->Record.SetCommitId(GenerateBlobIdsResponse.GetCommitId());

        FillUnalignedDataRanges(*request->Record.MutableUnalignedDataRanges());

        AddStorageStatusInfo(request->Record);

        PrepareTabletRequest(
            ctx,
            EFileStoreRequest::AddData,
            request->Record,
            false /* addWriteRangeInfo */);

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

        SERVICE_VERIFY(InFlightRequest);
        FinalizeProfileLogRequestInfo(
            InFlightRequest->AccessProfileLogRequest(),
            msg->Record);
        InFlightRequest->Complete(ctx.Now(), msg->GetError());
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

    void ConfirmAddData(const TActorContext& ctx)
    {
        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            RequestInfo->CallContext,
            "ConfirmAddData");

        auto request =
            std::make_unique<TEvIndexTablet::TEvConfirmAddDataRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetCommitId(GenerateBlobIdsResponse.GetCommitId());

        // Add storage status info (now available after WriteBlobs completed)
        AddStorageStatusInfo(request->Record);

        PrepareTabletRequest(
            ctx,
            EFileStoreRequest::ConfirmAddData,
            request->Record,
            true);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Sending ConfirmAddData request, commitId=%lu",
            LogTag.c_str(),
            GenerateBlobIdsResponse.GetCommitId());

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void CancelAddData(const TActorContext& ctx)
    {
        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            RequestInfo->CallContext,
            "CancelAddData");

        auto request =
            std::make_unique<TEvIndexTablet::TEvCancelAddDataRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetCommitId(GenerateBlobIdsResponse.GetCommitId());

        PrepareTabletRequest(
            ctx,
            EFileStoreRequest::CancelAddData,
            request->Record,
            true);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Sending CancelAddData request, commitId=%lu",
            LogTag.c_str(),
            GenerateBlobIdsResponse.GetCommitId());

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void HandleConfirmAddDataResponse(
        const TEvIndexTablet::TEvConfirmAddDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        SERVICE_VERIFY(InFlightRequest);
        FinalizeProfileLogRequestInfo(
            InFlightRequest->AccessProfileLogRequest(),
            msg->Record);
        InFlightRequest->Complete(ctx.Now(), msg->GetError());
        HandleServiceTraceInfo(
            "ConfirmAddData",
            ctx,
            TraceSerializer,
            RequestInfo->CallContext,
            msg->Record);

        if (HasError(msg->GetError())) {
            if (ev->Sender == MakeIndexTabletProxyServiceId()) {
                ScheduleTabletProxyRetry(
                    ctx,
                    ETabletRetryRequest::ConfirmAddData,
                    msg->GetError());
                return;
            }

            ResetTabletProxyRetryState();
            return WriteData(ctx, msg->GetError());
        }

        ResetTabletProxyRetryState();
        ReplyAndDie(ctx, {});
    }

    void HandleCancelAddDataResponse(
        const TEvIndexTablet::TEvCancelAddDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        SERVICE_VERIFY(InFlightRequest);
        FinalizeProfileLogRequestInfo(
            InFlightRequest->AccessProfileLogRequest(),
            msg->Record);
        InFlightRequest->Complete(ctx.Now(), msg->GetError());
        HandleServiceTraceInfo(
            "CancelAddData",
            ctx,
            TraceSerializer,
            RequestInfo->CallContext,
            msg->Record);

        if (HasError(msg->GetError())) {
            if (ev->Sender == MakeIndexTabletProxyServiceId()) {
                ScheduleTabletProxyRetry(
                    ctx,
                    ETabletRetryRequest::CancelAddData,
                    msg->GetError());
                return;
            }
        }

        ResetTabletProxyRetryState();
        WriteData(ctx, WriteBlobError);
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        if (TabletRetryRequest == ETabletRetryRequest::None) {
            return;
        }

        const auto retryRequest = TabletRetryRequest;
        TabletRetryRequest = ETabletRetryRequest::None;

        switch (retryRequest) {
            case ETabletRetryRequest::ConfirmAddData:
                ConfirmAddData(ctx);
                return;

            case ETabletRetryRequest::CancelAddData:
                CancelAddData(ctx);
                return;

            case ETabletRetryRequest::None:
                return;
        }
    }

    void ScheduleTabletProxyRetry(
        const TActorContext& ctx,
        ETabletRetryRequest retryRequest,
        const NProto::TError& error)
    {
        const auto delay = TabletProxyRetryDelayProvider.GetDelayAndIncrease();
        ++TabletProxyRetries;

        if (TabletProxyRetries % ProxyCriticalRetryThreshold == 0) {
            SERVICE_VERIFY(retryRequest != ETabletRetryRequest::None);

            const auto requestType =
                retryRequest == ETabletRetryRequest::ConfirmAddData
                ? EFileStoreRequest::ConfirmAddData
                : EFileStoreRequest::CancelAddData;

            ReportUnconfirmedFlowProxyRetryThresholdReached(
                TStringBuilder()
                << "retry=" << TabletProxyRetries
                << ", request=" << GetFileStoreRequestName(requestType)
                << ", error=" << FormatError(error));
        }

        TabletRetryRequest = retryRequest;
        ctx.Schedule(delay, new TEvents::TEvWakeup());
    }

    void ResetTabletProxyRetryState()
    {
        TabletProxyRetries = 0;
        TabletRetryRequest = ETabletRetryRequest::None;
        TabletProxyRetryDelayProvider.Reset();
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
            NFileStore::CalculateByteCount(WriteRequest),
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

    void FillUnalignedDataRanges(
        google::protobuf::RepeatedPtrField<NProtoPrivate::TFreshDataRange>&
            ranges)
    {
        if (Range.Offset < BlobRange.Offset) {
            auto& unalignedHead = *ranges.Add();
            unalignedHead.SetOffset(Range.Offset);
            const ui64 length = BlobRange.Offset - Range.Offset;
            if (WriteRequest.GetIovecs().empty()) {
                unalignedHead.SetContent(
                    WriteRequest.GetBuffer().substr(0, length));
            } else {
                TString buffer;
                auto bytesCopied = CopyBufferFromRope(Rope, buffer, 0, length);
                SERVICE_VERIFY(bytesCopied == length);
                unalignedHead.SetContent(std::move(buffer));
            }
        }

        if (Range.End() > BlobRange.End()) {
            auto& unalignedTail = *ranges.Add();
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
                SERVICE_VERIFY(bytesCopied == length);
                unalignedTail.SetContent(std::move(buffer));
            }
        }
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

    const auto bytesCount = NFileStore::CalculateByteCount(msg->Record);
    const TByteRange range(msg->Record.GetOffset(), bytesCount, blockSize);
    const bool threeStageWriteEnabled =
        range.Length >= filestore.GetFeatures().GetThreeStageWriteThreshold() &&
        threeStageWriteAllowed &&
        (range.IsAligned() ||
         StorageConfig->GetUnalignedThreeStageWriteEnabled()) &&
        range.AlignedSubRange().Length > 0;
    const bool blockChecksumsEnabled =
        filestore.GetFeatures().GetBlockChecksumsInProfileLogEnabled() ||
        StorageConfig->GetBlockChecksumsInProfileLogEnabled();

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

        InitProfileLogRequestInfo(
            inflight->AccessProfileLogRequest(),
            msg->Record);
        inflight->AccessProfileLogRequest().SetClientId(session->ClientId);
        if (blockChecksumsEnabled) {
            CalculateWriteDataRequestChecksums(
                msg->Record,
                blockSize,
                inflight->AccessProfileLogRequest());
        }

        auto requestInfo =
            CreateRequestInfo(SelfId(), cookie, msg->CallContext);

        auto actor = std::make_unique<TWriteDataActor>(
            std::move(msg->Record),
            range,
            std::move(requestInfo),
            std::move(logTag),
            filestore.GetFeatures().GetWriteBlobDisabled(),
            filestore.GetFeatures().GetUnconfirmedFlowEnabled(),
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
