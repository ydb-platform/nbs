#include "service_actor.h"

#include "rope_utils.h"
#include "verify.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/blob_id.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/iterator/enumerate.h>

#include <optional>
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
 * @brief Prepares a WriteData request to be sent to the tablet.
 *
 * If the request payload is represented by iovecs, this function copies the
 * required data slice from the iovecs into the request buffer, or into the
 * request payload when @p useExternalPayload is set. The iovecs are then
 * cleared from the request.
 *
 * @param request The write request containing iovecs used as the data source.
 * @param useExternalPayload If true, stores the copied data as an external
 * payload; otherwise, stores it in the request buffer.
 */
void PrepareWriteDataRequestPayload(
    TEvService::TEvWriteDataRequest& request,
    bool useExternalPayload)
{
    auto& record = request.Record;
    if (record.GetIovecs().empty()) {
        if (useExternalPayload) {
            auto& buffer = *record.MutableBuffer();
            request.AddPayload(TRope(std::move(buffer)));
            buffer.clear();
        }
        return;
    }

    auto rope = CreateRope(record.GetIovecs());
    TString buffer;
    const auto bytesToCopy = NFileStore::CalculateByteCount(record);
    buffer.ReserveAndResize(bytesToCopy);
    auto bytesCopied =
        TRopeUtils::SafeMemcpy(buffer.begin(), rope.Begin(), bytesToCopy);
    record.MutableIovecs()->Clear();
    Y_ABORT_UNLESS(bytesCopied == bytesToCopy);

    if (useExternalPayload) {
        request.AddPayload(TRope(std::move(buffer)));
    } else {
        record.SetBuffer(std::move(buffer));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TWriteRequestContext
{
    // Original request
    NProto::TWriteDataRequest WriteRequest;
    const TByteRange Range;
    const TRequestInfoPtr RequestInfo;

    // Filesystem-specific params
    const TString LogTag;

    // Turns off writes to blob storage for performance testing purposes
    const bool WriteBlobDisabled;

    bool UseUnconfirmedFlow = false;
    IRequestStatsPtr RequestStats;
    const NCloud::NProto::EStorageMediaKind MediaKind;
    const bool UseThreeStageWrite = false;
    const bool ExternalWriteDataPayloadEnabled = false;
    [[maybe_unused]] const bool EnableRdmaForWrites = false;

    TWriteRequestContext(
        NProto::TWriteDataRequest writeRequest,
        TByteRange range,
        TRequestInfoPtr requestInfo,
        TString logTag,
        bool writeBlobDisabled,
        bool useUnconfirmedFlow,
        IRequestStatsPtr requestStats,
        NCloud::NProto::EStorageMediaKind mediaKind,
        bool useThreeStageWrite,
        bool externalWriteDataPayloadEnabled,
        bool enableRdmaForWrites)
        : WriteRequest(std::move(writeRequest))
        , Range(range)
        , RequestInfo(std::move(requestInfo))
        , LogTag(std::move(logTag))
        , WriteBlobDisabled(writeBlobDisabled)
        , UseUnconfirmedFlow(useUnconfirmedFlow)
        , RequestStats(std::move(requestStats))
        , MediaKind(mediaKind)
        , UseThreeStageWrite(useThreeStageWrite)
        , ExternalWriteDataPayloadEnabled(externalWriteDataPayloadEnabled)
        , EnableRdmaForWrites(enableRdmaForWrites)
    {}
};

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

    struct TRequestState
    {
        TWriteRequestContext RequestContext;

        const TByteRange BlobRange;

        // generated blob id and associated data
        NProtoPrivate::TGenerateBlobIdsResponse GenerateBlobIdsResponse;

        // WriteData state
        ui32 RemainingBlobsToWrite = 0;
        NProto::TError WriteBlobError;

        ui32 TabletProxyRetries = 0;
        ETabletRetryRequest TabletRetryRequest = ETabletRetryRequest::None;
        TBackoffDelayProvider TabletProxyRetryDelayProvider{
            TDuration::MilliSeconds(50),
            TDuration::Seconds(1)};

        // Refers to GenerateBlobIds/AddData/ConfirmAddData/CancelAddData
        // request, depending on which one is in flight
        TMaybe<TInFlightRequest> InFlightRequest;
        TVector<std::unique_ptr<TInFlightRequest>> InFlightBSRequests;
        TVector<ui32> StorageStatusFlags;
        TVector<double> ApproximateFreeSpaceShares;
        TRope Rope;

        explicit TRequestState(TWriteRequestContext params)
            : RequestContext(std::move(params))
            , BlobRange(RequestContext.Range.AlignedSubRange())
        {}
    };

    const IProfileLogPtr ProfileLog;
    const ITraceSerializerPtr TraceSerializer;
    std::optional<TRequestState> RequestState;

public:
    TWriteDataActor(
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer)
        : ProfileLog(std::move(profileLog))
        , TraceSerializer(std::move(traceSerializer))
    {}

    void Initialize(TWriteRequestContext params)
    {
        Y_ABORT_UNLESS(!RequestState);
        RequestState.emplace(std::move(params));
    }

    void Bootstrap(const TActorContext& ctx)
    {
        auto& state = GetState();

        if (!state.RequestContext.UseThreeStageWrite) {
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "Forwarding WriteData request to tablet");

            WriteData(ctx, false /* isFallback */);
            Become(&TThis::StateWork);
            return;
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Using three-stage write for request, size: %lu",
            state.RequestContext.LogTag.c_str(),
            state.RequestContext.Range.Length);

        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            state.RequestContext.RequestInfo->CallContext,
            "GenerateBlobIds");

        state.Rope = CreateRope(state.RequestContext.WriteRequest.GetIovecs());

        auto request =
            std::make_unique<TEvIndexTablet::TEvGenerateBlobIdsRequest>();

        request->Record.MutableHeaders()->CopyFrom(
            state.RequestContext.WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(
            state.RequestContext.WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(state.RequestContext.WriteRequest.GetNodeId());
        request->Record.SetHandle(state.RequestContext.WriteRequest.GetHandle());
        request->Record.SetOffset(state.BlobRange.Offset);
        request->Record.SetLength(state.BlobRange.Length);
        request->Record.SetUnconfirmedFlowRequested(
            state.RequestContext.UseUnconfirmedFlow);

        if (state.RequestContext.UseUnconfirmedFlow) {
            FillUnalignedDataRanges(
                *request->Record.MutableUnalignedDataRanges());
        }

        PrepareTabletRequest(
            ctx,
            EFileStoreRequest::GenerateBlobIds,
            request->Record,
            false /* addWriteRangeInfo */);
        request->CallContext = state.InFlightRequest->CallContext;

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s WriteDataActor started, data size: %lu, offset: %lu, aligned "
            "size: %lu, aligned offset: %lu",
            state.RequestContext.LogTag.c_str(),
            NFileStore::CalculateByteCount(state.RequestContext.WriteRequest),
            state.RequestContext.WriteRequest.GetOffset(),
            state.BlobRange.Length,
            state.BlobRange.Offset);

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());

        Become(&TThis::StateWork);
    }

private:
    TRequestState& GetState()
    {
        Y_ABORT_UNLESS(RequestState);
        return *RequestState;
    }

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
        auto& state = GetState();
        const auto& LogTag = state.RequestContext.LogTag;

        auto* msg = ev->Get();
        const auto& error = msg->GetError();

        SERVICE_VERIFY(state.InFlightRequest);

        state.RequestContext.RequestInfo->CallContext->LWOrbit.Join(
            state.InFlightRequest->CallContext->LWOrbit);
        FinalizeProfileLogRequestInfo(
            state.InFlightRequest->AccessProfileLogRequest(),
            msg->Record);
        state.InFlightRequest->Complete(ctx.Now(), error);

        if (HasError(error)) {
            HandleError(ctx, error);
            return;
        }

        state.GenerateBlobIdsResponse.CopyFrom(msg->Record);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s GenerateBlobIds response received: %s",
            state.RequestContext.LogTag.c_str(),
            state.GenerateBlobIdsResponse.DebugString().Quote().c_str());

        SERVICE_VERIFY(
            state.RequestContext.UseUnconfirmedFlow ||
            !state.GenerateBlobIdsResponse.GetUnconfirmedFlowEnabled());

        state.RequestContext.UseUnconfirmedFlow &=
            state.GenerateBlobIdsResponse.GetUnconfirmedFlowEnabled();

        if (state.RequestContext.WriteBlobDisabled) {
            // Pretend that the blobs were written and trigger a critical event
            // to alert the on-call engineer, as fake blobs should never be
            // written in production
            ReportFakeBlobWasWritten();
            // Proceed to the final part: adding fake blobs to the index
            if (state.RequestContext.UseUnconfirmedFlow) {
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
        auto& state = GetState();
        const auto& LogTag = state.RequestContext.LogTag;

        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            state.RequestContext.RequestInfo->CallContext,
            "WriteBlobs");

        state.RemainingBlobsToWrite = state.GenerateBlobIdsResponse.BlobsSize();
        ui64 offset = state.BlobRange.Offset - state.RequestContext.Range.Offset;

        state.InFlightBSRequests.reserve(state.RemainingBlobsToWrite);
        state.StorageStatusFlags.resize(
            state.GenerateBlobIdsResponse.BlobsSize());
        state.ApproximateFreeSpaceShares.resize(
            state.GenerateBlobIdsResponse.BlobsSize());

        const auto& iovecs = state.RequestContext.WriteRequest.GetIovecs();
        auto ropeIt = state.Rope.Begin();
        if (!state.Rope.empty()) {
            ropeIt += offset;
        }
        for (const auto& blob: state.GenerateBlobIdsResponse.GetBlobs()) {
            NKikimr::TLogoBlobID blobId =
                LogoBlobIDFromLogoBlobID(blob.GetBlobId());

            auto writeBlobCallContext = MakeIntrusive<TCallContext>(
                state.RequestContext.RequestInfo->CallContext->FileSystemId,
                state.RequestContext.RequestInfo->CallContext->RequestId);
            writeBlobCallContext->SetRequestStartedCycles(GetCycleCount());
            writeBlobCallContext->RequestType = EFileStoreRequest::WriteBlob;
            state.InFlightBSRequests.emplace_back(
                std::make_unique<TInFlightRequest>(
                    TRequestInfo(
                        state.RequestContext.RequestInfo->Sender,
                        state.RequestContext.RequestInfo->Cookie,
                        std::move(writeBlobCallContext)),
                    ProfileLog,
                    state.RequestContext.MediaKind,
                    state.RequestContext.RequestStats));
            state.InFlightBSRequests.back()->Start(ctx.Now());

            std::unique_ptr<TEvBlobStorage::TEvPut> request;

            if (!iovecs.empty()) {
                TRcBuf putData;

                // GetRcBufAllocator is available
                // starting from ydb/stable-26-1
                // TODO(myagkov): issue-7526
                // Remove ifdef OPENSOURCE after backporting
                // GetRcBufAllocator to 25-* stable
                // branches or dropping support of 25-* stable branches
#ifdef OPENSOURCE
                if (state.RequestContext.EnableRdmaForWrites) {
                    IRcBufAllocator* allocator =
                        ctx.ActorSystem()->GetRcBufAllocator();
                    if (allocator) {
                        putData =
                            allocator->AllocRcBuf(blobId.BlobSize(), 0, 0);
                    }
                }
#endif
                if (!putData) {
                    putData = TRcBuf(TString::Uninitialized(blobId.BlobSize()));
                }

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
                if (state.GenerateBlobIdsResponse.BlobsSize() == 1 &&
                    state.RequestContext.Range == state.BlobRange)
                {
                    // do not copy the buffer if there is only one blob
                    request = std::make_unique<TEvBlobStorage::TEvPut>(
                        blobId,
                        state.RequestContext.WriteRequest.GetBuffer(),
                        TInstant::Max(),
                        NKikimrBlobStorage::UserData);
                } else {
                    request = std::make_unique<TEvBlobStorage::TEvPut>(
                        blobId,
                        TString(
                            state.RequestContext.WriteRequest.GetBuffer().data() +
                                offset,
                            blobId.BlobSize()),
                        TInstant::Max(),
                        NKikimrBlobStorage::UserData);
                }
                offset += blobId.BlobSize();
            }

            if (!state.RequestContext.RequestInfo->CallContext->LWOrbit.Fork(
                    request->Orbit))
            {
                FILESTORE_TRACK(
                    ForkFailed,
                    state.RequestContext.RequestInfo->CallContext,
                    "TEvBlobStorage::TEvPut");
            }

            NKikimr::TActorId proxy =
                MakeBlobStorageProxyID(blob.GetBSGroupId());
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s Sending TEvPut request to blob storage, blobId: %s, proxy: "
                "%s iovecs size: %ul",
                state.RequestContext.LogTag.c_str(),
                blobId.ToString().c_str(),
                proxy.ToString().c_str(),
                state.RequestContext.WriteRequest.GetIovecs().size());
            SendToBSProxy(ctx, proxy, request.release(), blobId.Cookie());
        }
    }

    void HandleWriteBlobResponse(
        const TEvBlobStorage::TEvPutResult::TPtr& ev,
        const TActorContext& ctx)
    {
        auto& state = GetState();
        const auto& LogTag = state.RequestContext.LogTag;

        if (HasError(state.WriteBlobError)) {
            return;
        }

        const auto* msg = ev->Get();
        state.RequestContext.RequestInfo->CallContext->LWOrbit.Join(msg->Orbit);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s TEvPutResult response received: %s",
            state.RequestContext.LogTag.c_str(),
            msg->ToString().c_str());

        ui64 blobIdx = msg->Id.Cookie();

        if (msg->Status != NKikimrProto::OK) {
            const auto error =
                MakeError(MAKE_KIKIMR_ERROR(msg->Status), msg->ErrorReason);
            state.WriteBlobError = error;
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "%s WriteData error: %s, group: %lu",
                state.RequestContext.LogTag.c_str(),
                msg->ErrorReason.Quote().c_str(),
                blobIdx < state.GenerateBlobIdsResponse.BlobsSize()
                    ? state.GenerateBlobIdsResponse.GetBlobs(blobIdx)
                          .GetBSGroupId()
                    : 0);
            // We still may receive some responses, but we do not want to
            // process them
            for (auto& inFlight: state.InFlightBSRequests) {
                if (inFlight && !inFlight->IsCompleted()) {
                    inFlight->Complete(ctx.Now(), error);
                }
            }

            if (state.RequestContext.UseUnconfirmedFlow) {
                return CancelAddData(ctx);
            }

            return WriteData(ctx, true /* isFallback */);
        }

        // It is implicitly expected that cookies are generated in increasing
        // order starting from 0.
        // TODO: replace this SERVICE_VERIFY with a critical event + WriteData
        // fallback
        SERVICE_VERIFY(
            blobIdx < state.InFlightBSRequests.size() &&
            state.InFlightBSRequests[blobIdx] &&
            !state.InFlightBSRequests[blobIdx]->IsCompleted());
        state.InFlightBSRequests[blobIdx]->Complete(ctx.Now(), {});
        state.StorageStatusFlags[blobIdx] = msg->StatusFlags.Raw;
        state.ApproximateFreeSpaceShares[blobIdx] =
            msg->ApproximateFreeSpaceShare;

        --state.RemainingBlobsToWrite;
        if (state.RemainingBlobsToWrite == 0) {
            if (state.RequestContext.UseUnconfirmedFlow) {
                ConfirmAddData(ctx);
            } else {
                AddData(ctx);
            }
        }
    }

    template <typename TRecord>
    void AddStorageStatusInfo(TRecord& record)
    {
        auto& state = GetState();

        record.MutableStorageStatusFlags()->Reserve(
            state.StorageStatusFlags.size());
        for (const auto flags: state.StorageStatusFlags) {
            record.AddStorageStatusFlags(flags);
        }
        record.MutableApproximateFreeSpaceShares()->Reserve(
            state.ApproximateFreeSpaceShares.size());
        for (const auto share: state.ApproximateFreeSpaceShares) {
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
        auto& state = GetState();

        auto callContext = MakeIntrusive<TCallContext>(
            state.RequestContext.RequestInfo->CallContext->FileSystemId,
            state.RequestContext.RequestInfo->CallContext->RequestId);
        callContext->SetRequestStartedCycles(GetCycleCount());
        callContext->RequestType = requestType;
        if (!state.RequestContext.RequestInfo->CallContext->LWOrbit.Fork(
                callContext->LWOrbit))
        {
            FILESTORE_TRACK(
                ForkFailed,
                state.RequestContext.RequestInfo->CallContext,
                GetFileStoreRequestName(requestType));
        }
        state.InFlightRequest.ConstructInPlace(
            TRequestInfo(
                state.RequestContext.RequestInfo->Sender,
                state.RequestContext.RequestInfo->Cookie,
                std::move(callContext)),
            ProfileLog,
            state.RequestContext.MediaKind,
            state.RequestContext.RequestStats);
        state.InFlightRequest->Start(ctx.Now());
        InitProfileLogRequestInfo(
            state.InFlightRequest->AccessProfileLogRequest(),
            record);

        if (addWriteRangeInfo) {
            auto* rangeInfo =
                state.InFlightRequest->AccessProfileLogRequest().AddRanges();
            rangeInfo->SetNodeId(state.RequestContext.WriteRequest.GetNodeId());
            rangeInfo->SetHandle(state.RequestContext.WriteRequest.GetHandle());
            rangeInfo->SetOffset(state.RequestContext.WriteRequest.GetOffset());
            rangeInfo->SetBytes(
                NFileStore::CalculateByteCount(state.RequestContext.WriteRequest));
        }

        auto* trace =
            record.MutableHeaders()->MutableInternal()->MutableTrace();
        TraceSerializer->BuildTraceRequest(
            *trace,
            state.RequestContext.RequestInfo->CallContext->LWOrbit);
    }

    void AddData(const TActorContext& ctx)
    {
        auto& state = GetState();

        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            state.RequestContext.RequestInfo->CallContext,
            "AddData");

        auto request = std::make_unique<TEvIndexTablet::TEvAddDataRequest>();

        request->Record.MutableHeaders()->CopyFrom(
            state.RequestContext.WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(
            state.RequestContext.WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(state.RequestContext.WriteRequest.GetNodeId());
        request->Record.SetHandle(state.RequestContext.WriteRequest.GetHandle());
        request->Record.SetOffset(state.RequestContext.WriteRequest.GetOffset());
        request->Record.SetLength(
            NFileStore::CalculateByteCount(state.RequestContext.WriteRequest));
        for (const auto& blob: state.GenerateBlobIdsResponse.GetBlobs()) {
            *request->Record.AddBlobIds() = blob.GetBlobId();
        }
        request->Record.SetCommitId(
            state.GenerateBlobIdsResponse.GetCommitId());

        FillUnalignedDataRanges(*request->Record.MutableUnalignedDataRanges());

        AddStorageStatusInfo(request->Record);

        PrepareTabletRequest(
            ctx,
            EFileStoreRequest::AddData,
            request->Record,
            false /* addWriteRangeInfo */);
        request->CallContext = state.InFlightRequest->CallContext;

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Sending AddData request to tablet: %s",
            state.RequestContext.LogTag.c_str(),
            request->Record.DebugString().Quote().c_str());

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void HandleAddDataResponse(
        const TEvIndexTablet::TEvAddDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto& state = GetState();
        const auto& LogTag = state.RequestContext.LogTag;

        auto* msg = ev->Get();

        SERVICE_VERIFY(state.InFlightRequest);
        state.RequestContext.RequestInfo->CallContext->LWOrbit.Join(
            state.InFlightRequest->CallContext->LWOrbit);
        FinalizeProfileLogRequestInfo(
            state.InFlightRequest->AccessProfileLogRequest(),
            msg->Record);
        state.InFlightRequest->Complete(ctx.Now(), msg->GetError());

        if (HasError(msg->GetError())) {
            LogFallbackToWriteData(ctx, msg->GetError());
            WriteData(ctx, true /* isFallback */);
            return;
        }

        ReplyAndDie(ctx, {} /* record */);
    }

    void ConfirmAddData(const TActorContext& ctx)
    {
        auto& state = GetState();

        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            state.RequestContext.RequestInfo->CallContext,
            "ConfirmAddData");

        auto request =
            std::make_unique<TEvIndexTablet::TEvConfirmAddDataRequest>();

        request->Record.MutableHeaders()->CopyFrom(
            state.RequestContext.WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(
            state.RequestContext.WriteRequest.GetFileSystemId());
        request->Record.SetCommitId(
            state.GenerateBlobIdsResponse.GetCommitId());

        // Add storage status info (now available after WriteBlobs completed)
        AddStorageStatusInfo(request->Record);

        PrepareTabletRequest(
            ctx,
            EFileStoreRequest::ConfirmAddData,
            request->Record,
            true);
        request->CallContext = state.InFlightRequest->CallContext;

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Sending ConfirmAddData request, commitId=%lu",
            state.RequestContext.LogTag.c_str(),
            state.GenerateBlobIdsResponse.GetCommitId());

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void CancelAddData(const TActorContext& ctx)
    {
        auto& state = GetState();

        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            state.RequestContext.RequestInfo->CallContext,
            "CancelAddData");

        auto request =
            std::make_unique<TEvIndexTablet::TEvCancelAddDataRequest>();

        request->Record.MutableHeaders()->CopyFrom(
            state.RequestContext.WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(
            state.RequestContext.WriteRequest.GetFileSystemId());
        request->Record.SetCommitId(
            state.GenerateBlobIdsResponse.GetCommitId());

        PrepareTabletRequest(
            ctx,
            EFileStoreRequest::CancelAddData,
            request->Record,
            true);
        request->CallContext = state.InFlightRequest->CallContext;

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Sending CancelAddData request, commitId=%lu",
            state.RequestContext.LogTag.c_str(),
            state.GenerateBlobIdsResponse.GetCommitId());

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void HandleConfirmAddDataResponse(
        const TEvIndexTablet::TEvConfirmAddDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto& state = GetState();
        const auto& LogTag = state.RequestContext.LogTag;

        auto* msg = ev->Get();

        SERVICE_VERIFY(state.InFlightRequest);
        state.RequestContext.RequestInfo->CallContext->LWOrbit.Join(
            state.InFlightRequest->CallContext->LWOrbit);
        FinalizeProfileLogRequestInfo(
            state.InFlightRequest->AccessProfileLogRequest(),
            msg->Record);
        state.InFlightRequest->Complete(ctx.Now(), msg->GetError());

        if (HasError(msg->GetError())) {
            if (ev->Sender == MakeIndexTabletProxyServiceId()) {
                ScheduleTabletProxyRetry(
                    ctx,
                    ETabletRetryRequest::ConfirmAddData,
                    msg->GetError());
                return;
            }

            ResetTabletProxyRetryState();
            LogFallbackToWriteData(ctx, msg->GetError());
            return WriteData(ctx, true /* isFallback */);
        }

        ResetTabletProxyRetryState();
        ReplyAndDie(ctx, {});
    }

    void HandleCancelAddDataResponse(
        const TEvIndexTablet::TEvCancelAddDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto& state = GetState();
        const auto& LogTag = state.RequestContext.LogTag;

        auto* msg = ev->Get();

        SERVICE_VERIFY(state.InFlightRequest);
        state.RequestContext.RequestInfo->CallContext->LWOrbit.Join(
            state.InFlightRequest->CallContext->LWOrbit);
        FinalizeProfileLogRequestInfo(
            state.InFlightRequest->AccessProfileLogRequest(),
            msg->Record);
        state.InFlightRequest->Complete(ctx.Now(), msg->GetError());

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
        LogFallbackToWriteData(ctx, state.WriteBlobError);
        WriteData(ctx, true /* isFallback */);
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        auto& state = GetState();

        Y_UNUSED(ev);
        if (state.TabletRetryRequest == ETabletRetryRequest::None) {
            return;
        }

        const auto retryRequest = state.TabletRetryRequest;
        state.TabletRetryRequest = ETabletRetryRequest::None;

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
        auto& state = GetState();
        const auto& LogTag = state.RequestContext.LogTag;

        const auto delay =
            state.TabletProxyRetryDelayProvider.GetDelayAndIncrease();
        ++state.TabletProxyRetries;

        if (state.TabletProxyRetries % ProxyCriticalRetryThreshold == 0) {
            SERVICE_VERIFY(retryRequest != ETabletRetryRequest::None);

            const auto requestType =
                retryRequest == ETabletRetryRequest::ConfirmAddData
                ? EFileStoreRequest::ConfirmAddData
                : EFileStoreRequest::CancelAddData;

            ReportUnconfirmedFlowProxyRetryThresholdReached(
                TStringBuilder()
                << "retry=" << state.TabletProxyRetries
                << ", request=" << GetFileStoreRequestName(requestType)
                << ", error=" << FormatError(error));
        }

        state.TabletRetryRequest = retryRequest;
        ctx.Schedule(delay, new TEvents::TEvWakeup());
    }

    void ResetTabletProxyRetryState()
    {
        auto& state = GetState();

        state.TabletProxyRetries = 0;
        state.TabletRetryRequest = ETabletRetryRequest::None;
        state.TabletProxyRetryDelayProvider.Reset();
    }

    inline void LogFallbackToWriteData(
        const TActorContext& ctx,
        const NProto::TError& error)
    {
        auto& state = GetState();

        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s Falling back to WriteData for %lu, %lu, %lu (%lu bytes). "
            "Message: %s",
            state.RequestContext.LogTag.c_str(),
            state.RequestContext.WriteRequest.GetNodeId(),
            state.RequestContext.WriteRequest.GetHandle(),
            state.RequestContext.WriteRequest.GetOffset(),
            NFileStore::CalculateByteCount(state.RequestContext.WriteRequest),
            FormatError(error).Quote().c_str());
    }

    /**
     * @brief Use regular write if three-stage write fails for any reason or
     * disabled in filesystem configuration
     * @param isFallback true if this is a fallback write after three-stage
     * write failed
     */
    void WriteData(const TActorContext& ctx, bool isFallback)
    {
        auto& state = GetState();

        FILESTORE_TRACK(
            RequestReceived_ServiceWorker,
            state.RequestContext.RequestInfo->CallContext,
            "WriteData");

        auto request = std::make_unique<TEvService::TEvWriteDataRequest>();
        request->Record = std::move(state.RequestContext.WriteRequest);
        PrepareWriteDataRequestPayload(
            *request,
            state.RequestContext.ExternalWriteDataPayloadEnabled);
        if (isFallback) {
            request->Record.MutableHeaders()->SetThrottlingDisabled(true);
        }
        request->CallContext = state.RequestContext.RequestInfo->CallContext;
        auto* trace =
            request->Record.MutableHeaders()->MutableInternal()->MutableTrace();
        TraceSerializer->BuildTraceRequest(
            *trace,
            state.RequestContext.RequestInfo->CallContext->LWOrbit);

        // forward request through tablet proxy
        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
    }

    void HandleWriteDataResponse(
        const TEvService::TEvWriteDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto& state = GetState();

        auto* msg = ev->Get();

        if (HasError(msg->GetError())) {
            HandleError(ctx, msg->GetError());
            return;
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "%s WriteData succeeded",
            state.RequestContext.LogTag.c_str());

        ReplyAndDie(ctx, std::move(msg->Record));
    }

    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TWriteDataResponse record)
    {
        auto& state = GetState();

        FILESTORE_TRACK(
            ResponseSent_ServiceWorker,
            state.RequestContext.RequestInfo->CallContext,
            "WriteData");

        auto response = std::make_unique<TEvService::TEvWriteDataResponse>();
        response->Record = std::move(record);
        NCloud::Reply(
            ctx,
            *state.RequestContext.RequestInfo,
            std::move(response));
        Die(ctx);
    }

    void HandleError(const TActorContext& ctx, const NProto::TError& error)
    {
        auto& state = GetState();

        FILESTORE_TRACK(
            ResponseSent_ServiceWorker,
            state.RequestContext.RequestInfo->CallContext,
            "WriteData");

        auto response =
            std::make_unique<TEvService::TEvWriteDataResponse>(error);
        NCloud::Reply(
            ctx,
            *state.RequestContext.RequestInfo,
            std::move(response));
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
        auto& state = GetState();
        const auto& LogTag = state.RequestContext.LogTag;

        if (state.RequestContext.Range.Offset < state.BlobRange.Offset) {
            auto& unalignedHead = *ranges.Add();
            unalignedHead.SetOffset(state.RequestContext.Range.Offset);
            const ui64 length =
                state.BlobRange.Offset - state.RequestContext.Range.Offset;
            if (state.RequestContext.WriteRequest.GetIovecs().empty()) {
                unalignedHead.SetContent(
                    state.RequestContext.WriteRequest.GetBuffer().substr(
                        0,
                        length));
            } else {
                TString buffer;
                auto bytesCopied =
                    CopyBufferFromRope(state.Rope, buffer, 0, length);
                SERVICE_VERIFY(bytesCopied == length);
                unalignedHead.SetContent(std::move(buffer));
            }
        }

        if (state.RequestContext.Range.End() > state.BlobRange.End()) {
            auto& unalignedTail = *ranges.Add();
            unalignedTail.SetOffset(state.BlobRange.End());
            const auto offset =
                state.BlobRange.End() - state.RequestContext.Range.Offset;
            const auto length =
                state.RequestContext.Range.End() - state.BlobRange.End();
            if (state.RequestContext.WriteRequest.GetIovecs().empty()) {
                unalignedTail.SetContent(
                    state.RequestContext.WriteRequest.GetBuffer().substr(
                        offset,
                        length));
            } else {
                TString buffer;
                auto bytesCopied =
                    CopyBufferFromRope(state.Rope, buffer, offset, length);
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

    auto* session =
        GetAndValidateSession<TEvService::TWriteDataMethod>(ctx, ev);
    if (!session) {
        return;
    }

    if (TryHandleControlNamespaceWriteData(ctx, ev, session)) {
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

    auto logTag = filestore.GetFileSystemId();
    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        session->MediaKind,
        session->RequestStats,
        startTime);

    InitProfileLogRequestInfo(inflight->AccessProfileLogRequest(), msg->Record);
    inflight->AccessProfileLogRequest().SetClientId(session->ClientId);
    if (blockChecksumsEnabled) {
        CalculateWriteDataRequestChecksums(
            msg->Record,
            blockSize,
            inflight->AccessProfileLogRequest());
    }

    auto requestInfo = CreateRequestInfo(SelfId(), cookie, msg->CallContext);

    auto actor = std::make_unique<TWriteDataActor>(ProfileLog, TraceSerializer);
    actor->Initialize(TWriteRequestContext(
        std::move(msg->Record),
        range,
        std::move(requestInfo),
        std::move(logTag),
        filestore.GetFeatures().GetWriteBlobDisabled(),
        filestore.GetFeatures().GetUnconfirmedFlowEnabled(),
        session->RequestStats,
        session->MediaKind,
        threeStageWriteEnabled,
        filestore.GetFeatures().GetExternalWriteDataPayloadEnabled(),
        filestore.GetFeatures().GetEnableRdmaForWrites()));
    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
