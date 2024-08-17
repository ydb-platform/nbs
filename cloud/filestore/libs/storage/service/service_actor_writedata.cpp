#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
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
    const NCloud::NProto::EStorageMediaKind MediaKind;

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
        auto request =
            std::make_unique<TEvIndexTablet::TEvGenerateBlobIdsRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(WriteRequest.GetNodeId());
        request->Record.SetHandle(WriteRequest.GetHandle());
        request->Record.SetOffset(BlobRange.Offset);
        request->Record.SetLength(BlobRange.Length);

        RequestInfo->CallContext->RequestType = EFileStoreRequest::GenerateBlobIds;
        InFlightRequest.ConstructInPlace(
            TRequestInfo(
                RequestInfo->Sender,
                RequestInfo->Cookie,
                RequestInfo->CallContext),
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
            "WriteDataActor started, data size: %lu, offset: %lu"
            ", aligned size: %lu, aligned offset: %lu",
            WriteRequest.GetBuffer().size(),
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
                HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
                break;
        }
    }

    void HandleGenerateBlobIdsResponse(
        const TEvIndexTablet::TEvGenerateBlobIdsResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        TABLET_VERIFY(InFlightRequest);

        InFlightRequest->Complete(ctx.Now(), msg->GetError());
        FinalizeProfileLogRequestInfo(
            InFlightRequest->ProfileLogRequest,
            msg->Record);
        // After the GenerateBlobIds response is received, we continue to consider the
        // request as a WriteData request
        RequestInfo->CallContext->RequestType = EFileStoreRequest::WriteData;

        if (HasError(msg->GetError())) {
            WriteData(ctx, msg->GetError());
            return;
        }

        GenerateBlobIdsResponse.CopyFrom(msg->Record);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "GenerateBlobIds response received: %s",
            GenerateBlobIdsResponse.DebugString().Quote().c_str());

        WriteBlobs(ctx);
    }

    void WriteBlobs(const TActorContext& ctx)
    {
        RemainingBlobsToWrite = GenerateBlobIdsResponse.BlobsSize();
        ui64 offset = BlobRange.Offset - Range.Offset;

        RequestInfo->CallContext->RequestType = EFileStoreRequest::WriteBlob;
        InFlightBSRequests.reserve(RemainingBlobsToWrite);
        StorageStatusFlags.resize(GenerateBlobIdsResponse.BlobsSize());
        for (const auto& blob: GenerateBlobIdsResponse.GetBlobs()) {
            NKikimr::TLogoBlobID blobId =
                LogoBlobIDFromLogoBlobID(blob.GetBlobId());
            InFlightBSRequests.emplace_back(std::make_unique<TInFlightRequest>(
                TRequestInfo(
                    RequestInfo->Sender,
                    RequestInfo->Cookie,
                    RequestInfo->CallContext),
                ProfileLog,
                MediaKind,
                RequestStats));
            InFlightBSRequests.back()->Start(ctx.Now());

            std::unique_ptr<TEvBlobStorage::TEvPut> request;
            if (GenerateBlobIdsResponse.BlobsSize() == 1
                    && Range == BlobRange)
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
                        WriteRequest.GetBuffer().Data() + offset,
                        blobId.BlobSize()),
                    TInstant::Max(),
                    NKikimrBlobStorage::UserData);
            }
            NKikimr::TActorId proxy =
                MakeBlobStorageProxyID(blob.GetBSGroupId());
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "Sending TEvPut request to blob storage, blobId: %s, proxy: %s",
                blobId.ToString().c_str(),
                proxy.ToString().c_str());
            SendToBSProxy(ctx, proxy, request.release(), blobId.Cookie());
            offset += blobId.BlobSize();
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
                "WriteData error: %s",
                msg->ErrorReason.Quote().c_str());
            // We still may receive some responses, but we do not want to
            // process them
            return WriteData(ctx, error);
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "TEvPutResult response received: %s",
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

        --RemainingBlobsToWrite;
        if (RemainingBlobsToWrite == 0) {
            RequestInfo->CallContext->RequestType =
                EFileStoreRequest::WriteData;
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
        request->Record.SetLength(WriteRequest.GetBuffer().size());
        for (auto& blob: *GenerateBlobIdsResponse.MutableBlobs()) {
            request->Record.AddBlobIds()->Swap(blob.MutableBlobId());
        }
        request->Record.SetCommitId(GenerateBlobIdsResponse.GetCommitId());
        request->Record.MutableStorageStatusFlags()->Reserve(
            StorageStatusFlags.size());
        for (const auto flags: StorageStatusFlags) {
            request->Record.AddStorageStatusFlags(flags);
        }

        if (Range.Offset < BlobRange.Offset) {
            auto& unalignedHead = *request->Record.AddUnalignedDataRanges();
            unalignedHead.SetOffset(Range.Offset);
            unalignedHead.SetContent(WriteRequest.GetBuffer().substr(
                0,
                BlobRange.Offset - Range.Offset));
        }

        if (Range.End() > BlobRange.End()) {
            auto& unalignedTail = *request->Record.AddUnalignedDataRanges();
            unalignedTail.SetOffset(BlobRange.End());
            unalignedTail.SetContent(WriteRequest.GetBuffer().substr(
                BlobRange.End() - Range.Offset,
                Range.End() - BlobRange.End()));
        }

        RequestInfo->CallContext->RequestType = EFileStoreRequest::AddData;
        InFlightRequest.ConstructInPlace(
            TRequestInfo(
                RequestInfo->Sender,
                RequestInfo->Cookie,
                RequestInfo->CallContext),
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
            "Sending AddData request to tablet: %s",
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
        RequestInfo->CallContext->RequestType = EFileStoreRequest::WriteData;

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
        RequestInfo->CallContext->RequestType = EFileStoreRequest::WriteData;

        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "Falling back to WriteData for %lu, %lu, %lu (%lu bytes). Message: "
            "%s",
            WriteRequest.GetNodeId(),
            WriteRequest.GetHandle(),
            WriteRequest.GetOffset(),
            WriteRequest.GetBuffer().size(),
            FormatError(error).Quote().c_str());

        auto request = std::make_unique<TEvService::TEvWriteDataRequest>();
        request->Record = std::move(WriteRequest);

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

        LOG_DEBUG(ctx, TFileStoreComponents::SERVICE, "WriteData succeeded");

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

    auto [fsId, error] = SelectShard(
        ctx,
        sessionId,
        seqNo,
        msg->Record.GetHeaders().GetDisableMultiTabletForwarding(),
        TEvService::TWriteDataMethod::Name,
        msg->CallContext->RequestId,
        filestore,
        ExtractShardNo(msg->Record.GetHandle()));

    if (HasError(error)) {
        auto response = std::make_unique<TEvService::TEvWriteDataResponse>(
            std::move(error));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    if (fsId) {
        msg->Record.SetFileSystemId(fsId);
    }

    if (!filestore.GetFeatures().GetThreeStageWriteEnabled()) {
        // If three-stage write is disabled, forward the request to the tablet
        // in the same way as all other requests.
        ForwardRequest<TEvService::TWriteDataMethod>(ctx, ev);
        return;
    }

    ui32 blockSize = filestore.GetBlockSize();

    const TByteRange range(
        msg->Record.GetOffset(),
        msg->Record.GetBuffer().Size(),
        blockSize);
    const bool threeStageWriteEnabled =
        range.Length >= filestore.GetFeatures().GetThreeStageWriteThreshold()
        && filestore.GetFeatures().GetThreeStageWriteEnabled()
        && (range.IsAligned()
                || StorageConfig->GetUnalignedThreeStageWriteEnabled());
    if (threeStageWriteEnabled) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Using three-stage write for request, size: %lu",
            msg->Record.GetBuffer().Size());

        auto [cookie, inflight] = CreateInFlightRequest(
            TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            session->MediaKind,
            session->RequestStats,
            ctx.Now());

        InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

        auto requestInfo =
            CreateRequestInfo(SelfId(), cookie, msg->CallContext);

        auto actor = std::make_unique<TWriteDataActor>(
            std::move(msg->Record),
            range,
            std::move(requestInfo),
            filestore.GetFileSystemId(),
            session->RequestStats,
            ProfileLog,
            session->MediaKind);
        NCloud::Register(ctx, std::move(actor));
    } else {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Forwarding WriteData request to tablet");
        return ForwardRequest<TEvService::TWriteDataMethod>(ctx, ev);
    }
}

}   // namespace NCloud::NFileStore::NStorage
