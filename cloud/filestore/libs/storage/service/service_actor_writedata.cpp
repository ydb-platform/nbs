#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
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
    const TRequestInfoPtr RequestInfo;

    // generated blob id and associated data
    NProtoPrivate::TGenerateBlobIdsResponse GenerateBlobIdsResponse;

    // WriteData state
    ui32 RemainingBlobsToWrite = 0;

public:
    TWriteDataActor(
            NProto::TWriteDataRequest request,
            TRequestInfoPtr requestInfo)
        : WriteRequest(std::move(request))
        , RequestInfo(std::move(requestInfo))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto request =
            std::make_unique<TEvIndexTablet::TEvGenerateBlobIdsRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(WriteRequest.GetNodeId());
        request->Record.SetHandle(WriteRequest.GetHandle());
        request->Record.SetOffset(WriteRequest.GetOffset());
        request->Record.SetLength(WriteRequest.GetBuffer().size());

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "WriteDataActor started, data size: %lu, offset: %lu",
            WriteRequest.GetBuffer().size(),
            WriteRequest.GetOffset());

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
        ui64 offset = 0;

        for (const auto& blob: GenerateBlobIdsResponse.GetBlobs()) {
            NKikimr::TLogoBlobID blobId =
                LogoBlobIDFromLogoBlobID(blob.GetBlobId());
            std::unique_ptr<TEvBlobStorage::TEvPut> request;
            if (GenerateBlobIdsResponse.BlobsSize() == 1) {
                // do not copy the buffer if there is only one blob
                request = std::make_unique<TEvBlobStorage::TEvPut>(
                    blobId,
                    WriteRequest.GetBuffer(),
                    TInstant::Max());
            } else {
                request = std::make_unique<TEvBlobStorage::TEvPut>(
                    blobId,
                    TString(
                        WriteRequest.GetBuffer().Data() + offset,
                        blobId.BlobSize()),
                    TInstant::Max());
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
            RemainingBlobsToWrite = std::numeric_limits<ui32>::max();
            return WriteData(ctx, error);
        }

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "TEvPutResult response received: %s",
            msg->ToString().c_str());

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
        request->Record.SetLength(WriteRequest.GetBuffer().size());
        for (auto& blob: *GenerateBlobIdsResponse.MutableBlobs()) {
            request->Record.AddBlobIds()->Swap(blob.MutableBlobId());
        }
        request->Record.SetCommitId(GenerateBlobIdsResponse.GetCommitId());

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

    if (!filestore.GetFeatures().GetThreeStageWriteEnabled()) {
        // If three-stage write is disabled, forward the request to the tablet
        // in the same way as all other requests.
        ForwardRequest<TEvService::TWriteDataMethod>(ctx, ev);
        return;
    }

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        session->MediaKind,
        session->RequestStats,
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    ui32 blockSize = filestore.GetBlockSize();

    // TODO(debnatkh): Consider supporting unaligned writes
    if (filestore.GetFeatures().GetThreeStageWriteEnabled() &&
        msg->Record.GetOffset() % blockSize == 0 &&
        msg->Record.GetBuffer().Size() % blockSize == 0 &&
        msg->Record.GetBuffer().Size() >=
            filestore.GetFeatures().GetThreeStageWriteThreshold())
    {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Using three-stage write for request, size: %lu",
            msg->Record.GetBuffer().Size());

        auto requestInfo =
            CreateRequestInfo(SelfId(), cookie, msg->CallContext);

        auto actor = std::make_unique<TWriteDataActor>(
            std::move(msg->Record),
            std::move(requestInfo));
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
