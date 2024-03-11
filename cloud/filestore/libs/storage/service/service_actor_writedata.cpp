#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

TAtomicCounter REQUEST_COUNT = {0};

////////////////////////////////////////////////////////////////////////////////

class TWriteDataActor final: public TActorBootstrapped<TWriteDataActor>
{
private:
    // Original request
    NProto::TWriteDataRequest WriteRequest;
    const TRequestInfoPtr RequestInfo;

    // Issued blob id and asspciated data
    NProtoPrivate::TIssueBlobResponse IssueBlobResponse;
    NKikimr::TLogoBlobID BlobId;

public:
    TWriteDataActor(
        NProto::TWriteDataRequest request,
        TRequestInfoPtr requestInfo)
        : WriteRequest(std::move(request))
        , RequestInfo(std::move(requestInfo))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto request = std::make_unique<TEvIndexTablet::TEvIssueBlobRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(WriteRequest.GetNodeId());
        request->Record.SetHandle(WriteRequest.GetHandle());
        request->Record.SetOffset(WriteRequest.GetOffset());
        request->Record.SetLength(WriteRequest.GetBuffer().size());

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "WriteDataActor started, data size: %lu, request: %s",
            WriteRequest.GetBuffer().size(),
            request->Record.DebugString().c_str());

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());

        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(TEvBlobStorage::TEvPutResult, HandleWriteBlobResponse);

            HFunc(
                TEvIndexTablet::TEvIssueBlobResponse,
                HandleIssueBlobResponse);

            HFunc(TEvService::TEvWriteDataResponse, HandleWriteDataResponse)

                default
                : HandleUnexpectedEvent(
                      ev,
                      TFileStoreComponents::SERVICE_WORKER);
            break;
        }
    }

    void HandleIssueBlobResponse(
        const TEvIndexTablet::TEvIssueBlobResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        // TODO(debnatkh): proper error handling

        IssueBlobResponse.CopyFrom(msg->Record);
        BlobId = LogoBlobIDFromLogoBlobID(IssueBlobResponse.GetBlobId());

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "IssueBlob response received: %s, blobId: %s",
            IssueBlobResponse.DebugString().c_str(),
            BlobId.ToString().c_str());

        auto request = std::make_unique<TEvBlobStorage::TEvPut>(
            BlobId,
            WriteRequest.GetBuffer(),
            TInstant::Max());

        NKikimr::TActorId proxy =
            MakeBlobStorageProxyID(IssueBlobResponse.GetBSGroupId());

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Sending WriteBlob request to blob storage, blobId: %s, size: %lu",
            BlobId.ToString().c_str(),
            WriteRequest.GetBuffer().size());

        SendToBSProxy(ctx, proxy, request.release(), 57);
    }

    void HandleWriteBlobResponse(
        const TEvBlobStorage::TEvPutResult::TPtr& ev,
        const TActorContext& ctx)
    {
        // TODO(debnatkh): proper error handling

        const auto* msg = ev->Get();

        Y_UNUSED(ctx);
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "WriteBlob response received: %s",
            msg->ToString().c_str());

        if (msg->Status != NKikimrProto::OK) {
            const auto errorReason = FormatError(
                MakeError(MAKE_KIKIMR_ERROR(msg->Status), msg->ErrorReason));
            // TODO(debnatkh): proper fallback
            LOG_ERROR(
                ctx,
                TFileStoreComponents::SERVICE,
                "WriteBlob failed: %s",
                errorReason.c_str());
        } else {
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "WriteBlob response is OK");
        }

        // Now we can send TEv to tablet

        auto request =
            std::make_unique<TEvIndexTablet::TEvMarkWriteCompletedRequest>();

        request->Record.MutableHeaders()->CopyFrom(WriteRequest.GetHeaders());
        request->Record.SetFileSystemId(WriteRequest.GetFileSystemId());
        request->Record.SetNodeId(WriteRequest.GetNodeId());
        request->Record.SetHandle(WriteRequest.GetHandle());
        request->Record.SetOffset(WriteRequest.GetOffset());
        request->Record.SetLength(WriteRequest.GetBuffer().size());
        LogoBlobIDFromLogoBlobID(BlobId, request->Record.MutableBlobId());
        request->Record.SetCommitId(IssueBlobResponse.GetCommitId());

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Sending TwoStageWrite request to tablet, blobId: %s",
            BlobId.ToString().c_str());

        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());

        // Once the request is completed, ServiceActor will be notified by the
        // tablet (no, it will not)
    }

    void HandleWriteDataResponse(
        const TEvService::TEvWriteDataResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        auto response = std::make_unique<TEvService::TEvWriteDataResponse>();
        response->Record = std::move(msg->Record);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        Die(ctx);
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

    if (!filestore.GetFeatures().GetTwoStageWriteEnabled()) {
        // If two-stage write is disabled, forward the request to the tablet in
        // the same way as all other requests.
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

    // TODO(debnatkh): Support two-stage write for nonaligned writes.
    if (msg->Record.GetOffset() % blockSize == 0 &&
        msg->Record.GetBuffer().Size() == blockSize)
    {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Using two-stage write for request, size: %lu",
            msg->Record.GetBuffer().Size());
        NProto::TWriteDataRequest request;
        request.CopyFrom(msg->Record);

        auto requestInfo =
            CreateRequestInfo(SelfId(), cookie, msg->CallContext);

        auto actor = std::make_unique<TWriteDataActor>(
            std::move(request),
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
