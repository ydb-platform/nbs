#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/stream/str.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TWriteBlobRequest
{
    TActorId Proxy;
    TLogoBlobID BlobId;
    std::unique_ptr<TEvBlobStorage::TEvPut> Request;

    TWriteBlobRequest(
            const TActorId& proxy,
            const TLogoBlobID& blobId,
            std::unique_ptr<TEvBlobStorage::TEvPut> request)
        : Proxy(proxy)
        , BlobId(blobId)
        , Request(std::move(request))
    {}
};

////////////////////////////////////////////////////////////////////////////////

TString DumpBlobIds(const TVector<TWriteBlobRequest>& requests)
{
    TStringStream out;

    out << requests[0].BlobId;
    for (size_t i = 1; i < requests.size(); ++i) {
        out << ", " << requests[i].BlobId;
    }

    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

class TWriteBlobActor final
    : public TActorBootstrapped<TWriteBlobActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const TString FileSystemId;
    const IProfileLogPtr ProfileLog;

    TVector<TWriteBlobRequest> Requests;
    ui32 TotalSize = 0;

    using TWriteResult =
        TEvIndexTabletPrivate::TWriteBlobCompleted::TWriteRequestResult;
    TVector<TWriteResult> WriteResults;

    NProto::TProfileLogRequestInfo ProfileLogRequest;

public:
    TWriteBlobActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        IProfileLogPtr profileLog,
        TVector<TWriteBlobRequest> requests,
        NProto::TProfileLogRequestInfo profileLogRequest);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequests(const TActorContext& ctx);
    void HandlePutResult(
        const TEvBlobStorage::TEvPutResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyError(
        const TActorContext& ctx,
        const TEvBlobStorage::TEvPutResult& response,
        const TString reason);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TWriteBlobActor::TWriteBlobActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        IProfileLogPtr profileLog,
        TVector<TWriteBlobRequest> requests,
        NProto::TProfileLogRequestInfo profileLogRequest)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , FileSystemId(std::move(fileSystemId))
    , ProfileLog(std::move(profileLog))
    , Requests(std::move(requests))
    , ProfileLogRequest(std::move(profileLogRequest))
{}

void TWriteBlobActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "WriteBlob");

    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TWriteBlobActor::SendRequests(const TActorContext& ctx)
{
    size_t requestIndex = 0;
    for (auto& request: Requests) {
        TotalSize += request.BlobId.BlobSize();

        SendToBSProxy(
            ctx,
            request.Proxy,
            request.Request.release(),
            requestIndex++);
    }
}

void TWriteBlobActor::HandlePutResult(
    const TEvBlobStorage::TEvPutResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    RequestInfo->CallContext->LWOrbit.Join(msg->Orbit);

    if (msg->Status != NKikimrProto::OK) {
        ReplyError(ctx, *msg, msg->ErrorReason);
        return;
    }

    WriteResults.push_back({
        msg->Id,
        msg->StatusFlags,
        msg->ApproximateFreeSpaceShare,
    });

    TABLET_VERIFY(WriteResults.size() <= Requests.size());
    if (WriteResults.size() == Requests.size()) {
        ReplyAndDie(ctx);
    }
}

void TWriteBlobActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TWriteBlobActor::ReplyError(
    const TActorContext& ctx,
    const TEvBlobStorage::TEvPutResult& response,
    const TString reason)
{
      LOG_ERROR(ctx, TFileStoreComponents::TABLET,
          "%s TEvBlobStorage::TEvPut failed: %s\n%s",
          LogTag.c_str(),
          reason.c_str(),
          response.Print(false).data());

      auto error = MakeError(E_REJECTED, "TEvBlobStorage::TEvPut failed: " + reason);
      ReplyAndDie(ctx, error);
}

void TWriteBlobActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    const auto t = ctx.Now()
        - TInstant::MicroSeconds(ProfileLogRequest.GetTimestampMcs());

    // log request
    FinalizeProfileLogRequestInfo(
        std::move(ProfileLogRequest),
        ctx.Now(),
        FileSystemId,
        error,
        ProfileLog);

    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvWriteBlobCompleted;
        auto response = std::make_unique<TCompletion>(
            error,
            Requests.size(),
            TotalSize,
            t,
            std::move(WriteResults));
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "WriteBlob");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response =
            std::make_unique<TEvIndexTabletPrivate::TEvWriteBlobResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TWriteBlobActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvPutResult, HandlePutResult);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleWriteBlob(
    const TEvIndexTabletPrivate::TEvWriteBlobRequest::TPtr& ev,
    const TActorContext& ctx)
{
    NProto::TProfileLogRequestInfo profileLogRequest;
    InitProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreSystemRequest::WriteBlob,
        ctx.Now());

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    FILESTORE_TRACK(
        BackgroundRequestReceived_Tablet,
        requestInfo->CallContext,
        "WriteBlob");

    TVector<TWriteBlobRequest> requests(Reserve(msg->Blobs.size()));
    for (auto& blob: msg->Blobs) {
        if (blob.BlobContent.size() >= MaxBlobStorageBlobSize) {
            TStringStream ss;
            ss << blob.BlobId;

            LOG_ERROR(ctx, TFileStoreComponents::TABLET,
                "%s Too large blob [%s] %lu",
                LogTag.c_str(),
                ss.Str().c_str(),
                blob.BlobContent.size());

            ReportTabletBSFailure();
            Suicide(ctx);
            return;
        }

        const auto compRate = Config->GetBlobCompressionRate();
        if (BlobCodec && compRate && blob.BlobId.GetHash() % compRate == 0) {
            size_t compressedSize = 0;

            const auto chunkSize = Config->GetBlobCompressionChunkSize();
            TString chunk;
            chunk.ReserveAndResize(chunkSize);

            const auto& data = blob.BlobContent;
            for (auto begin = data.begin(); begin < data.end(); begin += chunkSize) {
                const auto end = Min(begin + chunkSize, data.end());
                compressedSize += BlobCodec->Compress(
                    TStringBuf(begin, end),
                    chunk.begin());
            }

            Metrics.UncompressedBytesWritten.fetch_add(
                data.size(),
                std::memory_order_relaxed);
            Metrics.CompressedBytesWritten.fetch_add(
                compressedSize,
                std::memory_order_relaxed);
        }

        auto blobId = MakeBlobId(TabletID(), blob.BlobId);

        auto proxy = Info()->BSProxyIDForChannel(
            blob.BlobId.Channel(),
            blob.BlobId.Generation());

        auto request = std::make_unique<TEvBlobStorage::TEvPut>(
            blobId,
            std::move(blob.BlobContent),
            blob.Deadline,
            blob.Async
                ? NKikimrBlobStorage::AsyncBlob
                : NKikimrBlobStorage::UserData);

        if (!msg->CallContext->LWOrbit.Fork(request->Orbit)) {
            FILESTORE_TRACK(
                ForkFailed,
                msg->CallContext,
                "TEvBlobStorage::TEvPut");
        }

        requests.emplace_back(proxy, blobId, std::move(request));

        AddRange(
            blob.BlobId.CommitId(),
            0,
            blob.BlobContent.size(),
            profileLogRequest);
    }

    LOG_TRACE(ctx, TFileStoreComponents::TABLET,
        "%s WriteBlob started (%s)",
        LogTag.c_str(),
        DumpBlobIds(requests).c_str());

    auto actor = std::make_unique<TWriteBlobActor>(
        LogTag,
        ctx.SelfID,
        std::move(requestInfo),
        GetFileSystemId(),
        ProfileLog,
        std::move(requests),
        std::move(profileLogRequest));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::RegisterEvPutResult(
    const TActorContext& ctx,
    ui32 generation,
    ui32 channel,
    const NKikimr::TStorageStatusFlags flags,
    double freeSpaceShare)
{
    const auto validFlag = NKikimrBlobStorage::EStatusFlags::StatusIsValid;
    if (flags.Check(validFlag)) {
        ui32 group = Info()->GroupFor(channel, generation);

        bool writable = true;
        bool toMove = false;

        if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
            LOG_WARN(ctx, TFileStoreComponents::TABLET,
                "%s Yellow move flag received for channel %u and group %u",
                LogTag.c_str(),
                channel,
                group);

            toMove = true;
        }
        if (flags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
            LOG_WARN(ctx, TFileStoreComponents::TABLET,
                "%s Yellow stop flag received for channel %u and group %u",
                LogTag.c_str(),
                channel,
                group);

            writable = false;
        }

        UpdateChannelStats(channel, writable, toMove, freeSpaceShare);

        ReassignDataChannelsIfNeeded(ctx);
    }
}

void TIndexTabletActor::HandleWriteBlobCompleted(
    const TEvIndexTabletPrivate::TEvWriteBlobCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s WriteBlob completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    WorkerActors.erase(ev->Sender);

    Metrics.WriteBlob.Update(msg->Count, msg->Size, msg->Time);

    for (const auto& result: msg->Results) {
        RegisterEvPutResult(
            ctx,
            result.BlobId.Generation(),
            result.BlobId.Channel(),
            result.StorageStatusFlags,
            result.ApproximateFreeSpaceShare);
    }

    if (FAILED(msg->GetStatus())) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s Stop tablet because of WriteBlob error: %s",
            LogTag.c_str(),
            FormatError(msg->GetError()).data());

        ReportTabletBSFailure();
        Suicide(ctx);
        return;
    }
}

}   // namespace NCloud::NFileStore::NStorage
