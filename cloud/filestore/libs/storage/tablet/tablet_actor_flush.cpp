#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFlushActor final
    : public TActorBootstrapped<TFlushActor>
{
private:
    const TActorId Tablet;
    const TString FileSystemId;
    const TRequestInfoPtr RequestInfo;
    const ui64 CommitId;
    const ui32 BlockSize;
    const IProfileLogPtr ProfileLog;
    /*const*/ TVector<TMixedBlob> Blobs;
    ui32 OperationSize = 0;

    NProto::TProfileLogRequestInfo ProfileLogRequest;

public:
    TFlushActor(
        const TActorId& tablet,
        TString fileSystemId,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        ui32 blockSize,
        IProfileLogPtr profileLog,
        TVector<TMixedBlob> blobs,
        NProto::TProfileLogRequestInfo profileLogRequest);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void WriteBlob(const TActorContext& ctx);
    void HandleWriteBlobResponse(
        const TEvIndexTabletPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void AddBlob(const TActorContext& ctx);
    void HandleAddBlobResponse(
        const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TFlushActor::TFlushActor(
        const TActorId& tablet,
        TString fileSystemId,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        ui32 blockSize,
        IProfileLogPtr profileLog,
        TVector<TMixedBlob> blobs,
        NProto::TProfileLogRequestInfo profileLogRequest)
    : Tablet(tablet)
    , FileSystemId(std::move(fileSystemId))
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , BlockSize(blockSize)
    , ProfileLog(std::move(profileLog))
    , Blobs(std::move(blobs))
    , ProfileLogRequest(std::move(profileLogRequest))
{
    for (const auto& b: Blobs) {
        OperationSize += b.Blocks.size() * BlockSize;
    }
}

void TFlushActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "Flush");

    AddBlobsInfo(BlockSize, Blobs, ProfileLogRequest);

    WriteBlob(ctx);
    Become(&TThis::StateWork);
}

void TFlushActor::WriteBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvWriteBlobRequest>(
        RequestInfo->CallContext
    );

    for (auto& blob: Blobs) {
        request->Blobs.emplace_back(blob.BlobId, std::move(blob.BlobContent));
        request->Blobs.back().Async = true;
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TFlushActor::HandleWriteBlobResponse(
    const TEvIndexTabletPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    AddBlob(ctx);
}

void TFlushActor::AddBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvAddBlobRequest>(
        RequestInfo->CallContext
    );
    request->Mode = EAddBlobMode::Flush;

    for (auto& blob: Blobs) {
        request->MixedBlobs.emplace_back(blob.BlobId, std::move(blob.Blocks));
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TFlushActor::HandleAddBlobResponse(
    const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TFlushActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TFlushActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    // log request
    FinalizeProfileLogRequestInfo(
        std::move(ProfileLogRequest),
        ctx.Now(),
        FileSystemId,
        error,
        ProfileLog);

    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvFlushCompleted;
        auto response = std::make_unique<TCompletion>(
            error,
            TSet<ui32>(),
            CommitId,
            1,
            OperationSize,
            ctx.Now() - RequestInfo->StartedTs);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "Flush");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response =
            std::make_unique<TEvIndexTabletPrivate::TEvFlushResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TFlushActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTabletPrivate::TEvWriteBlobResponse,
            HandleWriteBlobResponse);
        HFunc(TEvIndexTabletPrivate::TEvAddBlobResponse, HandleAddBlobResponse);

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

void TIndexTabletActor::EnqueueFlushIfNeeded(const TActorContext& ctx)
{
    auto freshBlocksDataSize = GetFreshBlocksCount() * GetBlockSize();
    if (freshBlocksDataSize < Config->GetFlushThreshold()) {
        return;
    }

    if (FlushState.Enqueue()) {
        ctx.Send(SelfId(), new TEvIndexTabletPrivate::TEvFlushRequest());
    }
}

void TIndexTabletActor::HandleFlush(
    const TEvIndexTabletPrivate::TEvFlushRequest::TPtr& ev,
    const TActorContext& ctx)
{
    NProto::TProfileLogRequestInfo profileLogRequest;
    InitProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreSystemRequest::Flush,
        ctx.Now());

    auto* msg = ev->Get();

    FILESTORE_TRACK(
        BackgroundTaskStarted_Tablet,
        msg->CallContext,
        "Flush",
        msg->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    auto replyError = [&] (
        const TActorContext& ctx,
        auto& ev,
        NProto::TProfileLogRequestInfo profileLogRequest,
        const NProto::TError& error)
    {
        // log request
        FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            error,
            ProfileLog);

        FILESTORE_TRACK(
            ResponseSent_Tablet,
            ev.Get()->CallContext,
            "Flush");

        if (ev.Sender != ctx.SelfID) {
            // reply to caller
            auto response = std::make_unique<TEvIndexTabletPrivate::TEvFlushResponse>(error);
            NCloud::Reply(ctx, ev, std::move(response));
        }
    };

    if (!CompactionStateLoadStatus.Finished) {
        if (FlushState.GetOperationState() == EOperationState::Enqueued) {
            FlushState.Complete();
        }

        replyError(
            ctx,
            *ev,
            std::move(profileLogRequest),
            MakeError(E_TRY_AGAIN, "compaction state not loaded yet")
        );

        return;
    }

    if (!FlushState.Start()) {
        replyError(
            ctx,
            *ev,
            std::move(profileLogRequest),
            MakeError(E_TRY_AGAIN, "flush is in progress"));
        return;
    }

    TMixedBlobBuilder builder(
        GetRangeIdHasher(),
        GetBlockSize(),
        CalculateMaxBlocksInBlob(Config->GetMaxBlobSize(), GetBlockSize()));

    FindFreshBlocks(builder);

    auto blobs = builder.Finish();
    if (!blobs) {
        replyError(
            ctx,
            *ev,
            std::move(profileLogRequest),
            MakeError(S_FALSE, "nothing to flush"));
        FlushState.Complete();

        return;
    }

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Flush started (%u blocks in %u blobs)",
        LogTag.c_str(),
        builder.GetBlocksCount(),
        builder.GetBlobsCount());

    ui64 commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "Flush");
    }

    ui32 blobIndex = 0;
    for (auto& blob: blobs) {
        const auto ok = GenerateBlobId(
            commitId,
            blob.BlobContent.size(),
            blobIndex++,
            &blob.BlobId);

        if (!ok) {
            ReassignDataChannelsIfNeeded(ctx);

            replyError(
                ctx,
                *ev,
                std::move(profileLogRequest),
                MakeError(E_FS_OUT_OF_SPACE, "failed to generate blobId"));

            FlushState.Complete();

            return;
        }
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AcquireCollectBarrier(commitId);

    auto actor = std::make_unique<TFlushActor>(
        ctx.SelfID,
        GetFileSystemId(),
        std::move(requestInfo),
        commitId,
        GetBlockSize(),
        ProfileLog,
        std::move(blobs),
        std::move(profileLogRequest));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleFlushCompleted(
    const TEvIndexTabletPrivate::TEvFlushCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Flush completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    TABLET_VERIFY(TryReleaseCollectBarrier(msg->CommitId));
    FlushState.Complete();

    WorkerActors.erase(ev->Sender);
    EnqueueFlushIfNeeded(ctx);
    EnqueueBlobIndexOpIfNeeded(ctx);

    Metrics.Flush.Update(msg->Count, msg->Size, msg->Time);
}

}   // namespace NCloud::NFileStore::NStorage
