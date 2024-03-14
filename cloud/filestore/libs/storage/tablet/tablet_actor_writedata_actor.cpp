#include "tablet_actor_writedata_actor.h"

#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

TWriteDataActor::TWriteDataActor(
        ITraceSerializerPtr traceSerializer,
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TVector<TMergedBlob> blobs,
        TWriteRange writeRange,
        bool skipBlobStorage)
    : TraceSerializer(std::move(traceSerializer))
    , LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , Blobs(std::move(blobs))
    , WriteRange(writeRange)
    , SkipBlobStorage(skipBlobStorage)
{
    for (const auto& blob: Blobs) {
        BlobsSize += blob.BlobContent.Size();
    }
}

void TWriteDataActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "WriteData");

    WriteBlob(ctx);
    Become(&TThis::StateWork);
}

void TWriteDataActor::WriteBlob(const TActorContext& ctx)
{
    if (SkipBlobStorage) {
        // In the case, when there is already a blobId with data written to,
        // bypass BlobStorage access
        AddBlob(ctx);
        return;
    }

    auto request = std::make_unique<TEvIndexTabletPrivate::TEvWriteBlobRequest>(
        RequestInfo->CallContext
    );

    for (auto& blob: Blobs) {
        request->Blobs.emplace_back(blob.BlobId, std::move(blob.BlobContent));
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TWriteDataActor::HandleWriteBlobResponse(
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

void TWriteDataActor::AddBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvAddBlobRequest>(
        RequestInfo->CallContext
    );
    request->Mode = EAddBlobMode::Write;
    request->WriteRanges.push_back(WriteRange);

    for (const auto& blob: Blobs) {
        request->MergedBlobs.emplace_back(
            blob.BlobId,
            blob.Block,
            blob.BlocksCount);
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TWriteDataActor::HandleAddBlobResponse(
    const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TWriteDataActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TWriteDataActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    // notify tablet
    if (SkipBlobStorage) {
        NCloud::Send(
            ctx,
            Tablet,
            std::make_unique<TEvIndexTabletPrivate::TEvReleaseCollectBarrier>(
                CommitId));
    } else {
        using TCompletion = TEvIndexTabletPrivate::TEvWriteDataCompleted;
        auto response = std::make_unique<TCompletion>(error);
        response->CommitId = CommitId;
        response->Count = 1;
        response->Size = BlobsSize;
        response->Time = ctx.Now() - RequestInfo->StartedTs;
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "WriteData");

    if (RequestInfo->Sender != Tablet) {
        if (SkipBlobStorage) {
            auto response = std::make_unique<TEvIndexTablet::TEvAddDataResponse>(error);
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s AddData: #%lu completed (%s)",
                LogTag.c_str(),
                RequestInfo->CallContext->RequestId,
                FormatError(response->Record.GetError()).c_str());
            NCloud::Reply(ctx, *RequestInfo, std::move(response));
        } else {
            auto response =
                std::make_unique<TEvService::TEvWriteDataResponse>(error);
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s WriteData: #%lu completed (%s)",
                LogTag.c_str(),
                RequestInfo->CallContext->RequestId,
                FormatError(response->Record.GetError()).c_str());

            BuildTraceInfo(
                TraceSerializer,
                RequestInfo->CallContext,
                response->Record);
            BuildThrottlerInfo(*RequestInfo->CallContext, response->Record);

            NCloud::Reply(ctx, *RequestInfo, std::move(response));
        }
    }

    Die(ctx);
}

STFUNC(TWriteDataActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTabletPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvIndexTabletPrivate::TEvAddBlobResponse, HandleAddBlobResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace NCloud::NFileStore::NStorage
