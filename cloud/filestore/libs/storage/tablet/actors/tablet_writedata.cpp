#include "tablet_writedata.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/core/request_info.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TWriteDataActor::TWriteDataActor(
        ITraceSerializerPtr traceSerializer,
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TVector<TMergedBlob> blobs,
        TWriteRange writeRange)
    : TraceSerializer(std::move(traceSerializer))
    , LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , Blobs(std::move(blobs))
    , WriteRange(writeRange)
{
    for (const auto& blob: Blobs) {
        BlobsSize += blob.BlobContent.size();
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
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TWriteDataActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvWriteDataCompleted;
        auto response = std::make_unique<TCompletion>(
            error,
            TSet<ui32>(),
            CommitId,
            1,
            BlobsSize,
            ctx.Now() - RequestInfo->StartedTs);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "WriteData");

    if (RequestInfo->Sender != Tablet) {
        auto response = std::make_unique<TEvService::TEvWriteDataResponse>(error);
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
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

    Die(ctx);
}

STFUNC(TWriteDataActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTabletPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvIndexTabletPrivate::TEvAddBlobResponse, HandleAddBlobResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NFileStore::NStorage
