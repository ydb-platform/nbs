#include "tablet_adddata.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/probes.h>
#include <cloud/filestore/libs/storage/core/request_info.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TAddDataActor::TAddDataActor(
        ITraceSerializerPtr traceSerializer,
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TVector<TMergedBlob> blobs,
        TVector<TBlockBytesMeta> unalignedDataParts,
        TWriteRange writeRange)
    : TraceSerializer(std::move(traceSerializer))
    , LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , Blobs(std::move(blobs))
    , UnalignedDataParts(std::move(unalignedDataParts))
    , WriteRange(writeRange)
{
    for (const auto& blob: Blobs) {
        BlobsSize += blob.BlobId.BlobSize();
    }
}

void TAddDataActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "AddData");

    AddBlob(ctx);
    Become(&TThis::StateWork);
}

void TAddDataActor::AddBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvAddBlobRequest>(
        RequestInfo->CallContext);
    request->Mode = EAddBlobMode::Write;
    request->WriteRanges.push_back(WriteRange);
    request->UnalignedDataParts = std::move(UnalignedDataParts);

    for (const auto& blob: Blobs) {
        request->MergedBlobs.emplace_back(
            blob.BlobId,
            blob.Block,
            blob.BlocksCount);
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TAddDataActor::HandleAddBlobResponse(
    const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TAddDataActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TAddDataActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvAddDataCompleted;
        auto response = std::make_unique<TCompletion>(
            error,
            1,
            BlobsSize,
            ctx.Now() - RequestInfo->StartedTs,
            CommitId);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "AddData");

    if (RequestInfo->Sender != Tablet) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvAddDataResponse>(error);
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s AddData: #%lu completed (%s)",
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

STFUNC(TAddDataActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

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
