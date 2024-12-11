#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void Crop(TVector<T>& data, TVector<T>& rem, ui64 maxItems)
{
    if (data.size() > maxItems) {
        rem.resize(data.size() - maxItems);
        std::copy(data.begin() + maxItems, data.end(), rem.begin());
        data.resize(maxItems);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDeleteGarbage(
    const TEvIndexTabletPrivate::TEvDeleteGarbageRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DeleteGarbage started (collect: %lu, new: %lu, garbage: %lu)",
        LogTag.c_str(),
        msg->CollectCommitId,
        msg->NewBlobs.size(),
        msg->GarbageBlobs.size());

    FILESTORE_TRACK(
        BackgroundTaskStarted_Tablet,
        msg->CallContext,
        "DeleteGarbage",
        msg->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    ExecuteTx<TDeleteGarbage>(
        ctx,
        std::move(requestInfo),
        msg->CollectCommitId,
        std::move(msg->NewBlobs),
        std::move(msg->GarbageBlobs));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DeleteGarbage(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteGarbage& args)
{
    Y_UNUSED(tx);

    InitProfileLogRequestInfo(args.ProfileLogRequest, ctx.Now());

    return true;
}

void TIndexTabletActor::ExecuteTx_DeleteGarbage(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteGarbage& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    const ui32 maxBlobsPerTx = Config->GetMaxDeleteGarbageBlobsPerTx();
    Crop(args.NewBlobs, args.RemainingNewBlobs, maxBlobsPerTx);
    Crop(args.GarbageBlobs, args.RemainingGarbageBlobs, maxBlobsPerTx);

    AddRange(
        args.CollectCommitId,
        args.NewBlobs.size(),
        args.GarbageBlobs.size(),
        args.ProfileLogRequest);

    DeleteGarbage(
        db,
        args.CollectCommitId,
        args.NewBlobs,
        args.GarbageBlobs);
}

void TIndexTabletActor::CompleteTx_DeleteGarbage(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteGarbage& args)
{
    // log request
    FinalizeProfileLogRequestInfo(
        std::move(args.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        {},
        ProfileLog);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DeleteGarbage completed",
        LogTag.c_str());

    FILESTORE_TRACK(
        ResponseSent_Tablet,
        args.RequestInfo->CallContext,
        "DeleteGarbage");

    if (args.RemainingNewBlobs || args.RemainingGarbageBlobs) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s Garbage queue too big, splitting into multiple DeleteGarbage"
            " txs (next: collect: %lu, new: %u, garbage: %u)",
            LogTag.c_str(),
            args.CollectCommitId,
            args.RemainingNewBlobs.size(),
            args.RemainingGarbageBlobs.size());

        ExecuteTx<TDeleteGarbage>(
            ctx,
            args.RequestInfo,
            args.CollectCommitId,
            std::move(args.RemainingNewBlobs),
            std::move(args.RemainingGarbageBlobs));
    } else {
        auto response =
            std::make_unique<TEvIndexTabletPrivate::TEvDeleteGarbageResponse>();
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }
}

}   // namespace NCloud::NFileStore::NStorage
