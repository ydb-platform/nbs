#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDumpCompactionRange(
    const TEvIndexTabletPrivate::TEvDumpCompactionRangeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DumpCompactionRange: rangeId %u",
        LogTag.c_str(),
        msg->RangeId);

    FILESTORE_TRACK(
        BackgroundRequestReceived_Tablet,
        msg->CallContext,
        "DumpCompactionRange");

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    ExecuteTx<TDumpCompactionRange>(
        ctx,
        std::move(requestInfo),
        msg->RangeId);
}

bool TIndexTabletActor::PrepareTx_DumpCompactionRange(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDumpCompactionRange& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    if (!LoadMixedBlocks(db, args.RangeId)) {
        return false;
    }

    args.Blobs = GetBlobsForCompaction(args.RangeId);
    ReleaseMixedBlocks(args.RangeId);

    return true;
}

void TIndexTabletActor::ExecuteTx_DumpCompactionRange(
    const TActorContext&,
    TTransactionContext&,
    TTxIndexTablet::TDumpCompactionRange&)
{}

void TIndexTabletActor::CompleteTx_DumpCompactionRange(
    const TActorContext& ctx,
    TTxIndexTablet::TDumpCompactionRange& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DumpCompactionRange completed: rangeId %u, blobs %lu",
        LogTag.c_str(),
        args.RangeId,
        args.Blobs.size());

    FILESTORE_TRACK(
        ResponseSent_Tablet,
        args.RequestInfo->CallContext,
        "DumpCompactionRange");

    using TResponse = TEvIndexTabletPrivate::TEvDumpCompactionRangeResponse;
    auto response = std::make_unique<TResponse>(
        args.RangeId,
        std::move(args.Blobs));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

void TIndexTabletActor::HandleDumpCompactionRangeCompleted(
    const TEvIndexTabletPrivate::TEvDumpCompactionRangeCompleted::TPtr& ev,
    const TActorContext&)
{
    WorkerActors.erase(ev->Sender);
}

}   // namespace NCloud::NFileStore::NStorage
