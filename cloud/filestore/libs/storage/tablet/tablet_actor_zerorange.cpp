#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleZeroRange(
    const TEvIndexTabletPrivate::TEvZeroRangeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvIndexTabletPrivate::TEvZeroRangeResponse>(
                std::move(error)));

        return;
    }

    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ZeroRange @%lu %s",
        LogTag.c_str(),
        msg->NodeId,
        msg->Range.Describe().c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    FILESTORE_TRACK(
        BackgroundRequestReceived_Tablet,
        msg->CallContext,
        "ZeroRange");

    AddInFlightRequest<TEvIndexTabletPrivate::TZeroRangeMethod>(*requestInfo);

    ExecuteTx<TZeroRange>(
        ctx,
        std::move(requestInfo),
        msg->NodeId,
        msg->Range);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_ZeroRange(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TZeroRange& args)
{
    Y_UNUSED(tx);

    InitTabletProfileLogRequestInfo(args.ProfileLogRequest, ctx.Now());

    return true;
}

void TIndexTabletActor::ExecuteTx_ZeroRange(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TZeroRange& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    AddRange(
        args.NodeId,
        args.Range.Offset,
        args.Range.Length,
        args.ProfileLogRequest);

    args.Error = ZeroRange(db, args.NodeId, args.CommitId, args.Range);
}

void TIndexTabletActor::CompleteTx_ZeroRange(
    const TActorContext& ctx,
    TTxIndexTablet::TZeroRange& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    // log request
    FinalizeProfileLogRequestInfo(
        std::move(args.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        args.Error,
        ProfileLog);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ZeroRange %lu %s completed: %s",
        LogTag.c_str(),
        args.NodeId,
        args.Range.Describe().c_str(),
        FormatError(args.Error).Quote().c_str());

    auto response =
        std::make_unique<TEvIndexTabletPrivate::TEvZeroRangeResponse>(
            std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    EnqueueCollectGarbageIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
