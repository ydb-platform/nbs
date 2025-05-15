#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/tablet/model/group_by.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTruncateWorker
    : public TActorBootstrapped<TTruncateWorker>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;

    const ui64 NodeId;
    const TByteRange TotalRange;

    const ui64 Stripe;
    const ui32 MaxInflight;

    ui32 Inflight = 0;
    ui64 LastOffset = 0;

public:
    TTruncateWorker(
            TString logTag,
            TActorId tablet,
            TRequestInfoPtr requestInfo,
            ui64 nodeId,
            TByteRange range,
            ui64 maxBlocks,
            ui32 inflight)
        : LogTag(std::move(logTag))
        , Tablet(tablet)
        , RequestInfo(std::move(requestInfo))
        , NodeId(nodeId)
        , TotalRange(range)
        , Stripe(maxBlocks * range.BlockSize)
        , MaxInflight(inflight)
        , LastOffset(TotalRange.Offset)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        ScheduleRequests(ctx);
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvIndexTabletPrivate::TEvTruncateRangeResponse, HandleTruncate);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::TABLET_WORKER,
                    __PRETTY_FUNCTION__);
        }
    }

    void ScheduleRequests(const TActorContext& ctx)
    {
        const ui64 end = TotalRange.End();
        while (LastOffset < end && Inflight < MaxInflight) {
            ui64 next = Min(end, LastOffset + Stripe);
            if (next != end) {
                next = AlignDown<ui64>(next, TotalRange.BlockSize);
            }

            TABLET_VERIFY(next > LastOffset);
            TByteRange range(LastOffset, next - LastOffset, TotalRange.BlockSize);
            auto request = std::make_unique<TEvIndexTabletPrivate::TEvTruncateRangeRequest>(
                NodeId,
                range);

            NCloud::Send(ctx, Tablet, std::move(request));

            LastOffset = next;
            ++Inflight;
        }
    }

    void HandleTruncate(
        const TEvIndexTabletPrivate::TEvTruncateRangeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        TABLET_VERIFY(Inflight);
        --Inflight;

        ScheduleRequests(ctx);
        if (Inflight) {
            return;
        }

        ReplyAndDie(ctx);
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx)
    {
        {
            // notify tablet
            auto response =
                std::make_unique<TEvIndexTabletPrivate::TEvTruncateCompleted>();
            response->NodeId = NodeId;

            NCloud::Send(ctx, Tablet, std::move(response));
        }

        if (RequestInfo->Sender != Tablet) {
            // reply to caller
            auto response =
                std::make_unique<TEvIndexTabletPrivate::TEvTruncateResponse>();
            response->NodeId = NodeId;

            NCloud::Reply(ctx, *RequestInfo, std::move(response));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::EnqueueTruncateIfNeeded(const NActors::TActorContext& ctx)
{
    while (HasPendingTruncateOps()) {
        auto [nodeId, range] = DequeueTruncateOp();
        using TRequest = TEvIndexTabletPrivate::TEvTruncateRequest;
        auto request = std::make_unique<TRequest>(nodeId, range);
        NCloud::Send(ctx, ctx.SelfID, std::move(request));
    }
}

void TIndexTabletActor::HandleTruncate(
    const TEvIndexTabletPrivate::TEvTruncateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvIndexTabletPrivate::TEvTruncateResponse>(
                std::move(error)));

        return;
    }

    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Truncate @%lu %s",
        LogTag.c_str(),
        msg->NodeId,
        msg->Range.Describe().c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    FILESTORE_TRACK(
        BackgroundTaskStarted_Tablet,
        msg->CallContext,
        "Truncate",
        msg->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    auto actor = std::make_unique<TTruncateWorker>(
        LogTag,
        ctx.SelfID,
        std::move(requestInfo),
        msg->NodeId,
        msg->Range,
        Config->GetMaxBlocksPerTruncateTx(),
        Config->GetMaxTruncateTxInflight());

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleTruncateCompleted(
    const TEvIndexTabletPrivate::TEvTruncateCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    WorkerActors.erase(ev->Sender);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>(GetFileSystemId()));
    requestInfo->StartedTs = ctx.Now();

    ExecuteTx<TTruncateCompleted>(
        ctx,
        std::move(requestInfo),
        msg->NodeId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_TruncateCompleted(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TTruncateCompleted& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_TruncateCompleted(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TTruncateCompleted& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    DeleteTruncate(db, args.NodeId);
}

void TIndexTabletActor::CompleteTx_TruncateCompleted(
    const TActorContext& ctx,
    TTxIndexTablet::TTruncateCompleted& args)
{
    Y_UNUSED(ctx);
    // Now it's only safe to remove barrier only when tx is commited or we
    // could let writes but tear them down right after tablet restart
    CompleteTruncateOp(args.NodeId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleTruncateRange(
    const TEvIndexTabletPrivate::TEvTruncateRangeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s TruncateRange @%lu %s",
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
        "TruncateRange");

    ExecuteTx<TTruncateRange>(
        ctx,
        std::move(requestInfo),
        msg->NodeId,
        msg->Range);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_TruncateRange(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TTruncateRange& args)
{
    Y_UNUSED(tx);

    InitProfileLogRequestInfo(args.ProfileLogRequest, ctx.Now());

    return true;
}

void TIndexTabletActor::ExecuteTx_TruncateRange(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TTruncateRange& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    ui64 commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "TruncateRange");
    }

    AddRange(
        args.NodeId,
        args.Range.Offset,
        args.Range.Length,
        args.ProfileLogRequest);

    args.Error = TruncateRange(db, args.NodeId, commitId, args.Range);
}

void TIndexTabletActor::CompleteTx_TruncateRange(
    const TActorContext& ctx,
    TTxIndexTablet::TTruncateRange& args)
{
    // log request
    FinalizeProfileLogRequestInfo(
        std::move(args.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        args.Error,
        ProfileLog);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s TruncateRange %lu %s completed",
        LogTag.c_str(),
        args.NodeId,
        args.Range.Describe().c_str());

    auto response =
        std::make_unique<TEvIndexTabletPrivate::TEvTruncateRangeResponse>(
            std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    EnqueueBlobIndexOpIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
