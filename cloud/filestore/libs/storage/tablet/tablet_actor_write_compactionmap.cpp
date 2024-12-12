#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleWriteCompactionMap(
    const TEvIndexTablet::TEvWriteCompactionMapRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    TVector<NProtoPrivate::TCompactionRangeStats> ranges(
        Reserve(msg->Record.GetRanges().size()));
    for (const auto& range: msg->Record.GetRanges()) {
        ranges.push_back(range);
    }

    ExecuteTx<TWriteCompactionMap>(
        ctx,
        std::move(requestInfo),
        std::move(ranges));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_WriteCompactionMap(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteCompactionMap& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_WriteCompactionMap(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteCompactionMap& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    for (const auto& range: args.Ranges) {
        db.ForceWriteCompactionMap(
            range.GetRangeId(),
            range.GetBlobCount(),
            range.GetDeletionCount(),
            range.GetGarbageBlockCount());
    }
}

void TIndexTabletActor::CompleteTx_WriteCompactionMap(
    const TActorContext& ctx,
    TTxIndexTablet::TWriteCompactionMap& args)
{
    auto response = std::make_unique<
        TEvIndexTablet::TEvWriteCompactionMapResponse>();
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
