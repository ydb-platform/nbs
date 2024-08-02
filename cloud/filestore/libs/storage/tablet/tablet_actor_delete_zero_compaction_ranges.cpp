#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDeleteZeroCompactionRanges(
    const TEvIndexTabletPrivate::TEvDeleteZeroCompactionRangesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TDeleteZeroCompactionRanges>(
        ctx,
        std::move(requestInfo),
        msg->RangeId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DeleteZeroCompactionRanges(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteZeroCompactionRanges& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_DeleteZeroCompactionRanges(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteZeroCompactionRanges& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    TVector<ui32> ranges(
        Reserve(Config->GetMaxZeroCompactionRangesToDeletePerTx()));
    ui32 rangesCount = RangesWithEmptyCompactionScore.size();
    ui32 rangesPerTx = Config->GetMaxZeroCompactionRangesToDeletePerTx();
    for (ui32 i = args.StartIndex;
            i < Min<ui32>(args.StartIndex + rangesPerTx, rangesCount); i++)
    {
        ui32 range = RangesWithEmptyCompactionScore[i];
        db.WriteCompactionMap(
            range,
            GetCompactionStats(range).BlobsCount,
            GetCompactionStats(range).DeletionsCount);
    }

}

void TIndexTabletActor::CompleteTx_DeleteZeroCompactionRanges(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteZeroCompactionRanges& args)
{
    auto response = std::make_unique<
        TEvIndexTabletPrivate::TEvDeleteZeroCompactionRangesResponse>();
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
