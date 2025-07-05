#include "volume_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleUpdateUsedBlocks(
    const TEvVolume::TEvUpdateUsedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto replyFalse = [&ctx, &ev](){
        // should neither cause a non-retriable error nor be retried
        auto response = std::make_unique<TEvVolume::TEvUpdateUsedBlocksResponse>(
            MakeError(S_FALSE, "Used block tracking not set up"));

        NCloud::Reply(ctx, *ev, std::move(response));
    };

    const bool needTrackPersistent = State->GetTrackUsedBlocks();
    const bool needTrackInMemory = State->HasCheckpointLight();

    if (!needTrackPersistent && !needTrackInMemory) {
        return replyFalse();
    }

    auto* msg = ev->Get();
    const auto startIndices = msg->Record.GetStartIndices();
    const auto blockCounts = msg->Record.GetBlockCounts();
    const auto used = msg->Record.GetUsed();

    if (startIndices.size() != blockCounts.size()) {
        auto response = std::make_unique<TEvVolume::TEvUpdateUsedBlocksResponse>(
            MakeError(
                E_ARGUMENT,
                "StartIndices' and BlockCounts' sizes should be equal"
            )
        );

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    TVector<TBlockRange64> ranges;
    for (int i = 0; i < startIndices.size(); ++i) {
        ranges.push_back(TBlockRange64::WithLength(
            startIndices[i],
            blockCounts[i]
        ));
    }

    if (needTrackInMemory) {
        for (const auto& range: ranges) {
            State->MarkBlocksAsDirtyInCheckpointLight(range);
        }
    }

    if (!needTrackPersistent) {
        return replyFalse();
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto& ub = State->AccessUsedBlocks();

    // XXX: possible race when we have 2 inflight writes for the same block:
    // 1. request 1 is received and updates used block map here
    // 2. request 2 is received, sees that there's nothing to update and
    //  completes successfully
    // 3. request 1 update used tx fails => blocks not updated in db
    ui64 c = 0;
    for (const auto& range: ranges) {
        if (used) {
            c += ub.Set(range.Start, range.End + 1);
        } else {
            c += ub.Unset(range.Start, range.End + 1);
        }
    }

    if (c) {
        AddTransaction(*requestInfo);

        ExecuteTx<TUpdateUsedBlocks>(
            ctx,
            std::move(requestInfo),
            std::move(ranges));

        return;
    }

    auto response = std::make_unique<TEvVolume::TEvUpdateUsedBlocksResponse>();
    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareUpdateUsedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateUsedBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateUsedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TUpdateUsedBlocks& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);

    for (const auto& range: args.Ranges) {
        auto serializer = State->GetUsedBlocks().RangeSerializer(
            range.Start,
            range.End + 1);

        TCompressedBitmap::TSerializedChunk sc;
        while (serializer.Next(&sc)) {
            db.WriteUsedBlocks(sc);
        }
    }
}

void TVolumeActor::CompleteUpdateUsedBlocks(
    const TActorContext& ctx,
    TTxVolume::TUpdateUsedBlocks& args)
{
    for (const auto& range: args.Ranges) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s UpdateUsedBlocks done %lu %u",
            LogTitle.GetWithTime().c_str(),
            range.Start,
            range.Size());
    }

    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvVolume::TEvUpdateUsedBlocksResponse>();
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
