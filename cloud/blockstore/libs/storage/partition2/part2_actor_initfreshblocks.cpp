#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/partition2/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_loadfreshblobs.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TInitFreshZonesActor final
    : public TActorBootstrapped<TInitFreshZonesActor>
{
private:
    const TActorId PartitionActorId;
    TVector<TBlockRange32> BlockRanges;

public:
    TInitFreshZonesActor(
        const TActorId& partitionActorId,
        TVector<TBlockRange32> blockRanges);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleInitIndexResponse(
        const TEvPartitionPrivate::TEvInitIndexResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TInitFreshZonesActor::TInitFreshZonesActor(
        const TActorId& partitionActorId,
        TVector<TBlockRange32> blockRanges)
    : PartitionActorId(partitionActorId)
    , BlockRanges(std::move(blockRanges))
{}

void TInitFreshZonesActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    using TRequest = TEvPartitionPrivate::TEvInitIndexRequest;
    auto request = std::make_unique<TRequest>(std::move(BlockRanges));

    NCloud::Send(ctx, PartitionActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TInitFreshZonesActor::HandleInitIndexResponse(
    const TEvPartitionPrivate::TEvInitIndexResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    NCloud::Send<TEvPartitionPrivate::TEvInitFreshZonesCompleted>(
        ctx,
        PartitionActorId);

    Die(ctx);
}

void TInitFreshZonesActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

STFUNC(TInitFreshZonesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvPartitionPrivate::TEvInitIndexResponse, HandleInitIndexResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::LoadFreshBlobs(const TActorContext& ctx)
{
    auto freshChannels = State->GetChannelsByKind([](auto kind) {
        return kind == EChannelDataKind::Fresh;
    });

    auto actor = NCloud::Register<TLoadFreshBlobsActor>(
        ctx,
        SelfId(),
        Info(),
        StorageAccessMode,
        0,  // trimFreshLogToCommitId
        std::move(freshChannels));

    Actors.insert(actor);
}

void TPartitionActor::HandleLoadFreshBlobsCompleted(
    const TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " LoadFreshBlobs failed: " << msg->GetStatus()
            << " reason: " << msg->GetError().GetMessage().Quote());

        ReportInitFreshBlocksError(
            TStringBuilder()
            << "[" << TabletID()
            << "] LoadFreshBlobs failed: " << msg->GetStatus());
        Suicide(ctx);
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Loaded fresh blobs",
        TabletID());

    Actors.erase(ev->Sender);

    TVector<TOwningFreshBlock> blocks;
    TBlobUpdatesByFresh updates;

    for (const auto& blob: msg->Blobs) {
        auto error = ParseFreshBlobContent(
            blob.CommitId,
            State->GetBlockSize(),
            blob.Data,
            blocks,
            updates);

        if (FAILED(error.GetCode())) {
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "]"
                << " Failed to parse fresh blob "
                << "(blob commitId: " << blob.CommitId << "): "
                << error.GetMessage());

            ReportInitFreshBlocksError(
                TStringBuilder()
                << "[" << TabletID() << "] Failed to parse fresh blob "
                << "(blob commitId: " << blob.CommitId << ")");
            Suicide(ctx);
            return;
        }
    }

    Sort(blocks, [] (const auto& lhs, const auto& rhs) {
        return std::make_pair(lhs.Meta.BlockIndex, lhs.Meta.MinCommitId)
            <  std::make_pair(rhs.Meta.BlockIndex, rhs.Meta.MinCommitId);
    });

    State->InitFreshBlocks(blocks);
    State->ApplyFreshBlockUpdates();

    TVector<TBlockRange32> blockRanges(Reserve(updates.size()));
    for (const auto& update: updates) {
        blockRanges.push_back(update.BlockRange);
    }

    State->SetBlobUpdatesByFresh(std::move(updates));

    auto actor = NCloud::Register<TInitFreshZonesActor>(
        ctx,
        SelfId(),
        std::move(blockRanges));

    Actors.insert(actor);
}

void TPartitionActor::HandleInitFreshZonesCompleted(
    const TEvPartitionPrivate::TEvInitFreshZonesCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Actors.erase(ev->Sender);

    State->ApplyBlobUpdatesByFresh();

    Activate(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
