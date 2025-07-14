#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_loadfreshblobs.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

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
        State->GetMeta().GetTrimFreshLogToCommitId(),
        std::move(freshChannels));

    Actors.Insert(actor);
}

void TPartitionActor::HandleLoadFreshBlobsCompleted(
    const TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReportInitFreshBlocksError(
            TStringBuilder()
            << LogTitle.GetWithTime() << " LoadFreshBlobs failed. error: "
            << FormatError(msg->GetError()));
        Suicide(ctx);
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Loaded fresh blobs",
        LogTitle.GetWithTime().c_str());

    Actors.Erase(ev->Sender);

    TVector<TOwningFreshBlock> blocks;
    for (const auto& blob: msg->Blobs) {
        auto error = ParseFreshBlobContent(
            blob.CommitId,
            State->GetBlockSize(),
            blob.Data,
            blocks);

        if (FAILED(error.GetCode())) {
            ReportInitFreshBlocksError(
                TStringBuilder()
                << LogTitle.GetWithTime()
                << " Failed to parse fresh blob (blob commitId: "
                << blob.CommitId << "): " << FormatError(error));
            Suicide(ctx);
            return;
        }

        State->AddFreshBlob({blob.CommitId, blob.Data.size()});
        State->IncrementUnflushedFreshBlobByteCount(blob.Data.size());
    }

    for (const auto& block: blocks) {
        State->GetTrimFreshLogBarriers().AcquireBarrier(block.Meta.CommitId);
    }

    State->InitFreshBlocks(blocks);
    State->IncrementUnflushedFreshBlobCount(msg->Blobs.size());
    State->IncrementUnflushedFreshBlocksFromChannelCount(blocks.size());

    // TODO(NBS-1976): update used blocks map

    FreshBlobsLoaded(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
