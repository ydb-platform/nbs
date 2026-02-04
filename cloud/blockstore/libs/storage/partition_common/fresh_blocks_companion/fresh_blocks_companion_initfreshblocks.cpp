#include "fresh_blocks_companion.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_loadfreshblobs.h>

#include <cloud/storage/core/libs/actors/helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TFreshBlocksCompanion::LoadFreshBlobs(
    const TActorContext& ctx,
    ui64 persistedTrimFreshLogToCommitId)
{
    auto freshChannels = ChannelsState.GetChannelsByKind(
        [](auto kind) { return kind == EChannelDataKind::Fresh; });

    auto actor = NCloud::Register<TLoadFreshBlobsActor>(
        ctx,
        ctx.SelfID,
        Info(),
        StorageAccessMode,
        persistedTrimFreshLogToCommitId,
        std::move(freshChannels));

    Actors.Insert(actor);
}

void TFreshBlocksCompanion::HandleLoadFreshBlobsCompleted(
    const TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReportInitFreshBlocksError(
            TStringBuilder() << "LoadFreshBlobs failed, error: "
                             << FormatError(msg->GetError()),
            {{"disk", PartitionConfig.GetDiskId()}});
        Client.Suicide(ctx);
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Loaded fresh blobs",
        LogTitle.GetWithTime().c_str());

    Actors.Erase(ev->Sender);

    TVector<NPartition::TOwningFreshBlock> blocks;
    for (const auto& blob: msg->Blobs) {
        auto error = ParseFreshBlobContent(
            blob.CommitId,
            PartitionConfig.GetBlockSize(),
            blob.Data,
            blocks);

        if (FAILED(error.GetCode())) {
            ReportInitFreshBlocksError(
                TStringBuilder() << "Failed to parse fresh blob error: "
                                 << FormatError(error),
                {{"disk", PartitionConfig.GetDiskId()},
                 {"commit_id", blob.CommitId}});
            Client.Suicide(ctx);
            return;
        }

        FreshBlobState.AddFreshBlob({blob.CommitId, blob.Data.size()});
        FlushState.IncrementUnflushedFreshBlobByteCount(blob.Data.size());
    }

    for (const auto& block: blocks) {
        TrimFreshLogState.AccessTrimFreshLogBarriers().AcquireBarrier(
            block.Meta.CommitId);
    }

    FreshBlocksState.InitFreshBlocks(blocks);
    FlushState.IncrementUnflushedFreshBlobCount(msg->Blobs.size());
    FreshBlocksState.IncrementUnflushedFreshBlocksFromChannelCount(
        blocks.size());

    // TODO(NBS-1976): update used blocks map

    Client.FreshBlobsLoaded(ctx);
}

}  // namespace NCloud::NBlockStore::NStorage
