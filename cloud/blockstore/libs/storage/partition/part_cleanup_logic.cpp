#include "part_cleanup_logic.h"

#include "part_database.h"
#include "part_state.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <util/generic/algorithm.h>

#include <ranges>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

void DecrementBlobCounters(
    TPartitionState& state,
    const TPartialBlobId& blobId,
    EChannelDataKind indexKind,
    ui64 blocksCount)
{
    // Deletion markers contribute to blob totals, but not block totals.
    if (IsDeletionMarker(blobId)) {
        blocksCount = 0;
    }

    switch (indexKind) {
        case EChannelDataKind::Mixed:
            state.DecrementMixedIndexBlobsCount(
                Min<ui64>(1, state.GetMixedIndexBlobsCount()));
            state.DecrementMixedIndexBlocksCount(
                Min(blocksCount, state.GetMixedIndexBlocksCount()));
            break;
        case EChannelDataKind::Merged:
            state.DecrementMergedIndexBlobsCount(
                Min<ui64>(1, state.GetMergedIndexBlobsCount()));
            state.DecrementMergedIndexBlocksCount(
                Min(blocksCount, state.GetMergedIndexBlocksCount()));
            break;
        default:
            Y_ABORT("Unexpected index kind: %u", static_cast<ui32>(indexKind));
    }

    // Deletion markers do not have a data channel.
    if (IsDeletionMarker(blobId) &&
        state.ShouldUseBlobChannelDataKindForCounters())
    {
        return;
    }

    // If channel-based counters are enabled, use the channel data kind
    // otherwise use the index kind
    const auto countersKind = state.ShouldUseBlobChannelDataKindForCounters()
                                  ? state.GetChannelDataKind(blobId.Channel())
                                  : indexKind;
    switch (countersKind) {
        case EChannelDataKind::Mixed:
            state.DecrementMixedBlobsCount(
                Min<ui64>(1, state.GetMixedBlobsCount()));
            state.DecrementMixedBlocksCount(
                Min(blocksCount, state.GetMixedBlocksCount()));
            break;
        case EChannelDataKind::Merged:
            state.DecrementMergedBlobsCount(
                Min<ui64>(1, state.GetMergedBlobsCount()));
            state.DecrementMergedBlocksCount(
                Min(blocksCount, state.GetMergedBlocksCount()));
            break;
        default:
            Y_ABORT(
                "Unexpected blob channel data kind: %u",
                static_cast<ui32>(countersKind));
    }
}

////////////////////////////////////////////////////////////////////////////////

TVerifyBlocksMetaResult VerifyMixedBlocksMeta(
    TPartitionDatabase& db,
    TPartialBlobId originalBlobId,
    const NProto::TBlobMeta::TMixedBlocks& originalMixedBlocks,
    const NProto::TBlobMeta::TMixedBlocks& recreatedMixedBlocks)
{
    // Check that blocks from recreated blob meta are present in the original
    // blob meta and that their commit ids are the same.
    // Some blocks may be missing in recreated blob meta because we delete some
    // blocks from mixed index on compaction.

    auto getCommitId = [&](const NProto::TBlobMeta::TMixedBlocks& mixedBlocks,
                           size_t i) -> ui64
    {
        return i < mixedBlocks.CommitIdsSize() ? mixedBlocks.GetCommitIds(i)
                                               : originalBlobId.CommitId();
    };

    TVector<TBlock> originalBlocks;
    for (size_t i = 0; i < originalMixedBlocks.BlocksSize(); ++i) {
        originalBlocks.emplace_back(
            originalMixedBlocks.GetBlocks(i),
            getCommitId(originalMixedBlocks, i),
            false);
    }

    TVector<TBlock> recreatedBlocks;
    for (size_t i = 0; i < recreatedMixedBlocks.BlocksSize(); ++i) {
        recreatedBlocks.emplace_back(
            recreatedMixedBlocks.GetBlocks(i),
            getCommitId(recreatedMixedBlocks, i),
            false);
    }

    auto cmp = [](const TBlock& l, const TBlock& r)
    {
        if (l.BlockIndex == r.BlockIndex) {
            return l.CommitId < r.CommitId;
        }
        return l.BlockIndex < r.BlockIndex;
    };

    Sort(originalBlocks, cmp);
    Sort(recreatedBlocks, cmp);

    TVector<TBlock> missedBlocks;

    size_t recreatedIdx = 0, originalIdx = 0;
    while (recreatedIdx < recreatedBlocks.size() &&
           originalIdx < originalBlocks.size())
    {
        if (recreatedBlocks[recreatedIdx] != originalBlocks[originalIdx]) {
            missedBlocks.emplace_back(originalBlocks[originalIdx]);

            ++originalIdx;
            continue;
        }

        ++recreatedIdx;
        ++originalIdx;
    }

    for (; originalIdx < originalBlocks.size(); ++originalIdx) {
        missedBlocks.emplace_back(originalBlocks[originalIdx]);
    }

    if (recreatedIdx < recreatedBlocks.size()) {
        auto error = MakeError(
            E_ARGUMENT,
            "there are blocks that are not present in the original blob "
            "meta");
        return {.Error = std::move(error)};
    }

    struct TVisitor final: public IMixedBlocksIndexVisitor
    {
        TPartialBlobId OriginalBlobId;
        TVector<TBlock> LeakedBlocks;

        TVisitor(TPartialBlobId originalBlobId)
            : OriginalBlobId(originalBlobId)
        {}

        bool VisitBlock(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset,
            ui8 compactionRangeCount) override
        {
            Y_UNUSED(blobOffset);
            Y_UNUSED(compactionRangeCount);

            // The same block may be present in multiple blobs, so we need to
            // check if the block is present in the original blob.
            // This can happen if the AddBlobs transaction for a flush has
            // already been committed when the tablet restarts, but the fresh
            // blobs have not yet been trimmed. After the restart, we load the
            // same fresh blobs and try to flush them again.
            if (blobId != OriginalBlobId) {
                return true;
            }

            LeakedBlocks.emplace_back(blockIndex, commitId, false);

            return true;
        }
    };

    TVisitor visitor{originalBlobId};
    bool ready = db.FindMixedBlocks(visitor, missedBlocks);
    if (!ready) {
        return {.Ready = false};
    }

    if (visitor.LeakedBlocks) {
        TStringBuilder sb;
        sb << "Leaked blocks in recreated blob meta: ";
        for (size_t i = 0; i < visitor.LeakedBlocks.size(); ++i) {
            sb << "{ BlockIndex: " << visitor.LeakedBlocks[i].BlockIndex
               << ", CommitId: " << visitor.LeakedBlocks[i].CommitId << " } ";
        }
        auto error = MakeError(E_ARGUMENT, std::move(sb));
        return {.Error = std::move(error)};
    }

    return {};
}

TVerifyBlocksMetaResult VerifyMergedBlocksMeta(
    const NProto::TBlobMeta::TMergedBlocks& originalMergedBlocks,
    const NProto::TBlobMeta::TMergedBlocks& recreatedMergedBlocks)
{
    bool ok =
        originalMergedBlocks.GetStart() == recreatedMergedBlocks.GetStart() &&
        originalMergedBlocks.GetEnd() == recreatedMergedBlocks.GetEnd() &&
        originalMergedBlocks.GetSkipped() == recreatedMergedBlocks.GetSkipped();

    if (!ok) {
        auto error = MakeError(E_ARGUMENT, "Mismatched merged blocks");
        return {.Error = std::move(error)};
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVerifyBlocksMetaResult VerifyRecreatedBlobMeta(
    TPartitionDatabase& db,
    TPartialBlobId originalBlobId,
    const NProto::TBlobMeta& blobMeta,
    const NProto::TBlobMeta& recreatedBlobMeta)
{
    if (blobMeta.HasMixedBlocks() != recreatedBlobMeta.HasMixedBlocks() ||
        blobMeta.HasMergedBlocks() != recreatedBlobMeta.HasMergedBlocks())
    {
        auto error = MakeError(E_ARGUMENT, "Mismatched blob meta types");
        return {.Error = std::move(error)};
    }

    if (blobMeta.HasMixedBlocks()) {
        return VerifyMixedBlocksMeta(
            db,
            originalBlobId,
            blobMeta.GetMixedBlocks(),
            recreatedBlobMeta.GetMixedBlocks());
    }

    if (blobMeta.HasMergedBlocks()) {
        return VerifyMergedBlocksMeta(
            blobMeta.GetMergedBlocks(),
            recreatedBlobMeta.GetMergedBlocks());
    }

    return {};
}

bool PrepareCleanupTransaction(
    const ui64 tabletId,
    const TString& diskId,
    TPartitionDatabase& db,
    TTxPartition::TCleanup& args)
{
    TRequestScope timer(*args.RequestInfo);

    THashSet<TPartialBlobId, TPartialBlobIdHash> blobIdsToRemoveFromQueue;
    THashMap<TPartialBlobId, NProto::TBlobMeta, TPartialBlobIdHash> blobMetas;

    bool ready = true;

    for (const auto& item: args.CleanupQueue) {
        const auto& recreatedBlobMeta = item.AdditionalFields.GetBlobMeta();
        // no need to read blob meta for blobs with already known blocks
        const bool hasValidMetaInCleanupQueue =
            recreatedBlobMeta.HasMixedBlocks() ||
            recreatedBlobMeta.HasMergedBlocks();
        if (hasValidMetaInCleanupQueue && args.UseRecreatedBlobMeta) {
            blobMetas[item.BlobId] = recreatedBlobMeta;
            continue;
        }

        TMaybe<NProto::TBlobMeta> blobMeta;
        ++args.ReadBlobMetasCount;
        if (db.ReadBlobMeta(item.BlobId, blobMeta)) {
            Y_ABORT_UNLESS(
                blobMeta.Defined(),
                "Could not read meta data for blob: %s",
                ToString(MakeBlobId(tabletId, item.BlobId)).data());
            auto& meta = blobMetas[item.BlobId];
            meta = std::move(blobMeta.GetRef());

            const bool needToVerify = args.VerifyRecreatedBlobMetasOnCleanup &&
                                      hasValidMetaInCleanupQueue;
            if (!needToVerify) {
                continue;
            }

            auto verifyResult = VerifyRecreatedBlobMeta(
                db,
                item.BlobId,
                meta,
                recreatedBlobMeta);
            if (!verifyResult.Ready) {
                ready = false;
                continue;
            }

            if (HasError(verifyResult.Error)) {
                blobIdsToRemoveFromQueue.insert(item.BlobId);
                ReportCleanupBlobMetaBlocksMismatch(
                    {{"diskId", diskId},
                     {"tabletId", tabletId},
                     {"blobId", ToString(MakeBlobId(tabletId, item.BlobId))},
                     {"recreatedBlobMeta",
                      recreatedBlobMeta.ShortUtf8DebugString()},
                     {"originalBlobMeta", meta.ShortUtf8DebugString()},
                     {"error", FormatError(verifyResult.Error)}});
            }
        } else {
            ready = false;
        }
    }

    if (ready) {
        // we not cleanup blobs with mismatched blocks meta intentionally
        // because it can help to investigate the issue
        auto itemsToRemove = std::ranges::remove_if(
            args.CleanupQueue,
            [&](const auto& item) -> bool
            { return blobIdsToRemoveFromQueue.contains(item.BlobId); });
        args.CleanupQueue.erase(itemsToRemove.begin(), itemsToRemove.end());

        for (const auto& item: args.CleanupQueue) {
            auto* blobMeta = blobMetas.FindPtr(item.BlobId);

            STORAGE_VERIFY_C(
                blobMeta,
                TWellKnownEntityTypes::TABLET,
                tabletId,
                "Blob meta not found for blob "
                    << MakeBlobId(tabletId, item.BlobId));

            args.BlobsMeta.push_back(std::move(*blobMeta));
        }
    }

    return ready;
}

namespace {

////////////////////////////////////////////////////////////////////////////////

bool ShouldSkipCleanupDueToCheckpoint(
    const TCleanupQueueItem& item,
    const NProto::TBlobMeta& blobMeta,
    ui64 minCheckpointCommitId,
    ui64 maxCheckpointCommitId)
{
    if (item.CommitId < minCheckpointCommitId) {
        // The blob was added to the cleanup queue before any checkpoint.
        // Therefore, it is covered by one or more blobs visible to all
        // checkpoints.
        return false;
    }

    ui64 minCommitIdInBlob = Max<ui64>();
    if (blobMeta.HasMixedBlocks()) {
        const auto& mixedBlocks = blobMeta.GetMixedBlocks();
        if (mixedBlocks.CommitIdsSize() == 0) {
            // Every block shares the same commitId.
            minCommitIdInBlob = item.BlobId.CommitId();
        } else {
            // Each block has its own commitId.
            for (ui64 commitId: mixedBlocks.GetCommitIds()) {
                minCommitIdInBlob = Min(minCommitIdInBlob, commitId);
            }
        }
    } else if (blobMeta.HasMergedBlocks()) {
        minCommitIdInBlob = item.BlobId.CommitId();
    }

    return minCommitIdInBlob <= maxCheckpointCommitId;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ExecuteCleanupTransaction(
    const NActors::TActorSystem* actorSystem,
    const TLogTitle& logTitle,
    const ui64 tabletId,
    TPartitionDatabase& db,
    TTxPartition::TCleanup& args,
    TPartitionState& state)
{
    TRequestScope timer(*args.RequestInfo);

    TVector<TCleanupQueueItem> filteredCleanupQueue;
    filteredCleanupQueue.reserve(args.CleanupQueue.size());

    Y_ABORT_UNLESS(args.CleanupQueue.size() == args.BlobsMeta.size());
    for (size_t i = 0; i < args.CleanupQueue.size(); ++i) {
        const auto& item = args.CleanupQueue[i];
        const auto& blobMeta = args.BlobsMeta[i];

        if (args.CheckpointAware && ShouldSkipCleanupDueToCheckpoint(
                                       item,
                                       blobMeta,
                                       args.MinCheckpointCommitId,
                                       args.MaxCheckpointCommitId))
        {
            LOG_DEBUG(
                *actorSystem,
                TBlockStoreComponents::PARTITION,
                "%s ExecuteCleanupTransaction: skipping blob=%s: "
                "deletionCommitId=%lu "
                "minCheckpointCommitId=%lu maxCheckpointCommitId=%lu",
                logTitle.GetWithTime().c_str(),
                ToString(MakeBlobId(tabletId, item.BlobId)).Quote().c_str(),
                item.CommitId,
                args.MinCheckpointCommitId,
                args.MaxCheckpointCommitId);
            ++args.BlobsSkipped;
            continue;
        }

        EChannelDataKind indexKind = EChannelDataKind::Max;
        ui64 blockCountInBlob = 0;
        if (blobMeta.HasMixedBlocks()) {
            const auto& mixedBlocks = blobMeta.GetMixedBlocks();

            if (mixedBlocks.CommitIdsSize() == 0) {
                // every block shares the same commitId
                ui64 commitId = item.BlobId.CommitId();
                for (ui32 blockIndex: mixedBlocks.GetBlocks()) {
                    state.DeleteMixedBlock(db, blockIndex, commitId);
                }
            } else {
                // each block has its own commitId
                Y_ABORT_UNLESS(
                    mixedBlocks.BlocksSize() == mixedBlocks.CommitIdsSize());
                for (size_t j = 0; j < mixedBlocks.BlocksSize(); ++j) {
                    ui32 blockIndex = mixedBlocks.GetBlocks(j);
                    ui64 commitId = mixedBlocks.GetCommitIds(j);
                    state.DeleteMixedBlock(db, blockIndex, commitId);
                }
            }

            if (args.UseRecreatedBlobMeta) {
                STORAGE_VERIFY_C(
                    state.GetBlockSize() > 0 &&
                        item.BlobId.BlobSize() % state.GetBlockSize() == 0,
                    TWellKnownEntityTypes::TABLET,
                    state.GetConfig().GetDiskId(),
                    "Blob size is not divisible by block size, blob: "
                        << ToString(MakeBlobId(tabletId, item.BlobId)));
                blockCountInBlob =
                    item.BlobId.BlobSize() / state.GetBlockSize();
            } else {
                blockCountInBlob = mixedBlocks.BlocksSize();
            }
            indexKind = EChannelDataKind::Mixed;
        } else if (blobMeta.HasMergedBlocks()) {
            const auto& mergedBlocks = blobMeta.GetMergedBlocks();

            auto blockRange = TBlockRange32::MakeClosedInterval(
                mergedBlocks.GetStart(),
                mergedBlocks.GetEnd());
            db.DeleteMergedBlocks(item.BlobId, blockRange);

            indexKind = EChannelDataKind::Merged;
            blockCountInBlob = blockRange.Size() - mergedBlocks.GetSkipped();
        }

        DecrementBlobCounters(state, item.BlobId, indexKind, blockCountInBlob);

        LOG_DEBUG(
            *actorSystem,
            TBlockStoreComponents::PARTITION,
            "%s Delete blob: %s",
            logTitle.GetWithTime().c_str(),
            ToString(MakeBlobId(tabletId, item.BlobId)).Quote().c_str());

        state.RemoveCleanupQueueItem(item);

        db.DeleteBlobMeta(item.BlobId);
        db.DeleteCleanupQueue(item.BlobId, item.CommitId);

        if (!IsDeletionMarker(item.BlobId)) {
            db.WriteGarbageBlob(item.BlobId);
        }

        filteredCleanupQueue.push_back(item);
    }

    if (args.CheckpointAware) {
        if (!args.CleanupQueue.empty()) {
            state.UpdateCleanupMilestoneIfNeeded(
                args.CleanupQueue.back().CommitId,
                args.CleanupQueue.back().BlobId,
                args.MinCheckpointCommitId,
                args.MaxCheckpointCommitId);
        }

        args.CleanupQueue = std::move(filteredCleanupQueue);
    } else {
        Y_DEBUG_ABORT_UNLESS(
            filteredCleanupQueue.size() == args.CleanupQueue.size());
    }

    db.WriteMeta(state.GetMeta());
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
