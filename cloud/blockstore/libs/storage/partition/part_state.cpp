#include "part_state.h"

#include <cloud/storage/core/libs/tablet/model/channels.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using TJsonValue = NJson::TJsonValue;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T SafeIncrement(T counter, size_t value)
{
    Y_ABORT_UNLESS(counter < Max<T>() - value);
    return counter + value;
}

template <typename T>
T SafeDecrement(T counter, size_t value)
{
    Y_ABORT_UNLESS(counter >= value);
    return counter - value;
}

////////////////////////////////////////////////////////////////////////////////

double BPFeature(const TBackpressureFeatureConfig& c, double x)
{
    double nx = Normalize(x, c.InputThreshold, c.InputLimit);
    return (1 - nx) + nx * c.MaxValue;
}

// Scales a threshold defined per allocation unit to the whole disk.
ui64 CalculatePerDiskThreshold(
    ui64 blocksCount,
    ui32 blockSize,
    ui64 allocationUnit,
    ui64 perUnitThreshold)
{
    const ui64 allocationUnitBlocks = Max<ui64>(1, allocationUnit / blockSize);
    const ui64 whole = blocksCount / allocationUnitBlocks;
    const ui64 remainder = blocksCount % allocationUnitBlocks;

    return whole * perUnitThreshold +
           CeilDiv(remainder * perUnitThreshold, allocationUnitBlocks);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPartitionState::TPartitionState(
    NProto::TPartitionMeta meta,
    ICompactionPolicyPtr compactionPolicy,
    ui32 compactionScoreHistorySize,
    ui32 cleanupScoreHistorySize,
    const TBackpressureFeaturesConfig& bpConfig,
    const TFreeSpaceConfig& freeSpaceConfig,
    ui32 maxIORequestsInFlight,
    ui32 reassignChannelsPercentageThreshold,
    ui32 reassignFreshChannelsPercentageThreshold,
    ui32 reassignMixedChannelsPercentageThreshold,
    bool reassignSystemChannelsImmediately,
    ui32 channelCount,
    ui32 mixedIndexCacheSize,
    ui64 allocationUnit,
    ui32 maxBlobsPerUnit,
    ui32 maxBlobsPerRange,
    ui32 compactionRangeCountPerRun,
    TPartitionThreadSafeStatePtr threadSafeState,
    ui64 tabletId,
    const std::optional<TMixedBlocksFilterConfig> mixedBlocksFilterConfig,
    bool checkpointAwareCleanupEnabled,
    bool compactionStatsTrackerEnabled,
    bool extendedBlobCountersEnabled)
    : TPartitionChannelsState(
          meta.GetConfig(),
          freeSpaceConfig,
          maxIORequestsInFlight,
          reassignChannelsPercentageThreshold,
          reassignFreshChannelsPercentageThreshold,
          reassignMixedChannelsPercentageThreshold,
          reassignSystemChannelsImmediately,
          channelCount)
    , TCommitIdsState()
    , TPartitionFreshBlobState(tabletId)
    , TPartitionTrimFreshLogState()
    , TPartitionFreshBlocksState(*this, *this, threadSafeState)
    , Meta(std::move(meta))
    , CompactionPolicy(compactionPolicy)
    , BPConfig(bpConfig)
    , FreeSpaceConfig(freeSpaceConfig)
    , ExtendedBlobCountersEnabled(extendedBlobCountersEnabled)
    , ThreadSafeState(std::move(threadSafeState))
    , Config(*Meta.MutableConfig())
    , MixedIndexCache(mixedIndexCacheSize, &MixedIndexCacheAllocator)
    , CompactionMap(GetMaxBlocksInBlob(), std::move(compactionPolicy))
    , CompactionScoreHistory(compactionScoreHistorySize)
    , UsedBlocks(Config.GetBlocksCount())
    , LogicalUsedBlocks(Config.GetBlocksCount())
    , MaxBlobsPerDisk(Min<ui64>(
          CalculatePerDiskThreshold(
              Config.GetBlocksCount(),
              Config.GetBlockSize(),
              allocationUnit,
              maxBlobsPerUnit),
          Max<ui32>()))
    , MaxBlobsPerRange(maxBlobsPerRange)
    , CompactionRangeCountPerRun(compactionRangeCountPerRun)
    , CleanupQueue(GetBlockSize())
    , CleanupScoreHistory(cleanupScoreHistorySize)
    , CheckpointAwareCleanupEnabled(checkpointAwareCleanupEnabled)
{
    if (compactionStatsTrackerEnabled) {
        CompactionStatsTracker.emplace(tabletId, CompactionMap, UsedBlocks);
    }

    if (mixedBlocksFilterConfig) {
        MixedBlocksFilter.emplace(
            tabletId,
            GetMaxBlocksInBlob(),
            Config.GetBlocksCount());

        if (mixedBlocksFilterConfig->MixedBlocksFilterRangesToLoadPerTx) {
            MixedBlocksFilterLoadState.emplace(
                *MixedBlocksFilter,
                CeilDiv<ui64>(Config.GetBlocksCount(), GetMaxBlocksInBlob()),
                mixedBlocksFilterConfig->MixedBlocksFilterRangesToLoadPerTx,
                mixedBlocksFilterConfig
                    ->MixedBlocksFilterAllowedCpuTimePerSecond);
        }
    }
    InitChannels();

    auto& stats = AccessStats();
    if (!ExtendedBlobCountersEnabled) {
        stats.ClearBlobByteCounters();
        stats.SetMixedChannelBlobsCount(0);
        stats.SetMergedChannelBlobsCount(0);
        stats.SetMixedChannelBlocksCount(0);
        stats.SetMergedChannelBlocksCount(0);
        stats.SetMixedIndexBlobsCount(0);
        stats.SetMergedIndexBlobsCount(0);
        stats.SetMixedIndexBlocksCount(0);
        stats.SetMergedIndexBlocksCount(0);
    } else if (!stats.HasBlobByteCounters()) {
        stats.SetMixedIndexBlobsCount(stats.GetMixedBlobsCount());
        stats.SetMergedIndexBlobsCount(stats.GetMergedBlobsCount());
        stats.SetMixedIndexBlocksCount(stats.GetMixedBlocksCount());
        stats.SetMergedIndexBlocksCount(stats.GetMergedBlocksCount());
        stats.SetMixedChannelBlobsCount(stats.GetMixedBlobsCount());
        stats.SetMergedChannelBlobsCount(stats.GetMergedBlobsCount());
        stats.SetMixedChannelBlocksCount(stats.GetMixedBlocksCount());
        stats.SetMergedChannelBlocksCount(stats.GetMergedBlocksCount());

        // Without mixed channels, mixed-index blobs live on merged channels.
        if (!GetChannelsByKind([](EChannelDataKind kind)
                               { return kind == EChannelDataKind::Mixed; }))
        {
            stats.SetMergedChannelBlobsCount(
                stats.GetMergedBlobsCount() + stats.GetMixedBlobsCount());
            stats.SetMixedChannelBlobsCount(0);
            stats.SetMergedChannelBlocksCount(
                stats.GetMergedBlocksCount() + stats.GetMixedBlocksCount());
            stats.SetMixedChannelBlocksCount(0);
        }

        // Enable before compression: initially stored sizes equal logical
        // sizes.
        auto& counters = *stats.MutableBlobByteCounters();
        counters.SetMixedBytesCount(
            stats.GetMixedChannelBlocksCount() * GetBlockSize());
        counters.SetMergedBytesCount(
            stats.GetMergedChannelBlocksCount() * GetBlockSize());
        counters.SetMixedIndexBytesCount(
            stats.GetMixedBlocksCount() * GetBlockSize());
        counters.SetMergedIndexBytesCount(
            stats.GetMergedBlocksCount() * GetBlockSize());
    }
}
bool TPartitionState::CheckBlockRange(const TBlockRange64& range) const
{
    Y_DEBUG_ABORT_UNLESS(Config.GetBlocksCount() <= Max<ui32>());
    const auto validRange =
        TBlockRange64::WithLength(0, Config.GetBlocksCount());
    return validRange.Contains(range);
}

TBackpressureReport TPartitionState::CalculateCurrentBackpressure() const
{
    const auto& freshFeature = BPConfig.FreshByteCountFeatureConfig;
    const auto& compactionFeature = BPConfig.CompactionScoreFeatureConfig;
    const auto& cleanupFeature = BPConfig.CleanupQueueBytesFeatureConfig;

    // Every unflushed fresh blob is also untrimmed.
    Y_DEBUG_ABORT_UNLESS(
        GetUnflushedFreshBlobByteCount() <= GetUntrimmedFreshBlobByteCount());

    const ui64 freshByteCount =
        GetUntrimmedFreshBlobByteCount() +
        GetStats().GetFreshBlocksCount() * GetBlockSize();

    return {
        BPFeature(freshFeature, freshByteCount),
        CompactionPolicy->BackpressureEnabled()
            ? BPFeature(compactionFeature, GetLegacyCompactionScore())
            : 0,
        GetBackpressureDiskSpaceScore(),
        GetCheckpoints().IsEmpty()
            ? BPFeature(cleanupFeature, CleanupQueue.GetQueueBytes())
            : 0,
    };
}

ui64 TPartitionState::GetMaxCheckpointCommitId() const
{
    return Max(
        GetCheckpoints().GetMaxCommitId(),
        GetCheckpointsInFlight()->GetMaxCommitId());
}

ui64 TPartitionState::GetMinCheckpointCommitId() const
{
    return Min(
        GetCheckpoints().GetMinCommitId(),
        GetCheckpointsInFlight()->GetMinCommitId());
}

ui64 TPartitionState::GetCleanupCommitId() const
{
    ui64 commitId = GetLastCommitId();

    // Should not cleanup after any barrier.
    commitId = Min(commitId, CleanupQueue.GetMinCommitId() - 1);

    if (!CheckpointAwareCleanupEnabled) {
        // Should not cleanup after any checkpoint.
        commitId = Min(commitId, GetMinCheckpointCommitId() - 1);
    }

    return commitId;
}

bool TPartitionState::HasBlobCountToCleanupReachedThreshold(
    ui64 cleanupCommitId,
    ui32 threshold) const
{
    const size_t blobCount = CleanupQueue.GetCount(
        GetCleanupMilestoneCommitId(),
        GetCleanupMilestoneBlobId(),
        cleanupCommitId,
        threshold);

    return blobCount >= threshold;
}

void TPartitionState::UpdateOrResetCleanupMilestone(
    ui64 newCommitId,
    TPartialBlobId newBlobId,
    ui64 minCheckpointCommitId,
    ui64 maxCheckpointCommitId)
{
    const auto& milestone = Meta.GetCleanupMilestone();

    if (minCheckpointCommitId != milestone.GetMinCheckpointCommitId() ||
        maxCheckpointCommitId != milestone.GetMaxCheckpointCommitId())
    {
        // Checkpoints changed, need to reset milestone.
        newCommitId = 0;
        newBlobId = {};
    }

    auto& updated = *Meta.MutableCleanupMilestone();
    updated.SetCommitId(newCommitId);
    updated.SetBlobCommitId(newBlobId.CommitId());
    updated.SetBlobUniqueId(newBlobId.UniqueId());
    updated.SetMinCheckpointCommitId(minCheckpointCommitId);
    updated.SetMaxCheckpointCommitId(maxCheckpointCommitId);
}

void TPartitionState::UpdateCleanupMilestoneIfNeeded(
    ui64 newCommitId,
    TPartialBlobId newBlobId,
    ui64 minCheckpointCommitId,
    ui64 maxCheckpointCommitId)
{
    const auto& milestone = Meta.GetCleanupMilestone();
    if (minCheckpointCommitId != milestone.GetMinCheckpointCommitId() ||
        maxCheckpointCommitId != milestone.GetMaxCheckpointCommitId())
    {
        // Checkpoint bounds are stale, should not update the milestone.
        return;
    }

    UpdateOrResetCleanupMilestone(
        newCommitId,
        newBlobId,
        minCheckpointCommitId,
        maxCheckpointCommitId);
}

ui64 TPartitionState::CalculateCheckpointBytes() const
{
    const auto* lastCheckpoint = GetCheckpoints().GetLast();
    if (!lastCheckpoint) {
        return 0;
    }

    const auto& lastStats = lastCheckpoint->Stats;
    ui64 blocksCount = GetUnflushedFreshBlocksCount();
    blocksCount += lastStats.GetMixedBlocksCount();
    blocksCount += lastStats.GetMergedBlocksCount();
    return blocksCount * GetBlockSize();
}

ui64 TPartitionState::GetCollectCommitId() const
{
    ui64 commitId = GetLastCommitId();

    // should not collect after any barrier
    commitId = Min(commitId, GarbageQueue.GetMinCommitId() - 1);

    return commitId;
}

bool TPartitionState::OverlapsUnconfirmedBlobs(
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange) const
{
    return Overlaps(UnconfirmedBlobs, lowCommitId, highCommitId, blockRange);
}

bool TPartitionState::OverlapsConfirmedBlobs(
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange) const
{
    return Overlaps(ConfirmedBlobs, lowCommitId, highCommitId, blockRange);
}

void TPartitionState::InitUnconfirmedBlobs(
    TCommitIdToBlobsToConfirm blobs)
{
    UnconfirmedBlobs = std::move(blobs);
    UnconfirmedBlobCount = 0;

    for (const auto& [commitId, blobs]: UnconfirmedBlobs) {
        UnconfirmedBlobCount += blobs.size();
        AccessCommitQueue()->AcquireBarrier(commitId);
        GarbageQueue.AcquireBarrier(commitId);
    }
}

void TPartitionState::WriteUnconfirmedBlob(
    TPartitionDatabase& db,
    ui64 commitId,
    const TBlobToConfirm& blob)
{
    auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
    db.WriteUnconfirmedBlob(blobId, blob);
    UnconfirmedBlobs[commitId].push_back(blob);
    UnconfirmedBlobCount++;
}

void TPartitionState::DeleteUnconfirmedBlobs(
    TPartitionDatabase& db,
    ui64 commitId)
{
    auto it = UnconfirmedBlobs.find(commitId);
    if (it != UnconfirmedBlobs.end()) {
        const auto& blobs = it->second;
        for (const auto& blob: blobs) {
            auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
            db.DeleteUnconfirmedBlob(blobId);
        }

        const size_t blobCount = blobs.size();
        UnconfirmedBlobs.erase(it);
        Y_DEBUG_ABORT_UNLESS(UnconfirmedBlobCount >= blobCount);
        UnconfirmedBlobCount -= blobCount;
    }
}

void TPartitionState::ConfirmedBlobsAdded(
    TPartitionDatabase& db,
    ui64 commitId)
{
    auto it = ConfirmedBlobs.find(commitId);
    if (it == ConfirmedBlobs.end()) {
        return;
    }

    auto& blobs = it->second;
    const size_t blobCount = blobs.size();

    for (const auto& blob: blobs) {
        auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
        db.DeleteUnconfirmedBlob(blobId);
    }

    ConfirmedBlobs.erase(it);
    Y_DEBUG_ABORT_UNLESS(ConfirmedBlobCount >= blobCount);
    ConfirmedBlobCount -= blobCount;

    GarbageQueue.ReleaseBarrier(commitId);
    AccessCommitQueue()->ReleaseBarrier(commitId);
}

void TPartitionState::BlobsConfirmed(
    ui64 commitId,
    TVector<TBlobToConfirm> blobs)
{
    auto it = UnconfirmedBlobs.find(commitId);
    Y_DEBUG_ABORT_UNLESS(it != UnconfirmedBlobs.end());

    auto& dstBlobs = it->second;
    const size_t blobCount = dstBlobs.size();
    Y_DEBUG_ABORT_UNLESS(blobs.empty() || blobCount == blobs.size());
    for (ui32 i = 0; i < Min(blobCount, blobs.size()); ++i) {
        const auto blockRange = dstBlobs[i].BlockRange;
        Y_DEBUG_ABORT_UNLESS(dstBlobs[i].UniqueId == blobs[i].UniqueId);
        Y_DEBUG_ABORT_UNLESS(blockRange.Start == blobs[i].BlockRange.Start);
        Y_DEBUG_ABORT_UNLESS(blockRange.End == blobs[i].BlockRange.End);
        Y_DEBUG_ABORT_UNLESS(blockRange.Size() == blobs[i].Checksums.size());
        if (dstBlobs[i].UniqueId == blobs[i].UniqueId) {
            dstBlobs[i].Checksums = std::move(blobs[i]).Checksums;
        }
    }

    ConfirmedBlobs[commitId] = std::move(dstBlobs);
    ConfirmedBlobCount += blobCount;

    UnconfirmedBlobs.erase(it);
    Y_DEBUG_ABORT_UNLESS(UnconfirmedBlobCount >= blobCount);
    UnconfirmedBlobCount -= blobCount;
}

void TPartitionState::ConfirmBlobs(
    TPartitionDatabase& db,
    const TVector<TPartialBlobId>& unrecoverableBlobs)
{
    THashSet<ui64> unrecoverableCommitIds;
    for (auto blobId: unrecoverableBlobs) {
        unrecoverableCommitIds.insert(blobId.CommitId());
    }

    for (ui64 commitId: unrecoverableCommitIds) {
        auto it = UnconfirmedBlobs.find(commitId);
        if (it == UnconfirmedBlobs.end()) {
            Y_DEBUG_ABORT_UNLESS(
                false,
                "CommitId %lu not found in UnconfirmedBlobs",
                commitId);
            continue;
        }

        auto& blobs = it->second;

        for (const auto& blob: blobs) {
            auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
            db.DeleteUnconfirmedBlob(blobId);
            Y_DEBUG_ABORT_UNLESS(UnconfirmedBlobCount >= 1);
            --UnconfirmedBlobCount;
        }

        UnconfirmedBlobs.erase(it);

        GarbageQueue.ReleaseBarrier(commitId);
        AccessCommitQueue()->ReleaseBarrier(commitId);
    }

    ConfirmedBlobs = std::move(UnconfirmedBlobs);
    ConfirmedBlobCount = UnconfirmedBlobCount;

    UnconfirmedBlobs.clear();
    UnconfirmedBlobCount = 0;
}

ui64 TPartitionState::GetMixedBytesCount() const
{
    return GetStats().GetBlobByteCounters().GetMixedBytesCount();
}

ui64 TPartitionState::GetMergedBytesCount() const
{
    return GetStats().GetBlobByteCounters().GetMergedBytesCount();
}

ui64 TPartitionState::GetMixedIndexBytesCount() const
{
    return GetStats().GetBlobByteCounters().GetMixedIndexBytesCount();
}

ui64 TPartitionState::GetMergedIndexBytesCount() const
{
    return GetStats().GetBlobByteCounters().GetMergedIndexBytesCount();
}

void TPartitionState::UpdateBlobCounters(
    const TPartialBlobId& blobId,
    EChannelDataKind indexKind,
    ui64 blocksCount,
    bool added)
{
    const bool isDeletionMarker = IsDeletionMarker(blobId);
    // Deletion markers contribute to index blob totals, but not block totals.
    if (isDeletionMarker) {
        blocksCount = 0;
    }

    const auto update = [added](ui64 value, ui64 delta)
    {
        return added ? SafeIncrement(value, delta)
                     : SafeDecrement(value, Min(value, delta));
    };

    auto& stats = AccessStats();
    switch (indexKind) {
        case EChannelDataKind::Mixed:
            stats.SetMixedBlobsCount(update(stats.GetMixedBlobsCount(), 1));
            stats.SetMixedBlocksCount(
                update(stats.GetMixedBlocksCount(), blocksCount));
            break;
        case EChannelDataKind::Merged:
            stats.SetMergedBlobsCount(update(stats.GetMergedBlobsCount(), 1));
            stats.SetMergedBlocksCount(
                update(stats.GetMergedBlocksCount(), blocksCount));
            break;
        default:
            Y_ABORT("Unexpected index kind: %u", static_cast<ui32>(indexKind));
    }

    if (!ExtendedBlobCountersEnabled) {
        return;
    }

    const ui64 bytesCount = isDeletionMarker ? 0 : blobId.BlobSize();
    auto& counters = *stats.MutableBlobByteCounters();
    switch (indexKind) {
        case EChannelDataKind::Mixed:
            stats.SetMixedIndexBlobsCount(
                update(stats.GetMixedIndexBlobsCount(), 1));
            stats.SetMixedIndexBlocksCount(
                update(stats.GetMixedIndexBlocksCount(), blocksCount));
            counters.SetMixedIndexBytesCount(
                update(counters.GetMixedIndexBytesCount(), bytesCount));
            break;
        case EChannelDataKind::Merged:
            stats.SetMergedIndexBlobsCount(
                update(stats.GetMergedIndexBlobsCount(), 1));
            stats.SetMergedIndexBlocksCount(
                update(stats.GetMergedIndexBlocksCount(), blocksCount));
            counters.SetMergedIndexBytesCount(
                update(counters.GetMergedIndexBytesCount(), bytesCount));
            break;
        default:
            Y_ABORT("Unexpected index kind: %u", static_cast<ui32>(indexKind));
    }

    // Deletion markers do not have a data channel.
    if (isDeletionMarker) {
        return;
    }

    switch (GetChannelDataKind(blobId.Channel())) {
        case EChannelDataKind::Mixed:
            stats.SetMixedChannelBlobsCount(
                update(stats.GetMixedChannelBlobsCount(), 1));
            stats.SetMixedChannelBlocksCount(
                update(stats.GetMixedChannelBlocksCount(), blocksCount));
            counters.SetMixedBytesCount(
                update(counters.GetMixedBytesCount(), bytesCount));
            break;
        case EChannelDataKind::Merged:
            stats.SetMergedChannelBlobsCount(
                update(stats.GetMergedChannelBlobsCount(), 1));
            stats.SetMergedChannelBlocksCount(
                update(stats.GetMergedChannelBlocksCount(), blocksCount));
            counters.SetMergedBytesCount(
                update(counters.GetMergedBytesCount(), bytesCount));
            break;
        default:
            Y_ABORT("Unexpected blob channel: %u", blobId.Channel());
    }
}

#define BLOCKSTORE_PARTITION_IMPLEMENT_COUNTER(name)                           \
    ui64 TPartitionState::Increment##name(size_t value)                        \
    {                                                                          \
        auto& stats = AccessStats();                                           \
        ui64 counter = SafeIncrement(stats.Get##name(), value);                \
        stats.Set##name(counter);                                              \
        return counter;                                                        \
    }                                                                          \
                                                                               \
    ui64 TPartitionState::Decrement##name(size_t value)                        \
    {                                                                          \
        auto& stats = AccessStats();                                           \
        ui64 counter = SafeDecrement(stats.Get##name(), value);                \
        stats.Set##name(counter);                                              \
        return counter;                                                        \
    }                                                                          \
// BLOCKSTORE_PARTITION_IMPLEMENT_COUNTER

BLOCKSTORE_PARTITION_PROTO_COUNTERS(BLOCKSTORE_PARTITION_IMPLEMENT_COUNTER)

#undef BLOCKSTORE_PARTITION_IMPLEMENT_COUNTER

void TPartitionState::AddFreshBlob(
    ui64 commitId,
    const TPartialBlobId& blobId,
    ui64 blobSize,
    TInstant timestamp)
{
    // Every installed fresh blob is above every barrier sent.
    Y_ABORT_UNLESS(
        commitId >
        Max(GetLastTrimFreshLogToCommitId(),
            GetLastSentSelectiveTrimToCommitId()));
    TPartitionFreshBlobState::AddFreshBlob(
        commitId,
        blobId,
        blobSize,
        timestamp);
}

ui32 TPartitionState::IncrementUnflushedFreshBlocksFromDbCount(size_t value)
{
    auto& stats = AccessStats();
    ui64 counter = SafeIncrement(stats.GetFreshBlocksCount(), value);
    stats.SetFreshBlocksCount(counter);
    return counter;
}

ui32 TPartitionState::DecrementUnflushedFreshBlocksFromDbCount(size_t value)
{
    auto& stats = AccessStats();
    ui64 counter = SafeDecrement(stats.GetFreshBlocksCount(), value);
    stats.SetFreshBlocksCount(counter);
    return counter;
}

void TPartitionState::WriteFreshBlocksToDb(
    TPartitionDatabase& db,
    const TBlockRange32& writeRange,
    ui64 commitId,
    TSgList sglist)
{
    Y_ABORT_UNLESS(writeRange.Size() == sglist.size());

    WriteFreshBlocksImpl(
        db,
        writeRange,
        commitId,
        [&](ui32 index) { return sglist[index]; });
}

void TPartitionState::ZeroFreshBlocksToDb(
    TPartitionDatabase& db,
    const TBlockRange32& zeroRange,
    ui64 commitId)
{
    WriteFreshBlocksImpl(
        db,
        zeroRange,
        commitId,
        [](ui32) { return TBlockDataRef(); });
}

void TPartitionState::DeleteFreshBlockFromDb(
    TPartitionDatabase& db,
    ui32 blockIndex,
    ui64 commitId)
{
    bool removed = Blocks.RemoveBlock(
        blockIndex,
        commitId,
        true);   // isStoredInDb

    Y_ABORT_UNLESS(removed);

    db.DeleteFreshBlock(blockIndex, commitId);
    DecrementUnflushedFreshBlocksFromDbCount(1);
}

////////////////////////////////////////////////////////////////////////////////
// Mixed blocks

void TPartitionState::WriteMixedBlock(TPartitionDatabase& db, TMixedBlock block)
{
    const ui32 rangeIdx = CompactionMap.GetRangeIndex(block.BlockIndex);
    MixedIndexCache.InsertBlockIfHot(rangeIdx, block);

    db.WriteMixedBlock(block);

    if (MixedBlocksFilter) {
        MixedBlocksFilter->BlocksAddedToMixedIndex(
            block.BlockIndex,
            block.CommitId);
    }
}

void TPartitionState::WriteMixedBlocks(
    TPartitionDatabase& db,
    const TPartialBlobId& blobId,
    const TVector<ui32>& blockIndices,
    ui8 compactionRangeCount)
{
    const ui64 commitId = blobId.CommitId();
    ui16 blobOffset = 0;

    for (const ui32 blockIndex: blockIndices) {
        const ui32 rangeIdx = CompactionMap.GetRangeIndex(blockIndex);
        MixedIndexCache.InsertBlockIfHot(
            rangeIdx,
            {blobId, commitId, blockIndex, blobOffset, compactionRangeCount});

        ++blobOffset;
    }

    db.WriteMixedBlocks(blobId, blockIndices, compactionRangeCount);

    if (MixedBlocksFilter) {
        for (const ui32 blockIndex: blockIndices) {
            MixedBlocksFilter->BlocksAddedToMixedIndex(blockIndex, commitId);
        }
    }
}

void TPartitionState::DeleteMixedBlock(
    TPartitionDatabase& db,
    ui32 blockIndex,
    ui64 commitId)
{
    const ui32 rangeIdx = CompactionMap.GetRangeIndex(blockIndex);
    MixedIndexCache.EraseBlockIfHot(rangeIdx, { blockIndex, commitId });
    db.DeleteMixedBlock(blockIndex, commitId);
}

bool TPartitionState::FindMixedBlocksForCompaction(
    TPartitionDatabase& db,
    IMixedBlocksIndexVisitor& visitor,
    ui32 rangeIdx)
{
    if (MixedIndexCache.VisitBlocksIfHot(rangeIdx, visitor)) {
        // Compaction range is hot: no need to query db.
        return true;
    }

    auto cacheInserter = MixedIndexCache.GetInserterForRange(rangeIdx);

    struct TVisitorAndCacheInserter final: public IMixedBlocksIndexVisitor
    {
        IMixedBlocksIndexVisitor& Visitor;
        TMixedIndexCache::TInserterPtr CacheInserter;

        TVisitorAndCacheInserter(
            IMixedBlocksIndexVisitor& visitor,
            TMixedIndexCache::TInserterPtr cacheInserter)
            : Visitor(visitor)
            , CacheInserter(std::move(cacheInserter))
        {}

        bool VisitBlock(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset,
            ui8 compactionRangeCount) override
        {
            bool ok = Visitor.VisitBlock(
                blockIndex,
                commitId,
                blobId,
                blobOffset,
                compactionRangeCount);
            Y_ABORT_UNLESS(ok);

            CacheInserter->Insert(
                {blobId,
                 commitId,
                 blockIndex,
                 blobOffset,
                 compactionRangeCount});
            return true;
        }

    } visitorAndCacheInserter(visitor, std::move(cacheInserter));

    return db.FindMixedBlocks(
        visitorAndCacheInserter,
        CompactionMap.GetBlockRange(rangeIdx),
        true);  // precharge
}

void TPartitionState::RaiseRangeTemperature(ui32 rangeIdx)
{
    MixedIndexCache.RaiseRangeTemperature(rangeIdx);
}

ui64 TPartitionState::GetMixedIndexCacheMemSize() const
{
    return MixedIndexCacheAllocator.GetBytesAllocated();
}

////////////////////////////////////////////////////////////////////////////////
// Compaction

TOperationState& TPartitionState::GetCompactionState(ECompactionType type)
{
    return type == ECompactionType::Forced ?
        ForcedCompactionState.State :
        CompactionState;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionState::SetUsedBlocks(
    TPartitionDatabase& db,
    const TBlockRange32& range,
    ui32 skipCount)
{
    ui64 blockCount = GetUsedBlocks().Set(range.Start, range.End + 1) - skipCount;
    ui32 logicalBlockCount = 0;

    if (GetBaseDiskId()) {
        logicalBlockCount = GetLogicalUsedBlocks().Set(range.Start, range.End + 1) - skipCount;
    } else {
        logicalBlockCount = blockCount;
    }

    IncrementUsedBlocksCount(blockCount);
    IncrementLogicalUsedBlocksCount(logicalBlockCount);

    if (blockCount || logicalBlockCount) {
        WriteUsedBlocksToDB(db, range.Start, range.End + 1);
    }
}

void TPartitionState::SetUsedBlocks(
    TPartitionDatabase& db,
    const TVector<ui32>& blocks)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(blocks.begin(), blocks.end()));

    ui32 blockCount = 0;
    ui32 logicalBlockCount = 0;

    for (const ui32 b: blocks) {
        ui64 count = GetUsedBlocks().Set(b, b + 1);

        blockCount += count;

        if (GetBaseDiskId()) {
            logicalBlockCount += GetLogicalUsedBlocks().Set(b, b + 1);
        } else {
            logicalBlockCount += count;
        }
    }

    IncrementUsedBlocksCount(blockCount);
    IncrementLogicalUsedBlocksCount(logicalBlockCount);

    if (blockCount || logicalBlockCount) {
        auto first = blocks.begin();
        auto last = std::next(first);

        while (last != blocks.end()) {
            if (*last != *std::prev(last) + 1) {
                WriteUsedBlocksToDB(db, *first, *std::prev(last) + 1);
                first = last;
            }
            ++last;
        }

        if (first != blocks.end()) {
            WriteUsedBlocksToDB(db, *first, *std::prev(last) + 1);
        }
    }
}

void TPartitionState::UnsetUsedBlocks(
    TPartitionDatabase& db,
    const TBlockRange32& range)
{
    ui32 blockCount = GetUsedBlocks().Unset(range.Start, range.End + 1);
    ui32 logicalBlockCount = 0;

    if (GetBaseDiskId()) {
        logicalBlockCount = GetLogicalUsedBlocks().Unset(range.Start, range.End + 1);
    } else {
        logicalBlockCount = blockCount;
    }

    DecrementUsedBlocksCount(blockCount);
    DecrementLogicalUsedBlocksCount(logicalBlockCount);

    if (blockCount || logicalBlockCount) {
        WriteUsedBlocksToDB(db, range.Start, range.End + 1);
    }
}

void TPartitionState::UnsetUsedBlocks(
    TPartitionDatabase& db,
    const TVector<ui32>& blocks)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(blocks.begin(), blocks.end()));

    ui32 blockCount = 0;
    ui32 logicalBlockCount = 0;

    for (const ui32 b: blocks) {
        ui64 count = GetUsedBlocks().Unset(b, b + 1);

        blockCount += count;

        if (GetBaseDiskId()) {
            logicalBlockCount += GetLogicalUsedBlocks().Unset(b, b + 1);
        } else {
            logicalBlockCount += count;
        }
    }

    DecrementUsedBlocksCount(blockCount);
    DecrementLogicalUsedBlocksCount(logicalBlockCount);

    if (blockCount || logicalBlockCount) {
        auto first = blocks.begin();
        auto last = std::next(first);

        while (last != blocks.end()) {
            if (*last != *std::prev(last) + 1) {
                WriteUsedBlocksToDB(db, *first, *std::prev(last) + 1);
                first = last;
            }
            ++last;
        }

        if (first != blocks.end()) {
            WriteUsedBlocksToDB(db, *first, *std::prev(last) + 1);
        }
    }
}

ui32 TPartitionState::CalculateNewlyZeroedBlocks(
    ui32 blockIndex,
    ui64 usedBlockCount) const
{
    const auto& prevRangeStat = CompactionMap.Get(blockIndex);
    const i64 usedBlockCountDiff =
        static_cast<i64>(usedBlockCount) - prevRangeStat.UsedBlockCount;

    return SafeIntegerCast<ui32>(std::max(
        static_cast<i64>(prevRangeStat.NewlyZeroedBlocks) - usedBlockCountDiff,
        0L));
}

void TPartitionState::WriteUsedBlocksToDB(
    TPartitionDatabase& db,
    ui32 begin,
    ui32 end)
{
    auto serializer = GetUsedBlocks().RangeSerializer(begin, end);
    TCompressedBitmap::TSerializedChunk sc;
    while (serializer.Next(&sc)) {
        db.WriteUsedBlocks(sc);
    }

    if (GetBaseDiskId()) {
        auto serializerLogical = GetLogicalUsedBlocks().RangeSerializer(begin, end);
        while (serializerLogical.Next(&sc)) {
            db.WriteLogicalUsedBlocks(sc);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionState::DumpHtml(IOutputStream& out) const
{
    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "LastCommitId"; }
                    TABLED() { out << GetLastCommitId(); }
                }
                TABLER() {
                    TABLED() { out << "FreshBlocks"; }
                    TABLED() {
                        out << "Total: " << GetUnflushedFreshBlocksCount()
                            << ", FromDb: " << GetStats().GetFreshBlocksCount()
                            << ", FromChannel: "
                            << GetUnflushedFreshBlocksCountFromChannel()
                            << ", InFlight: " << GetFreshBlocksInFlight()
                            << ", Queued: " << GetFreshBlocksQueued()
                            << ", UntrimmedBytes: "
                            << GetUntrimmedFreshBlobByteCount()
                            << ", DeadBytes: "
                            << GetDeadUntrimmedFreshBlobByteCount();
                    }
                }
                TABLER() {
                    TABLED() { out << "Flush"; }
                    TABLED () {
                        DumpOperationState(
                            out,
                            GetFlushState().GetOperationState());
                    }
                }                TABLER() {
                    TABLED() { out << "Compaction"; }
                    TABLED() { DumpOperationState(out, CompactionState); }
                }
                TABLER() {
                    TABLED() { out << "CompactionDelay"; }
                    TABLED() { out << CompactionDelay; }
                }
                TABLER() {
                    TABLED() { out << "Cleanup"; }
                    TABLED() { DumpOperationState(out, CleanupState); }
                }
                TABLER() {
                    TABLED() { out << "CleanupDelay"; }
                    TABLED() { out << CleanupDelay; }
                }
                TABLER() {
                    TABLED() { out << "CollectGarbage"; }
                    TABLED() { DumpOperationState(out, CollectGarbageState); }
                }
            }
        }
    }
}

TJsonValue TPartitionState::AsJson() const
{
    TJsonValue json;

    {
        TJsonValue state;
        state["LastCommitId"] = GetLastCommitId();
        state["FreshBlocksTotal"] = GetUnflushedFreshBlocksCount();
        state["FreshBlocksFromDb"] = GetStats().GetFreshBlocksCount();
        state["FreshBlocksFromChannel"] =
            GetUnflushedFreshBlocksCountFromChannel();
        state["FreshBlocksInFlight"] = GetFreshBlocksInFlight();
        state["FreshBlocksQueued"] = GetFreshBlocksQueued();
        state["FreshBlobUntrimmedBytes"] = GetUntrimmedFreshBlobByteCount();
        state["FreshBlobDeadBytes"] = GetDeadUntrimmedFreshBlobByteCount();
        const auto firstUnflushedFreshBlobTimestamp =
            GetFirstUnflushedFreshBlobTimestamp(GetTrimFreshLogToCommitId());
        if (firstUnflushedFreshBlobTimestamp != TInstant::Max()) {
            state["FirstUnflushedFreshBlobTimestamp"] =
                firstUnflushedFreshBlobTimestamp.MicroSeconds();
        }
        state["FlushState"] = ToJson(GetFlushState().GetOperationState());
        state["Compaction"] = ToJson(CompactionState);
        state["Cleanup"] = ToJson(CleanupState);
        state["CollectGarbage"] = ToJson(CollectGarbageState);

        json["State"] = std::move(state);
    }
    json["Checkpoints"] = GetCheckpoints().AsJson();

    {
        TJsonValue stats;
        try {
            NProtobufJson::Proto2Json(GetStats(), stats);
            json["Stats"] = std::move(stats);
        } catch (...) {}
    }
    {
        TJsonValue config;
        try {
            NProtobufJson::Proto2Json(Config, config);
            json["Config"] = std::move(config);
        } catch (...) {}
    }

    return json;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
