#include "part_fresh_blocks_state.h"

#include <cloud/blockstore/libs/storage/partition_common/model/operation_status.h>

#include <cloud/storage/core/libs/tablet/model/channels.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using TJsonValue = NJson::TJsonValue;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T SafeIncrement(T counter, size_t value)
{
    Y_ABORT_UNLESS(value <= Max<T>() - counter);
    return counter + value;
}

template <typename T>
T SafeDecrement(T counter, size_t value)
{
    Y_ABORT_UNLESS(counter >= value);
    return counter - value;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> TPartitionFreshBlobState::GetUnflushedFreshBlobCommitIds(
    ui64 commitId) const
{
    TVector<ui64> commitIds;
    for (const auto& [blobCommitId, blob]: UnflushedFreshBlobsByCommitId) {
        if (blobCommitId > commitId) {
            break;
        }
        commitIds.push_back(blobCommitId);
    }
    return commitIds;
}

TInstant TPartitionFreshBlobState::GetFirstUnflushedFreshBlobTimestamp(
    ui64 trimFreshLogToCommitId) const
{
    const auto it =
        UnflushedFreshBlobsByCommitId.upper_bound(trimFreshLogToCommitId);
    if (it == UnflushedFreshBlobsByCommitId.end()) {
        return TInstant::Max();
    }
    return it->second.Timestamp;
}

void TPartitionFreshBlobState::AddFreshBlob(
    ui64 commitId,
    const TPartialBlobId& blobId,
    ui64 blobSize,
    TInstant timestamp)
{
    // A flag with a wrong blob id would protect a blob that does not exist,
    // and a nonzero part id in a flag is rejected by every VDisk.
    STORAGE_VERIFY_C(
        blobId.CommitId() == commitId && blobId.PartId() == 0,
        TWellKnownEntityTypes::TABLET,
        TabletID,
        "Commit id: " << commitId << ", blob id: " << blobId);

    {
        const TUntrimmedFreshBlob blob{
            .ByteCount = blobSize,
            .BlobId = blobId};
        const bool inserted =
            UntrimmedFreshBlobsByCommitId.emplace(commitId, blob).second;
        STORAGE_VERIFY_C(
            inserted,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId);
        UntrimmedFreshBlobByteCount += blobSize;
    }

    {
        const TUnflushedFreshBlob blob{.ByteCount = blobSize,
                                       .Timestamp = timestamp};
        const bool inserted =
            UnflushedFreshBlobsByCommitId.emplace(commitId, blob).second;
        STORAGE_VERIFY_C(
            inserted,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId);
        UnflushedFreshBlobCount = SafeIncrement(UnflushedFreshBlobCount, 1);
        UnflushedFreshBlobByteCount =
            SafeIncrement(UnflushedFreshBlobByteCount, blobSize);
    }
}

void TPartitionFreshBlobState::MarkFreshBlobDead(ui64 commitId)
{
    auto it = UntrimmedFreshBlobsByCommitId.find(commitId);
    STORAGE_VERIFY_C(
        it != UntrimmedFreshBlobsByCommitId.end() && !it->second.Dead,
        TWellKnownEntityTypes::TABLET,
        TabletID,
        "Commit id: " << commitId);
    STORAGE_VERIFY_C(
        it->second.ByteCount <= Max<ui64>() - TotalDeadFreshBlobByteCount,
        TWellKnownEntityTypes::TABLET,
        TabletID,
        "Commit id: " << commitId << ", byte count: " << it->second.ByteCount
                      << ", total dead byte count: "
                      << TotalDeadFreshBlobByteCount);

    it->second.Dead = true;
    DeadUntrimmedFreshBlobByteCount += it->second.ByteCount;
    TotalDeadFreshBlobByteCount += it->second.ByteCount;
}

ui64 TPartitionFreshBlobState::GetAndUpdateOrdinaryTrimmableFreshBlobByteCount(
    ui64 ordinaryTrimToCommitId)
{
    Y_DEBUG_ABORT_UNLESS(
        ordinaryTrimToCommitId >= TrimmableByOrdinaryToCommitId,
        "OrdinaryTrimToCommitId moved down: %lu < %lu",
        ordinaryTrimToCommitId,
        TrimmableByOrdinaryToCommitId);

    if (ordinaryTrimToCommitId > TrimmableByOrdinaryToCommitId) {
        for (auto it = UntrimmedFreshBlobsByCommitId.upper_bound(
                 TrimmableByOrdinaryToCommitId);
             it != UntrimmedFreshBlobsByCommitId.end() &&
             it->first <= ordinaryTrimToCommitId;
             ++it)
        {
            OrdinaryTrimmableFreshBlobByteCount += it->second.ByteCount;
        }
        TrimmableByOrdinaryToCommitId = ordinaryTrimToCommitId;
    }

    return OrdinaryTrimmableFreshBlobByteCount;
}

// A blob is dead once it holds no trim barrier reference, i.e. all its blocks
// were flushed or overwritten. Only dead blobs may be trimmed.
//
// Checks the Dead flags against liveCommitIds and the cached byte counts.
// Selective trim relies on the Dead flags alone, so every live blob that it
// may pass must have an untrimmed entry.
bool TPartitionFreshBlobState::CheckDeadFreshBlobByteCount(
    const TVector<ui64>& liveCommitIds,
    ui64 addedToCommitId) const
{
    ui64 deadByteCount = 0;
    ui64 ordinaryTrimByteCount = 0;

    auto live = liveCommitIds.begin();
    for (const auto& [commitId, blob]: UntrimmedFreshBlobsByCommitId) {
        for (; live != liveCommitIds.end() && *live < commitId; ++live) {
            if (*live <= addedToCommitId) {
                return false;
            }
        }
        const bool isLive = live != liveCommitIds.end() && *live == commitId;
        if (isLive) {
            ++live;
        }
        if (blob.Dead == isLive) {
            return false;
        }
        if (blob.Dead) {
            deadByteCount += blob.ByteCount;
        }
        if (commitId <= TrimmableByOrdinaryToCommitId) {
            if (isLive) {
                return false;
            }
            ordinaryTrimByteCount += blob.ByteCount;
        }
    }

    return (live == liveCommitIds.end() || *live > addedToCommitId) &&
           deadByteCount == DeadUntrimmedFreshBlobByteCount &&
           ordinaryTrimByteCount == OrdinaryTrimmableFreshBlobByteCount;
}

TUntrimmedFreshBlobsBreakdown
TPartitionFreshBlobState::GetUntrimmedFreshBlobsBreakdown(
    ui64 ordinaryTrimToCommitId,
    ui64 lastSentSelectiveTrimToCommitId) const
{
    TUntrimmedFreshBlobsBreakdown result;

    auto add = [](TFreshBlobStats& stats, const TUntrimmedFreshBlob& blob)
    {
        ++stats.BlobCount;
        stats.ByteCount += blob.ByteCount;
    };

    for (const auto& [commitId, blob]: UntrimmedFreshBlobsByCommitId) {
        if (commitId <= ordinaryTrimToCommitId) {
            add(result.OrdinaryTrimmable, blob);
        } else if (blob.Dead) {
            add(result.DeadBehindLive, blob);
        } else {
            add(result.Live, blob);
            if (commitId <= lastSentSelectiveTrimToCommitId) {
                ++result.KeptBlobCount;
            }
        }
    }

    return result;
}

TTrimFreshBlobsResult TPartitionFreshBlobState::TrimFreshBlobs(ui64 commitId)
{
    auto& blobs = UntrimmedFreshBlobsByCommitId;

    while (blobs && blobs.begin()->first <= commitId) {
        EraseUntrimmedFreshBlob(blobs.begin());
    }

    TTrimFreshBlobsResult result;
    auto& unflushed = UnflushedFreshBlobsByCommitId;
    while (unflushed && unflushed.begin()->first <= commitId) {
        result.UnflushedBytesRemoved +=
            EraseUnflushedFreshBlob(unflushed.begin());
    }
    return result;
}

TTrimFreshBlobsResult TPartitionFreshBlobState::TrimCollectedFreshBlobs(
    const TVector<ui64>& commitIds)
{
    TTrimFreshBlobsResult result;
    for (ui64 commitId: commitIds) {
        auto it = UntrimmedFreshBlobsByCommitId.find(commitId);
        STORAGE_VERIFY_C(
            it != UntrimmedFreshBlobsByCommitId.end(),
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId);
        EraseUntrimmedFreshBlob(it);

        // A flush may have removed the unflushed entry already.
        auto& unflushed = UnflushedFreshBlobsByCommitId;
        if (auto jt = unflushed.find(commitId); jt != unflushed.end()) {
            result.UnflushedBytesRemoved += EraseUnflushedFreshBlob(jt);
        }
    }
    return result;
}

std::optional<TSelectiveTrimPlan>
TPartitionFreshBlobState::BuildSelectiveTrimPlan(
    const TSelectiveTrimPlanParams& params) const
{
    // The barrier must be above OrdinaryTrimToCommitId and at or below
    // SelectiveTrimToCommitId. An overwritten fresh write that has not
    // completed can let OrdinaryTrimToCommitId pass SelectiveTrimToCommitId.
    if (params.SelectiveTrimToCommitId <= params.OrdinaryTrimToCommitId) {
        return std::nullopt;
    }

    const ui64 minTrimToCommitId =
        Max(params.OrdinaryTrimToCommitId,
            params.LastSentSelectiveTrimToCommitId);

    TSelectiveTrimPlan plan{.TrimToCommitId = minTrimToCommitId};
    // The number of Keep flags at or below plan.TrimToCommitId. The flags for
    // the live blobs after the last dead one are dropped at the end.
    size_t keepCount = 0;

    for (auto it = UntrimmedFreshBlobsByCommitId.upper_bound(
             params.OrdinaryTrimToCommitId);
         it != UntrimmedFreshBlobsByCommitId.end() &&
         it->first <= params.SelectiveTrimToCommitId;
         ++it)
    {
        const auto& [commitId, blob] = *it;

        if (commitId <= minTrimToCommitId) {
            if (blob.Dead) {
                // An earlier soft command may have kept it, and only DoNotKeep
                // lets a soft barrier collect a kept blob.
                plan.DoNotKeep.push_back(blob.BlobId);
                plan.CollectingCommitIds.push_back(commitId);
            } else if (commitId > params.KeptToCommitId) {
                // Covered by a soft command that failed. The new barrier is at
                // least as high, so this Keep is mandatory.
                plan.Keep.push_back(blob.BlobId);
                keepCount = plan.Keep.size();
            }
            continue;
        }

        if (!blob.Dead) {
            plan.Keep.push_back(blob.BlobId);
            continue;
        }

        // A dead blob: move the barrier onto it and keep the live blobs below
        // it.
        plan.TrimToCommitId = commitId;
        plan.CollectingCommitIds.push_back(commitId);
        keepCount = plan.Keep.size();
    }
    // Drop Keep for the live blobs after the last dead one: they are above
    // the barrier, which does not touch them.
    Y_DEBUG_ABORT_UNLESS(
        keepCount <= plan.Keep.size(),
        "keepCount: %lu, Keep size: %lu",
        keepCount,
        plan.Keep.size());
    plan.Keep.resize(keepCount);

    // Without flags, a barrier at minTrimToCommitId collects nothing that the
    // hard barrier of an ordinary trim does not.
    if (plan.TrimToCommitId == minTrimToCommitId && !plan.Keep &&
        !plan.DoNotKeep)
    {
        return std::nullopt;
    }

    // We should add blobs that are definitely unkept and will be deleted
    // after the selective trim.
    const ui64 unkeptFromCommitId =
        Max(params.LastSentSelectiveTrimToCommitId,
            // Selective trim state is not restored on restart. Hence we do not
            // need to look below the current generation of commits.
            MakeCommitId(params.CurrentGeneration, 0));
    for (auto it =
             UntrimmedFreshBlobsByCommitId.upper_bound(unkeptFromCommitId);
         it != UntrimmedFreshBlobsByCommitId.end() &&
         it->first <= params.OrdinaryTrimToCommitId;
         ++it)
    {
        plan.CollectingCommitIds.push_back(it->first);
    }

    return plan;
}

TFlushFreshBlobResult TPartitionFreshBlobState::FlushFreshBlob(ui64 commitId)
{
    TFlushFreshBlobResult result;
    auto it = UnflushedFreshBlobsByCommitId.find(commitId);
    if (it == UnflushedFreshBlobsByCommitId.end()) {
        // A trim that completed during the flush removed the blob. Trims
        // erase both entries of a blob, so it has no untrimmed entry either.
        STORAGE_VERIFY_C(
            !UntrimmedFreshBlobsByCommitId.contains(commitId),
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId);
        return result;
    }
    result.UnflushedBytesRemoved = EraseUnflushedFreshBlob(it);
    return result;
}

void TPartitionFreshBlobState::EraseUntrimmedFreshBlob(
    TUntrimmedFreshBlobsByCommitId::iterator it)
{
    const auto& [commitId, blob] = *it;

    STORAGE_VERIFY_C(
        UntrimmedFreshBlobByteCount >= blob.ByteCount,
        TWellKnownEntityTypes::TABLET,
        TabletID,
        "UntrimmedFreshBlobByteCount: " << UntrimmedFreshBlobByteCount
                                        << " < BlobSize: " << blob.ByteCount);
    UntrimmedFreshBlobByteCount -= blob.ByteCount;

    if (blob.Dead) {
        STORAGE_VERIFY_C(
            DeadUntrimmedFreshBlobByteCount >= blob.ByteCount,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId << ", DeadUntrimmedFreshBlobByteCount: "
                          << DeadUntrimmedFreshBlobByteCount
                          << " < BlobSize: " << blob.ByteCount);
        DeadUntrimmedFreshBlobByteCount -= blob.ByteCount;
    }
    if (commitId <= TrimmableByOrdinaryToCommitId) {
        STORAGE_VERIFY_C(
            OrdinaryTrimmableFreshBlobByteCount >= blob.ByteCount,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId
                          << ", OrdinaryTrimmableFreshBlobByteCount: "
                          << OrdinaryTrimmableFreshBlobByteCount
                          << " < BlobSize: " << blob.ByteCount);
        OrdinaryTrimmableFreshBlobByteCount -= blob.ByteCount;
    }

    UntrimmedFreshBlobsByCommitId.erase(it);
}

ui64 TPartitionFreshBlobState::EraseUnflushedFreshBlob(
    TUnflushedFreshBlobsByCommitId::iterator it)
{
    const ui64 blobSize = it->second.ByteCount;
    UnflushedFreshBlobCount = SafeDecrement(UnflushedFreshBlobCount, 1);
    UnflushedFreshBlobByteCount =
        SafeDecrement(UnflushedFreshBlobByteCount, blobSize);

    UnflushedFreshBlobsByCommitId.erase(it);

    return blobSize;
}

////////////////////////////////////////////////////////////////////////////////

TPartitionFreshBlocksState::TPartitionFreshBlocksState(
        const TCommitIdsState& commitIdsState,
        const TPartitionFlushState& flushState,
        TPartitionThreadSafeStatePtr threadSafeState)
    : CommitIdsState(commitIdsState)
    , FlushState(flushState)
    , ThreadSafeState(std::move(threadSafeState))
{}

ui32 TPartitionFreshBlocksState::IncrementUnflushedFreshBlocksFromChannelCount(
    size_t value)
{
    UnflushedFreshBlocksFromChannelCount =
        SafeIncrement(UnflushedFreshBlocksFromChannelCount, value);

    return UnflushedFreshBlocksFromChannelCount;
}

ui32 TPartitionFreshBlocksState::DecrementUnflushedFreshBlocksFromChannelCount(
    size_t value)
{
    UnflushedFreshBlocksFromChannelCount =
        SafeDecrement(UnflushedFreshBlocksFromChannelCount, value);

    return UnflushedFreshBlocksFromChannelCount;
}

void TPartitionFreshBlocksState::InitFreshBlocks(
    const TVector<TOwningFreshBlock>& freshBlocks)
{
    for (const auto& freshBlock: freshBlocks) {
        const auto& meta = freshBlock.Meta;

        bool added = Blocks.AddBlock(
            meta.BlockIndex,
            meta.CommitId,
            meta.IsStoredInDb,
            freshBlock.Content,
            freshBlock.BlobId);

        Y_ABORT_UNLESS(
            added,
            "Duplicate block detected: %u @%lu",
            meta.BlockIndex,
            meta.CommitId);
    }
}

void TPartitionFreshBlocksState::FindFreshBlocks(
    IFreshBlocksIndexVisitor& visitor,
    const TBlockRange32& readRange,
    ui64 maxCommitId)
{
    Blocks.FindBlocks(visitor, readRange, maxCommitId);
}

void TPartitionFreshBlocksState::WriteFreshBlocks(
    const TBlockRange32& writeRange,
    ui64 commitId,
    TSgList sglist,
    TPartialBlobId blobId,
    ui64& removedBlocksCount,
    TVector<ui64>& deadCommitIds)
{
    Y_ABORT_UNLESS(writeRange.Size() == sglist.size());

    WriteFreshBlocksImpl(
        writeRange,
        commitId,
        [&](ui32 index) { return sglist[index]; },
        blobId,
        removedBlocksCount,
        deadCommitIds);
}

void TPartitionFreshBlocksState::ZeroFreshBlocks(
    const TBlockRange32& zeroRange,
    ui64 commitId,
    ui64& removedBlocksCount,
    TVector<ui64>& deadCommitIds)
{
    WriteFreshBlocksImpl(
        zeroRange,
        commitId,
        [](ui32) { return TBlockDataRef(); },
        {},  // blobId
        removedBlocksCount,
        deadCommitIds);
}

void TPartitionFreshBlocksState::DeleteFreshBlock(
    ui32 blockIndex,
    ui64 commitId)
{
    bool removed = Blocks.RemoveBlock(
        blockIndex,
        commitId,
        false);   // isStoredInDb

    Y_ABORT_UNLESS(removed);

    DecrementUnflushedFreshBlocksFromChannelCount(1);
}

void TPartitionFreshBlocksState::WriteFreshBlocksImpl(
    const TBlockRange32& writeRange,
    ui64 commitId,
    auto getBlockContent,
    TPartialBlobId blobId,
    ui64& removedBlocksCount,
    TVector<ui64>& deadCommitIds)
{
    TVector<ui64> checkpoints;
    CommitIdsState.GetCheckpointCommitIds(checkpoints);
    ThreadSafeState->GetCheckpointsInFlight()->GetCommitIds(checkpoints);
    // FindGarbageVersions expects checkpoints in descending order.
    SortUnique(checkpoints, TGreater<ui64>());

    TVector<ui64> existingCommitIds;
    TVector<ui64> garbage;

    for (ui32 blockIndex: xrange(writeRange)) {
        ui32 index = blockIndex - writeRange.Start;
        const auto& blockContent = getBlockContent(index);

        Blocks.GetCommitIds(blockIndex, existingCommitIds);

        NCloud::NStorage::FindGarbageVersions(
            checkpoints,
            existingCommitIds,
            garbage);
        for (ui64 garbageCommitId: garbage) {
            // This block is being flushed; we'll remove it on AddBlobs
            // and we'll release barrier on FlushCompleted
            if (FlushState.GetFlushedCommitIdsInProgress().contains(
                    garbageCommitId))
            {
                continue;
            }

            // Do not remove block if it is stored in db
            // to be able to remove it during flush, otherwise
            // we'll leave garbage in FreshBlocksTable
            bool removed = Blocks.RemoveBlock(
                blockIndex,
                garbageCommitId,
                false);   // isStoredInDb

            if (removed) {
                ++removedBlocksCount;
                DecrementUnflushedFreshBlocksFromChannelCount(1);
                if (ThreadSafeState->AccessTrimFreshLogBarriers()
                        ->ReleaseBarrier(garbageCommitId))
                {
                    deadCommitIds.push_back(garbageCommitId);
                }
            }
        }

        Blocks.AddBlock(
            blockIndex,
            commitId,
            false,   // isStoredInDb
            blockContent.AsStringBuf(),
            blobId);

        existingCommitIds.clear();
        garbage.clear();
    }

    IncrementUnflushedFreshBlocksFromChannelCount(writeRange.Size());
}

}   // namespace NCloud::NBlockStore::NStorage
