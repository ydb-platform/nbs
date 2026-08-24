#include "part_fresh_blocks_state.h"

#include <cloud/blockstore/libs/storage/partition/model/operation_status.h>

#include <cloud/storage/core/libs/tablet/model/channels.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NPartition;

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
    for (const auto& [blobCommitId, blobSize]:
         UnflushedFreshBlobByteCountByCommitId)
    {
        if (blobCommitId > commitId) {
            break;
        }
        commitIds.push_back(blobCommitId);
    }
    return commitIds;
}

void TPartitionFreshBlobState::AddFreshBlob(
    ui64 commitId,
    ui64 blobSize,
    ui64 zeroBlockCount)
{
    {
        const bool inserted =
            UntrimmedFreshBlobByteCountByCommitId.insert({commitId, blobSize})
                .second;
        STORAGE_VERIFY_C(
            inserted,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId);
        UntrimmedFreshBlobByteCount += blobSize;
    }

    {
        const bool inserted =
            UnflushedFreshBlobByteCountByCommitId.insert({commitId, blobSize})
                .second;
        STORAGE_VERIFY_C(
            inserted,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId);
        UnflushedFreshBlobCount = SafeIncrement(UnflushedFreshBlobCount, 1);
        UnflushedFreshBlobByteCount =
            SafeIncrement(UnflushedFreshBlobByteCount, blobSize);
    }

    if (zeroBlockCount) {
        const bool inserted =
            UnflushedFreshZeroBlockCountByCommitId
                .insert({commitId, zeroBlockCount})
                .second;
        STORAGE_VERIFY_C(
            inserted,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Commit id: " << commitId);
        UnflushedFreshZeroBlockCount =
            SafeIncrement(UnflushedFreshZeroBlockCount, zeroBlockCount);
    }
}

void TPartitionFreshBlobState::TrimFreshBlobs(ui64 commitId)
{
    auto& blobs = UntrimmedFreshBlobByteCountByCommitId;

    while (blobs && blobs.begin()->first <= commitId) {
        auto blobSize = blobs.begin()->second;
        STORAGE_VERIFY_C(
            UntrimmedFreshBlobByteCount >= blobSize,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "UntrimmedFreshBlobByteCount: " << UntrimmedFreshBlobByteCount
                                            << " < BlobSize: " << blobSize);
        UntrimmedFreshBlobByteCount -= blobSize;
        blobs.erase(blobs.begin());
    }
}

TPartitionFreshBlobState::TFlushedFreshBlob
TPartitionFreshBlobState::FlushFreshBlob(ui64 commitId)
{
    auto& blobs = UnflushedFreshBlobByteCountByCommitId;

    auto it = blobs.find(commitId);
    STORAGE_VERIFY_C(
        it != blobs.end(),
        TWellKnownEntityTypes::TABLET,
        TabletID,
        "Commit id: " << commitId);

    TFlushedFreshBlob flushed;
    flushed.ByteCount = it->second;

    UnflushedFreshBlobCount = SafeDecrement(UnflushedFreshBlobCount, 1);
    UnflushedFreshBlobByteCount =
        SafeDecrement(UnflushedFreshBlobByteCount, flushed.ByteCount);

    blobs.erase(it);

    auto zeroIt = UnflushedFreshZeroBlockCountByCommitId.find(commitId);
    if (zeroIt != UnflushedFreshZeroBlockCountByCommitId.end()) {
        flushed.ZeroBlockCount = zeroIt->second;
        UnflushedFreshZeroBlockCount = SafeDecrement(
            UnflushedFreshZeroBlockCount,
            flushed.ZeroBlockCount);
        UnflushedFreshZeroBlockCountByCommitId.erase(zeroIt);
    }

    return flushed;
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
    TPartialBlobId blobId)
{
    Y_ABORT_UNLESS(writeRange.Size() == sglist.size());

    WriteFreshBlocksImpl(
        writeRange,
        commitId,
        [&](ui32 index) { return sglist[index]; },
        blobId);
}

void TPartitionFreshBlocksState::ZeroFreshBlocks(
    const TBlockRange32& zeroRange,
    ui64 commitId)
{
    WriteFreshBlocksImpl(
        zeroRange,
        commitId,
        [](ui32) { return TBlockDataRef(); },
        {}  // blobId
    );
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
    TPartialBlobId blobId)
{
    TVector<ui64> checkpoints;
    CommitIdsState.GetCheckpointCommitIds(checkpoints);
    ThreadSafeState->GetCheckpointsInFlight()->GetCommitIds(checkpoints);
    SortUnique(checkpoints);

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
        for (auto garbageCommitId: garbage) {
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
            auto removed = Blocks.RemoveBlock(
                blockIndex,
                garbageCommitId,
                false);   // isStoredInDb

            if (removed) {
                DecrementUnflushedFreshBlocksFromChannelCount(1);
                ThreadSafeState->AccessTrimFreshLogBarriers()->ReleaseBarrier(
                    garbageCommitId);
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
