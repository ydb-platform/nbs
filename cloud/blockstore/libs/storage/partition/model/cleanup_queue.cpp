#include "cleanup_queue.h"

#include <util/generic/set.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Returns true if the blob might still be needed by a checkpoint and must not
// be cleaned up.
bool ShouldSkipCleanupDueToCheckpoint(
    const TCleanupQueueItem& item,
    ui64 minCheckpointCommitId,
    ui64 maxCheckpointCommitId)
{
    if (maxCheckpointCommitId == InvalidCommitId || minCheckpointCommitId == InvalidCommitId) {
        // std::cerr << "ShouldSkipCleanupDueToCheckpoint: invalid commit ids" << std::endl;
        return false;
    }

    // std::cerr << "ShouldSkipCleanupDueToCheckpoint: item.CommitId=" << item.CommitId << " minCheckpointCommitId=" << minCheckpointCommitId << " maxCheckpointCommitId=" << maxCheckpointCommitId << std::endl;

    if (item.CommitId < minCheckpointCommitId) {
        // Blob was added to the cleanup queue before any checkpoint.
        // std::cerr << "ShouldSkipCleanupDueToCheckpoint: blob was added to the cleanup queue before any checkpoint" << std::endl;
        return false;
    }

    const auto& blobMeta = item.BlobMeta;

    ui64 blobCommitId = Max<ui64>();
    if (blobMeta.HasMixedBlocks()) {
        const auto& mixedBlocks = blobMeta.GetMixedBlocks();
        if (mixedBlocks.CommitIdsSize() == 0) {
            // every block shares the same commitId
            blobCommitId = item.BlobId.CommitId();
        } else {
            // each block has its own commitId
            Y_ABORT_UNLESS(mixedBlocks.BlocksSize() == mixedBlocks.CommitIdsSize());
            for (ui64 commitId: mixedBlocks.GetCommitIds()) {
                blobCommitId = Min(blobCommitId, commitId);
            }
        }
    } else if (blobMeta.HasMergedBlocks()) {
        blobCommitId = item.BlobId.CommitId();
    } else {
        // TODO:_ is this a valid case? Seems yes.
        // std::cerr << "ShouldSkipCleanupDueToCheckpoint: blob has no mixed or merged blocks" << std::endl;
        return false;
    }

    // std::cerr << "ShouldSkipCleanupDueToCheckpoint: blobCommitId=" << blobCommitId << std::endl;
    return blobCommitId <= maxCheckpointCommitId;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TCleanupQueue::TImpl
{
    struct TLess
    {
        bool operator ()(const TCleanupQueueItem& l, const TCleanupQueueItem& r) const
        {
            return std::forward_as_tuple(l.CommitId, l.BlobId)
                <  std::forward_as_tuple(r.CommitId, r.BlobId);
        }
    };

    TSet<TCleanupQueueItem, TLess> Items;
    THashSet<TPartialBlobId, TPartialBlobIdHash> BlobIds;

    bool Add(const TCleanupQueueItem& item)
    {
        bool result = BlobIds.insert(item.BlobId).second;
        if (result) {
            auto inserted = Items.insert(item).second;
            Y_DEBUG_ABORT_UNLESS(inserted);
        }

        return result;
    }

    bool Remove(const TCleanupQueueItem& item)
    {
        auto itBlob = BlobIds.find(item.BlobId);
        if (itBlob == BlobIds.end()) {
            Y_DEBUG_ABORT_UNLESS(!Items.contains(item));
            return false;
        }

        auto itItem = Items.find(item);
        if (itItem == Items.end()) {
            return false;
        }

        BlobIds.erase(itBlob);
        Items.erase(itItem);
        return true;
    }

    bool HasBlob(const TPartialBlobId& blobId) const
    {
        return BlobIds.contains(blobId);
    }

    size_t GetCount(ui64 maxCommitId) const
    {
        if (maxCommitId == InvalidCommitId) {
            return Items.size();
        }
        size_t result = 0;
        for (const auto& item: Items) {
            if (item.CommitId > maxCommitId) {
                break;
            }
            ++result;
        }
        return result;
    }

    // TODO:_ add separate method for getting items with checkpoint filtering?
    TVector<TCleanupQueueItem> GetItems(
        ui64 maxCommitId,
        size_t limit,
        ui64 minCheckpointCommitId,
        ui64 maxCheckpointCommitId) const
    {
        TVector<TCleanupQueueItem> result;
        for (const auto& item: Items) {
            if (item.CommitId > maxCommitId) {
                break;
            }
            if (ShouldSkipCleanupDueToCheckpoint(
                    item,
                    minCheckpointCommitId,
                    maxCheckpointCommitId))
            {
                continue;
            }
            result.emplace_back(item);
            if (result.size() == limit) {
                break;
            }
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

TCleanupQueue::TCleanupQueue(ui64 blockSize)
    : Impl(new TImpl())
    , BlockSize(blockSize)
{}

TCleanupQueue::~TCleanupQueue()
{}

bool TCleanupQueue::Add(const TCleanupQueueItem& item)
{
    bool result = Impl->Add(item);
    if (result) {
        QueueBytes += item.BlobId.BlobSize();
        QueueBlocks += item.BlobId.BlobSize() / BlockSize;
    }
    return result;
}

bool TCleanupQueue::Add(const TVector<TCleanupQueueItem>& items)
{
    for (const auto& item: items) {
        bool result = Impl->Add(item);
        if (!result) {
            return false;
        }
        QueueBytes += item.BlobId.BlobSize();
        QueueBlocks += item.BlobId.BlobSize() / BlockSize;
    }
    return true;
}

bool TCleanupQueue::Remove(const TCleanupQueueItem& item)
{
    bool result = Impl->Remove(item);
    if (result) {
        QueueBytes -= item.BlobId.BlobSize();
        QueueBlocks -= item.BlobId.BlobSize() / BlockSize;
    }
    return result;
}

bool TCleanupQueue::HasBlob(const TPartialBlobId& blobId) const
{
    return Impl->HasBlob(blobId);
}

size_t TCleanupQueue::GetCount(ui64 maxCommitId) const
{
    return Impl->GetCount(maxCommitId);
}

TVector<TCleanupQueueItem> TCleanupQueue::GetItems(
    ui64 maxCommitId,
    size_t limit,
    ui64 minCheckpointCommitId,
    ui64 maxCheckpointCommitId) const
{
    return Impl->GetItems(
        maxCommitId,
        limit,
        minCheckpointCommitId,
        maxCheckpointCommitId);
}

ui64 TCleanupQueue::GetQueueBytes() const
{
    return QueueBytes;
}

ui64 TCleanupQueue::GetQueueBlocks() const
{
    return QueueBlocks;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
