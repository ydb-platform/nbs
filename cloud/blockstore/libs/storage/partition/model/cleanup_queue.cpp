#include "cleanup_queue.h"

#include <util/generic/set.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage::NPartition {

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
            Items.insert(item);
        }

        return result;
    }

    bool Remove(const TCleanupQueueItem& item)
    {
        auto itBlob = BlobIds.find(item.BlobId);
        if (itBlob == BlobIds.end()) {
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

    TVector<TCleanupQueueItem> GetItems(ui64 maxCommitId, size_t limit) const
    {
        TVector<TCleanupQueueItem> result;
        for (const auto& item: Items) {
            if (item.CommitId > maxCommitId) {
                break;
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

TVector<TCleanupQueueItem> TCleanupQueue::GetItems(ui64 maxCommitId, size_t limit) const
{
    return Impl->GetItems(maxCommitId, limit);
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
