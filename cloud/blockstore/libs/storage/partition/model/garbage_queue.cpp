#include "garbage_queue.h"

#include <util/generic/set.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TGarbageQueue::TImpl
{
    struct TBlobs: TSet<TPartialBlobId>
    {
        bool Add(const TPartialBlobId& blobId)
        {
            return insert(blobId).second;
        }

        bool Remove(const TPartialBlobId& blobId)
        {
            return erase(blobId);
        }

        size_t Count(ui64 maxCommitId) const
        {
            auto end = lower_bound(TPartialBlobId(maxCommitId, Max()));
            return std::distance(begin(), end);
        }

        TVector<TPartialBlobId> Get(ui64 maxCommitId) const
        {
            TVector<TPartialBlobId> result;

            auto end = lower_bound(TPartialBlobId(maxCommitId, Max()));
            for (auto it = begin(); it != end; ++it) {
                result.push_back(*it);
            }

            return result;
        }
    };

    TBlobs NewBlobs;
    TBlobs GarbageBlobs;
};

////////////////////////////////////////////////////////////////////////////////

TGarbageQueue::TGarbageQueue()
    : Impl(new TImpl())
{}

TGarbageQueue::~TGarbageQueue()
{}

////////////////////////////////////////////////////////////////////////////////
// New blobs

bool TGarbageQueue::AddNewBlob(const TPartialBlobId& blobId)
{
    return Impl->NewBlobs.Add(blobId);
}

bool TGarbageQueue::AddNewBlobs(const TVector<TPartialBlobId>& blobIds)
{
    for (const auto& blobId: blobIds) {
        if (!Impl->NewBlobs.Add(blobId)) {
            return false;
        }
    }
    return true;
}

bool TGarbageQueue::RemoveNewBlob(const TPartialBlobId& blobId)
{
    return Impl->NewBlobs.Remove(blobId);
}

size_t TGarbageQueue::GetNewBlobsCount(ui64 maxCommitId) const
{
    return Impl->NewBlobs.Count(maxCommitId);
}

TVector<TPartialBlobId> TGarbageQueue::GetNewBlobs(ui64 maxCommitId) const
{
    return Impl->NewBlobs.Get(maxCommitId);
}

////////////////////////////////////////////////////////////////////////////////
// Garbage blobs

bool TGarbageQueue::AddGarbageBlob(const TPartialBlobId& blobId)
{
    bool result = Impl->GarbageBlobs.Add(blobId);
    if (result) {
        GarbageQueueBytes += blobId.BlobSize();
    }
    return result;
}

bool TGarbageQueue::AddGarbageBlobs(const TVector<TPartialBlobId>& blobIds)
{
    for (const auto& blobId: blobIds) {
        bool result = Impl->GarbageBlobs.Add(blobId);
        if (!result) {
            return false;
        }
        GarbageQueueBytes += blobId.BlobSize();
    }
    return true;
}

bool TGarbageQueue::RemoveGarbageBlob(const TPartialBlobId& blobId)
{
    bool result = Impl->GarbageBlobs.Remove(blobId);
    if (result) {
        GarbageQueueBytes -= blobId.BlobSize();
    }
    return result;
}

size_t TGarbageQueue::GetGarbageBlobsCount(ui64 maxCommitId) const
{
    return Impl->GarbageBlobs.Count(maxCommitId);
}

TVector<TPartialBlobId> TGarbageQueue::GetGarbageBlobs(ui64 maxCommitId) const
{
    return Impl->GarbageBlobs.Get(maxCommitId);
}

ui64 TGarbageQueue::GetGarbageQueueBytes() const
{
    return GarbageQueueBytes;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
