#include "garbage_queue.h"

#include <util/generic/set.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct TGarbageQueue::TImpl
{
    struct TBlobs: TSet<TPartialBlobId>
    {
        bool Add(const TPartialBlobId& blobId)
        {
            Y_ABORT_UNLESS(!IsDeletionMarker(blobId));
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

    struct TBarrierComparer
    {
        using is_transparent = void;

        template <typename T1, typename T2>
        bool operator()(const T1& l, const T2& r) const
        {
            return GetCommitId(l) < GetCommitId(r);
        }
    };

    struct TBarrier
    {
        const ui64 CommitId;

        size_t RefCount;
        TVector<TPartialBlobId> GarbageBlobs;

        explicit TBarrier(ui64 commitId)
            : CommitId(commitId)
            , RefCount(1)
        {}
    };

    static ui64 GetCommitId(ui64 commitId)
    {
        return commitId;
    }

    static ui64 GetCommitId(const TBarrier& b)
    {
        return b.CommitId;
    }

    using TBarriers = TSet<TBarrier, TBarrierComparer>;
    TBarriers Barriers;
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
    if (Impl->Barriers) {
        // garbage will be collected when this barrier is released
        auto& barrier = const_cast<TImpl::TBarrier&>(*Impl->Barriers.rbegin());
        barrier.GarbageBlobs.push_back(blobId);
        GarbageQueueBytes += blobId.BlobSize();
        return true;
    }

    bool result = Impl->GarbageBlobs.Add(blobId);
    if (result) {
        GarbageQueueBytes += blobId.BlobSize();
    }
    return result;
}

bool TGarbageQueue::AddGarbageBlobs(const TVector<TPartialBlobId>& blobIds)
{
    for (const auto& blobId: blobIds) {
        if (!Impl->GarbageBlobs.Add(blobId)) {
            return false;
        }
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

////////////////////////////////////////////////////////////////////////////////
// GC barriers

void TGarbageQueue::AcquireCollectBarrier(ui64 commitId)
{
    TImpl::TBarriers::iterator it;
    bool inserted;
    std::tie(it, inserted) = Impl->Barriers.emplace(commitId);

    if (!inserted) {
        auto& barrier = const_cast<TImpl::TBarrier&>(*it);
        ++barrier.RefCount;
    }
}

void TGarbageQueue::ReleaseCollectBarrier(ui64 commitId)
{
    {
        auto it = Impl->Barriers.find(commitId);
        Y_ABORT_UNLESS(it != Impl->Barriers.end());

        auto& barrier = const_cast<TImpl::TBarrier&>(*it);

        Y_ABORT_UNLESS(barrier.RefCount > 0);
        --barrier.RefCount;
    }

    // we have to release barriers in order
    for (auto it = Impl->Barriers.begin(); it != Impl->Barriers.end();) {
        auto& barrier = const_cast<TImpl::TBarrier&>(*it);
        if (barrier.RefCount) {
            break;
        }

        // now we can safely collect garbage
        for (const auto& blobId: barrier.GarbageBlobs) {
            Impl->GarbageBlobs.Add(blobId);
        }

        it = Impl->Barriers.erase(it);
    }
}

ui64 TGarbageQueue::GetCollectCommitId() const
{
    if (Impl->Barriers) {
        const auto& barrier = *Impl->Barriers.begin();
        return barrier.CommitId - 1;
    }

    return InvalidCommitId;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
