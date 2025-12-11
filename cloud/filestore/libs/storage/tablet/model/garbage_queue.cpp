#include "garbage_queue.h"

#include "alloc.h"

#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBlobs: TSet<TPartialBlobId, TLess<TPartialBlobId>, TStlAllocator>
{
    using TBase = TSet<TPartialBlobId, TLess<TPartialBlobId>, TStlAllocator>;

    TBlobs(IAllocator* alloc)
        : TBase{{alloc}}
    {}

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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

struct TBarrierOps
{
    struct TComparer
    {
        using is_transparent = void;

        template <typename T1, typename T2>
        bool operator()(const T1& l, const T2& r) const
        {
            return GetCommitId(l) < GetCommitId(r);
        }
    };

    static ui64 GetCommitId(ui64 commitId)
    {
        return commitId;
    }

    static ui64 GetCommitId(const TBarrier& b)
    {
        return b.CommitId;
    }
};

using TBarriers = TSet<TBarrier, TBarrierOps::TComparer>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TGarbageQueue::TImpl
{
    TBlobs NewBlobs;
    TBlobs GarbageBlobs;

    TBarriers Barriers;

    TImpl(IAllocator* alloc)
        : NewBlobs(alloc)
        , GarbageBlobs(alloc)
    {}
};

////////////////////////////////////////////////////////////////////////////////

TGarbageQueue::TGarbageQueue(IAllocator* alloc)
    : Impl(new TImpl(alloc))
{}

TGarbageQueue::~TGarbageQueue()
{}

////////////////////////////////////////////////////////////////////////////////
// New blobs

bool TGarbageQueue::AddNewBlob(const TPartialBlobId& blobId)
{
    return Impl->NewBlobs.Add(blobId);
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
        auto& barrier = const_cast<TBarrier&>(*Impl->Barriers.rbegin());
        barrier.GarbageBlobs.push_back(blobId);
        return true;
    }

    return Impl->GarbageBlobs.Add(blobId);
}

bool TGarbageQueue::RemoveGarbageBlob(const TPartialBlobId& blobId)
{
    return Impl->GarbageBlobs.Remove(blobId);
}

size_t TGarbageQueue::GetGarbageBlobsCount(ui64 maxCommitId) const
{
    return Impl->GarbageBlobs.Count(maxCommitId);
}

TVector<TPartialBlobId> TGarbageQueue::GetGarbageBlobs(ui64 maxCommitId) const
{
    return Impl->GarbageBlobs.Get(maxCommitId);
}

////////////////////////////////////////////////////////////////////////////////
// GC barriers

void TGarbageQueue::AcquireCollectBarrier(ui64 commitId)
{
    auto [it, inserted] = Impl->Barriers.emplace(commitId);

    if (!inserted) {
        auto& barrier = const_cast<TBarrier&>(*it);
        ++barrier.RefCount;
    }
}

bool TGarbageQueue::TryReleaseCollectBarrier(ui64 commitId)
{
    {
        auto it = Impl->Barriers.find(commitId);
        if (it == Impl->Barriers.end()) {
            return false;
        }

        auto& barrier = const_cast<TBarrier&>(*it);

        if (barrier.RefCount == 0) {
            return false;
        }
        --barrier.RefCount;
    }

    // we have to release barriers in order
    for (auto it = Impl->Barriers.begin(); it != Impl->Barriers.end();) {
        auto& barrier = const_cast<TBarrier&>(*it);
        if (barrier.RefCount) {
            break;
        }

        // now we can safely collect garbage
        for (const auto& blobId: barrier.GarbageBlobs) {
            Impl->GarbageBlobs.Add(blobId);
        }

        it = Impl->Barriers.erase(it);
    }
    return true;
}

bool TGarbageQueue::IsCollectBarrierAcquired(ui64 commitId) const
{
    auto it = Impl->Barriers.find(commitId);
    return it != Impl->Barriers.end() && it->RefCount > 0;
}

ui64 TGarbageQueue::GetCollectCommitId() const
{
    if (Impl->Barriers) {
        const auto& barrier = *Impl->Barriers.begin();
        return barrier.CommitId - 1;
    }

    return InvalidCommitId;
}

}   // namespace NCloud::NFileStore::NStorage
