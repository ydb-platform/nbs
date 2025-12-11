#pragma once

#include "public.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TGarbageQueue
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TGarbageQueue(IAllocator* alloc);
    ~TGarbageQueue();

    //
    // New blobs
    //

    bool AddNewBlob(const TPartialBlobId& blobId);
    bool RemoveNewBlob(const TPartialBlobId& blobId);

    size_t GetNewBlobsCount(ui64 maxCommitId = InvalidCommitId) const;
    TVector<TPartialBlobId> GetNewBlobs(
        ui64 maxCommitId = InvalidCommitId) const;

    //
    // Garbage blobs
    //

    bool AddGarbageBlob(const TPartialBlobId& blobId);
    bool RemoveGarbageBlob(const TPartialBlobId& blobId);

    size_t GetGarbageBlobsCount(ui64 maxCommitId = InvalidCommitId) const;
    TVector<TPartialBlobId> GetGarbageBlobs(
        ui64 maxCommitId = InvalidCommitId) const;

    //
    // GC barriers
    //

    void AcquireCollectBarrier(ui64 commitId);
    [[nodiscard]] bool TryReleaseCollectBarrier(ui64 commitId);
    bool IsCollectBarrierAcquired(ui64 commitId) const;

    ui64 GetCollectCommitId() const;
};

}   // namespace NCloud::NFileStore::NStorage
