#pragma once

#include "public.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

class TGarbageQueue
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

    ui64 GarbageQueueBytes = 0;

public:
    TGarbageQueue();
    ~TGarbageQueue();

    //
    // New blobs
    //

    bool AddNewBlob(const TPartialBlobId& blobId);
    bool AddNewBlobs(const TVector<TPartialBlobId>& blobIds);

    bool RemoveNewBlob(const TPartialBlobId& blobId);

    size_t GetNewBlobsCount(ui64 maxCommitId = InvalidCommitId) const;
    TVector<TPartialBlobId> GetNewBlobs(
        ui64 maxCommitId = InvalidCommitId) const;

    //
    // Garbage blobs
    //

    bool AddGarbageBlob(const TPartialBlobId& blobId);
    bool AddGarbageBlobs(const TVector<TPartialBlobId>& blobIds);

    bool RemoveGarbageBlob(const TPartialBlobId& blobId);

    size_t GetGarbageBlobsCount(ui64 maxCommitId = InvalidCommitId) const;
    TVector<TPartialBlobId> GetGarbageBlobs(
        ui64 maxCommitId = InvalidCommitId) const;
    ui64 GetGarbageQueueBytes() const;

    //
    // GC barriers
    //

    void AcquireCollectBarrier(ui64 commitId);
    void ReleaseCollectBarrier(ui64 commitId);

    ui64 GetCollectCommitId() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
