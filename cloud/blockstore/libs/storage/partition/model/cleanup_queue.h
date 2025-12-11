#pragma once

#include "public.h"

#include "barrier.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TCleanupQueueItem
{
    TPartialBlobId BlobId;
    ui64 CommitId = 0;

    TCleanupQueueItem() = default;

    TCleanupQueueItem(const TPartialBlobId& blobId, ui64 commitId)
        : BlobId(blobId)
        , CommitId(commitId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TCleanupQueue: public TBarriers
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

    const ui64 BlockSize;

    ui64 QueueBytes = 0;
    ui64 QueueBlocks = 0;

public:
    explicit TCleanupQueue(ui64 blockSize);
    ~TCleanupQueue();

    //
    // Overwritten blobs
    //

    bool Add(const TCleanupQueueItem& item);
    bool Add(const TVector<TCleanupQueueItem>& items);

    bool Remove(const TCleanupQueueItem& item);

    size_t GetCount(ui64 maxCommitId = InvalidCommitId) const;

    TVector<TCleanupQueueItem> GetItems(
        ui64 maxCommitId = InvalidCommitId,
        size_t limit = 100) const;

    ui64 GetQueueBytes() const;
    ui64 GetQueueBlocks() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
