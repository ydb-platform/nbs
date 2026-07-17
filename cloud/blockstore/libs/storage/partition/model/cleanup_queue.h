#pragma once

#include "public.h"

#include "barrier.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TCleanupQueueItem
{
    TPartialBlobId BlobId;
    ui64 CommitId = 0;
    NProto::TBlobMeta BlobMeta;
};

////////////////////////////////////////////////////////////////////////////////

class TCleanupQueue
    : public TBarriers
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

    [[nodiscard]] bool HasBlob(const TPartialBlobId& blobId) const;

    size_t GetCount(ui64 maxCommitId = InvalidCommitId) const;

    // Returns cleanup queue items with CommitId <= maxCommitId, up to limit.
    // When cleanup-with-checkpoint is enabled, pass min/max checkpoint commit
    // ids so items that might still be needed by a checkpoint are skipped.
    // Defaults disable checkpoint filtering (minCheckpointCommitId == InvalidCommitId).
    // TODO:_ explain that if checkpoint cleanup is disabled, then maxCommitId must be less than minCheckpointCommitId.
    // TODO:_ add separate method for getting items with checkpoint filtering?
    TVector<TCleanupQueueItem> GetItems(
        ui64 maxCommitId = InvalidCommitId,
        size_t limit = 100,
        ui64 minCheckpointCommitId = InvalidCommitId,
        ui64 maxCheckpointCommitId = 0) const;

    ui64 GetQueueBytes() const;
    ui64 GetQueueBlocks() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
