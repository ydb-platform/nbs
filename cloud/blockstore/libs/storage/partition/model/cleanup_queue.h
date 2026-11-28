#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/partition_common/model/barrier.h>

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
    NProto::TCleanupQueueAdditionalFields AdditionalFields;
};

////////////////////////////////////////////////////////////////////////////////

class TCleanupQueue
    : public TBarriers
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

    const ui64 BlockSize;
    TVector<ui32> MixedChannels;

    ui64 QueueBytes = 0;
    ui64 QueueBlocks = 0;
    ui64 QueueMixedBytes = 0;
    ui64 QueueMixedBlocks = 0;

public:
    explicit TCleanupQueue(ui64 blockSize, TVector<ui32> mixedChannels);
    ~TCleanupQueue();

    //
    // Overwritten blobs
    //

    bool Add(const TCleanupQueueItem& item);
    bool Add(const TVector<TCleanupQueueItem>& items);

    bool Remove(const TCleanupQueueItem& item);

    [[nodiscard]] bool HasBlob(const TPartialBlobId& blobId) const;

    size_t GetCount(ui64 toCommitId = InvalidCommitId) const;

    // Count items after (fromCommitId, fromBlobId) with CommitId <= toCommitId,
    // up to limit.
    size_t GetCount(
        ui64 fromCommitId,
        const TPartialBlobId& fromBlobId,
        ui64 toCommitId,
        size_t limit) const;

    TVector<TCleanupQueueItem> GetItems(
        ui64 toCommitId = InvalidCommitId,
        size_t limit = 100) const;

    // Return items after (fromCommitId, fromBlobId) with CommitId <=
    // toCommitId, up to limit.
    TVector<TCleanupQueueItem> GetItems(
        ui64 fromCommitId,
        const TPartialBlobId& fromBlobId,
        ui64 toCommitId,
        size_t limit) const;

    ui64 GetQueueBytes() const;
    ui64 GetQueueBlocks() const;
    // Both bytes and blocks are classified by channel data kind.
    ui64 GetQueueMixedBytes() const;
    ui64 GetQueueMixedBlocks() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
