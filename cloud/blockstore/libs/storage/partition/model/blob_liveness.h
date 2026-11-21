#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TBlockVersion
{
    ui32 BlockIndex = 0;
    ui64 CommitId = 0;
    TPartialBlobId BlobId;
};

void AppendBlobBlocks(const NProto::TBlobMeta& blobMeta, TVector<ui32>& blocks);

using TLiveBlobs = THashSet<TPartialBlobId, TPartialBlobIdHash>;

// A blob is live if it holds the newest version of at least one block at
// commitId or at one of the checkpoints.
TLiveBlobs FindLiveBlobs(
    TVector<TBlockVersion> versions,
    ui64 commitId,
    TVector<ui64> checkpointCommitIds);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
