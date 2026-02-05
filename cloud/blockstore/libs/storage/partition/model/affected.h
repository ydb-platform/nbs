#pragma once

#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TAffectedBlob
{
    TVector<ui16> Offsets;
    TMaybe<TBlockMask> BlockMask;
    TVector<ui32> AffectedBlockIndices;

    // Filled only if a flag is set. BlobMeta is needed only to do some extra
    // consistency checks.
    TMaybe<NProto::TBlobMeta> BlobMeta;
};

using TAffectedBlobs = THashMap<TPartialBlobId, TAffectedBlob, TPartialBlobIdHash>;

////////////////////////////////////////////////////////////////////////////////

struct TAffectedBlock
{
    ui32 BlockIndex = 0;
    ui64 CommitId = 0;
};

using TAffectedBlocks = TVector<TAffectedBlock>;

}   // namespace NCloud::NBlockStore::NStorage::NPartition
