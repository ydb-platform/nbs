#pragma once

#include "part_database.h"
#include "part_tx.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

THashMap<
    TPartialBlobId,
    TTxPartition::TCompactionReadBlobInfo::TOutputIndex,
    TPartialBlobIdHash>
DeduplicateBlobInfos(
    ui64 tabletId,
    const TVector<TPartialBlobId>& blobsToReadBlockMasks,
    const TVector<TPartialBlobId>& blobsToReadBlobMetas);

bool ReadBlobsInfo(
    TPartitionDatabase& db,
    const THashMap<
        TPartialBlobId,
        TTxPartition::TCompactionReadBlobInfo::TOutputIndex,
        TPartialBlobIdHash>& blobsToOutputIndices,
    ui64 tabletId,
    TVector<TBlockMask>& blockMasks,
    TVector<NProto::TBlobMeta>& blobMetas);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
