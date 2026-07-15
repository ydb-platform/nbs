#pragma once

#include "part_database.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

bool ReadBlobsInfo(
    TPartitionDatabase& db,
    const TVector<TPartialBlobId>& blobsToReadBlockMasks,
    const TVector<TPartialBlobId>& blobsToReadBlobMetas,
    ui64 tabletId,
    TVector<TBlockMask>& blockMasks,
    TVector<NProto::TBlobMeta>& blobMetas);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
