#pragma once

#include "part_database.h"
#include "part_tx.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

using TBlobId2IndexMap = THashMap<
    TPartialBlobId,
    TTxPartition::TCompactionReadBlobInfo::TOutputIndex,
    TPartialBlobIdHash>;

TBlobId2IndexMap DeduplicateBlobInfos(
    ui64 tabletId,
    const TVector<TPartialBlobId>& blobsToReadBlockMasks,
    const TVector<TPartialBlobId>& blobsToReadBlobMetas);

template <typename TCounters>
bool ReadBlobsInfo(
    TPartitionDatabaseImpl<TCounters>& db,
    const TBlobId2IndexMap& blobsToOutputIndices,
    ui64 tabletId,
    TVector<TBlockMask>& blockMasks,
    TVector<NProto::TBlobMeta>& blobMetas);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
