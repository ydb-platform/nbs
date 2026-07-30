#pragma once

#include "part2_database.h"
#include "part2_tx.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

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

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
