#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TBlobUniqueIdWithRange
{
    ui64 UniqueId = 0;
    TBlockRange32 BlockRange;

    TBlobUniqueIdWithRange() = default;

    TBlobUniqueIdWithRange(ui64 uniqueId, const TBlockRange32& blockRange)
        : UniqueId(uniqueId)
        , BlockRange(blockRange)
    {}
};

using TCommitIdToBlobUniqueIdWithRange =
    THashMap<ui64, TVector<TBlobUniqueIdWithRange>>;

bool Overlaps(
    const TCommitIdToBlobUniqueIdWithRange& blobs,
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
