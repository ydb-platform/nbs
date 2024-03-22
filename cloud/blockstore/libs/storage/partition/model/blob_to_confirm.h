#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TBlobToConfirm
{
    ui64 UniqueId = 0;
    TBlockRange32 BlockRange;

    TBlobToConfirm() = default;

    TBlobToConfirm(ui64 uniqueId, const TBlockRange32& blockRange)
        : UniqueId(uniqueId)
        , BlockRange(blockRange)
    {}
};

using TCommitIdToBlobsToConfirm = THashMap<ui64, TVector<TBlobToConfirm>>;

bool Overlaps(
    const TCommitIdToBlobsToConfirm& blobs,
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
