#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TUnconfirmedBlob
{
    ui64 UniqueId = 0;
    TBlockRange32 BlockRange;

    TUnconfirmedBlob() = default;

    TUnconfirmedBlob(ui64 uniqueId, const TBlockRange32& blockRange)
        : UniqueId(uniqueId)
        , BlockRange(blockRange)
    {}
};

// mapping from CommitId
using TUnconfirmedBlobs = THashMap<ui64, TVector<TUnconfirmedBlob>>;
using TConfirmedBlobs = THashMap<ui64, TVector<TUnconfirmedBlob>>;

bool Overlaps(
    const TUnconfirmedBlobs& blobs,
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange);

bool Overlaps(
    const TUnconfirmedBlobs& blobs,
    ui64 commitId,
    const TBlockRange32& blockRange);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
