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
    TVector<ui32> Checksums;

    TBlobToConfirm() = default;

    TBlobToConfirm(ui64 uniqueId, const TBlockRange32& blockRange)
        : UniqueId(uniqueId)
        , BlockRange(blockRange)
    {}

    TBlobToConfirm(
            ui64 uniqueId,
            const TBlockRange32& blockRange,
            const TVector<ui32>& checksums)
        : UniqueId(uniqueId)
        , BlockRange(blockRange)
        , Checksums(checksums)
    {}

};

using TCommitIdToBlobsToConfirm = THashMap<ui64, TVector<TBlobToConfirm>>;

bool Overlaps(
    const TCommitIdToBlobsToConfirm& blobs,
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
