#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TBlobToConfirm
{
    ui64 UniqueId;
    TBlockRange32 BlockRange;
    TVector<ui32> Checksums;
    std::optional<NProto::TCompressionMetadata> CompressionMetadata;

    TBlobToConfirm(
        ui64 uniqueId,
        const TBlockRange32& blockRange,
        const TVector<ui32>& checksums,
        std::optional<NProto::TCompressionMetadata> compressionMetadata)
        : UniqueId(uniqueId)
        , BlockRange(blockRange)
        , Checksums(checksums)
        , CompressionMetadata(std::move(compressionMetadata))
    {}
};

using TCommitIdToBlobsToConfirm = THashMap<ui64, TVector<TBlobToConfirm>>;

bool Overlaps(
    const TCommitIdToBlobsToConfirm& blobs,
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
