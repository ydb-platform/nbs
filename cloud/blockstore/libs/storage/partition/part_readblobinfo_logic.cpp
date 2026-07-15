#include "part_readblobinfo_logic.h"

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

bool ReadBlobsInfo(
    TPartitionDatabase& db,
    const TVector<TPartialBlobId>& blobsToReadBlockMasks,
    const TVector<TPartialBlobId>& blobsToReadBlobMetas,
    ui64 tabletId,
    TVector<TBlockMask>& blockMasks,
    TVector<NProto::TBlobMeta>& blobMetas)
{
    bool ready = true;

    blockMasks.resize(blobsToReadBlockMasks.size());
    for (size_t i = 0; i < blobsToReadBlockMasks.size(); ++i) {
        const auto& blobId = blobsToReadBlockMasks[i];
        TMaybe<TBlockMask> mask;
        if (!db.ReadBlockMask(blobId, mask)) {
            ready = false;
            continue;
        }
        STORAGE_VERIFY_C(
            mask.Defined(),
            TWellKnownEntityTypes::TABLET,
            tabletId,
            TStringBuilder() << "Could not read block mask for blob: "
                             << MakeBlobId(tabletId, blobId));
        blockMasks[i] = *mask;
    }

    blobMetas.resize(blobsToReadBlobMetas.size());
    for (size_t i = 0; i < blobsToReadBlobMetas.size(); ++i) {
        const auto& blobId = blobsToReadBlobMetas[i];
        TMaybe<NProto::TBlobMeta> meta;
        if (!db.ReadBlobMeta(blobId, meta)) {
            ready = false;
            continue;
        }
        STORAGE_VERIFY_C(
            meta.Defined(),
            TWellKnownEntityTypes::TABLET,
            tabletId,
            TStringBuilder() << "Could not read blob meta for blob: "
                             << MakeBlobId(tabletId, blobId));
        blobMetas[i] = std::move(*meta);
    }

    return ready;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
