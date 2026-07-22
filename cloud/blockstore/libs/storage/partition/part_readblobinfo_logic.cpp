#include "part_readblobinfo_logic.h"

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using TOutputIndex = TTxPartition::TCompactionReadBlobInfo::TOutputIndex;

namespace {

///////////////////////////////////////////////////////////////////////////////

template <typename TCounters>
void ReadBlobInfo(
    TPartitionDatabaseImpl<TCounters>& db,
    const TPartialBlobId& blobId,
    ui64 tabletId,
    const TOutputIndex& outputIndex,
    TVector<TBlockMask>& blockMasks,
    TVector<NProto::TBlobMeta>& blobMetas,
    bool& ready)
{
    TMaybe<TBlockMask> mask;
    TMaybe<NProto::TBlobMeta> meta;
    if (!db.ReadBlobInfo(blobId, mask, meta)) {
        ready = false;
        return;
    }
    STORAGE_VERIFY_C(
        mask.Defined() && meta.Defined(),
        TWellKnownEntityTypes::TABLET,
        tabletId,
        TStringBuilder() << "Could not read blob info for blob: "
                         << MakeBlobId(tabletId, blobId));

    blockMasks[*outputIndex.BlockMaskIndex] = *mask;
    blobMetas[*outputIndex.BlobMetaIndex] = *meta;
}

template <typename TCounters>
void ReadBlobMeta(
    TPartitionDatabaseImpl<TCounters>& db,
    const TPartialBlobId& blobId,
    ui64 tabletId,
    const TOutputIndex& outputIndex,
    TVector<NProto::TBlobMeta>& blobMetas,
    bool& ready)
{
    TMaybe<NProto::TBlobMeta> meta;
    if (!db.ReadBlobMeta(blobId, meta)) {
        ready = false;
        return;
    }
    STORAGE_VERIFY_C(
        meta.Defined(),
        TWellKnownEntityTypes::TABLET,
        tabletId,
        TStringBuilder() << "Could not read blob meta for blob: "
                         << MakeBlobId(tabletId, blobId));
    blobMetas[*outputIndex.BlobMetaIndex] = *meta;
}

template <typename TCounters>
void ReadBlockMask(
    TPartitionDatabaseImpl<TCounters>& db,
    const TPartialBlobId& blobId,
    ui64 tabletId,
    const TOutputIndex& outputIndex,
    TVector<TBlockMask>& blockMasks,
    bool& ready)
{
    TMaybe<TBlockMask> mask;
    if (!db.ReadBlockMask(blobId, mask)) {
        ready = false;
        return;
    }
    STORAGE_VERIFY_C(
        mask.Defined(),
        TWellKnownEntityTypes::TABLET,
        tabletId,
        TStringBuilder() << "Could not read block mask for blob: "
                         << MakeBlobId(tabletId, blobId));

    blockMasks[*outputIndex.BlockMaskIndex] = *mask;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlobId2IndexMap DeduplicateBlobInfos(
    ui64 tabletId,
    const TVector<TPartialBlobId>& blobsToReadBlockMasks,
    const TVector<TPartialBlobId>& blobsToReadBlobMetas)
{
    THashMap<TPartialBlobId, TOutputIndex, TPartialBlobIdHash>
        blobsToOutputIndices;

    for (size_t i = 0; i < blobsToReadBlockMasks.size(); ++i) {
        const auto& blobId = blobsToReadBlockMasks[i];
        auto& indexes = blobsToOutputIndices[blobId];

        STORAGE_VERIFY_C(
            !indexes.BlockMaskIndex,
            TWellKnownEntityTypes::TABLET,
            tabletId,
            "All blobs in blobsToReadBlockMasks must be unique, but "
                << MakeBlobId(tabletId, blobId) << " is duplicated");

        indexes.BlockMaskIndex = i;
    }

    for (size_t i = 0; i < blobsToReadBlobMetas.size(); ++i) {
        const auto& blobId = blobsToReadBlobMetas[i];
        auto& indexes = blobsToOutputIndices[blobId];

        STORAGE_VERIFY_C(
            !indexes.BlobMetaIndex,
            TWellKnownEntityTypes::TABLET,
            tabletId,
            "All blobs in blobsToReadBlobMetas must be unique, but "
                << MakeBlobId(tabletId, blobId) << " is duplicated");

        indexes.BlobMetaIndex = i;
    }

    return blobsToOutputIndices;
}

template <typename TCounters>
bool ReadBlobsInfo(
    TPartitionDatabaseImpl<TCounters>& db,
    const TBlobId2IndexMap& blobsToOutputIndices,
    ui64 tabletId,
    TVector<TBlockMask>& blockMasks,
    TVector<NProto::TBlobMeta>& blobMetas)
{
    bool ready = true;

    for (const auto& [blobId, outputIndex]: blobsToOutputIndices) {
        if (outputIndex.BlobMetaIndex && outputIndex.BlockMaskIndex) {
            ReadBlobInfo(
                db,
                blobId,
                tabletId,
                outputIndex,
                blockMasks,
                blobMetas,
                ready);
        } else if (outputIndex.BlobMetaIndex) {
            ReadBlobMeta(db, blobId, tabletId, outputIndex, blobMetas, ready);
        } else if (outputIndex.BlockMaskIndex) {
            ReadBlockMask(db, blobId, tabletId, outputIndex, blockMasks, ready);
        }
    }

    return ready;
}

////////////////////////////////////////////////////////////////////////////////

template bool ReadBlobsInfo<TNoOpCounter>(
    TPartitionDatabase& db,
    const TBlobId2IndexMap& blobsToOutputIndices,
    ui64 tabletId,
    TVector<TBlockMask>& blockMasks,
    TVector<NProto::TBlobMeta>& blobMetas);

template bool ReadBlobsInfo<TMethodCallCounter>(
    TPartitionDatabaseWithCounters& db,
    const TBlobId2IndexMap& blobsToOutputIndices,
    ui64 tabletId,
    TVector<TBlockMask>& blockMasks,
    TVector<NProto::TBlobMeta>& blobMetas);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
