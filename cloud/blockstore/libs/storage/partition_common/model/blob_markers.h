#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/volume.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>

#include <ydb/core/base/logoblob.h>


namespace NCloud::NBlockStore::NStorage::NBlobMarkers {

////////////////////////////////////////////////////////////////////////////////

struct TUsedMark {};

struct TEmptyMark {};

struct TFreshMark {};

struct TFreshMarkOnBaseDisk {
    std::shared_ptr<NProto::TFreshBlockRange> Data;
    TBlockDataRef RefToData;
    ui64 BlockIndex;

    TFreshMarkOnBaseDisk(
            std::shared_ptr<NProto::TFreshBlockRange> data,
            TBlockDataRef refToData,
            ui64 blockIndex)
        : Data(std::move(data))
        , RefToData(std::move(refToData))
        , BlockIndex(blockIndex)
    {}
};

struct TZeroMark {};

struct TBlobMark
{
    TBlobMark(
            const NKikimr::TLogoBlobID& blobId,
            ui32 bSGroupId,
            ui16 blobOffset)
        : BlobId(blobId)
        , BSGroupId(bSGroupId)
        , BlobOffset(blobOffset)
    {}

    NKikimr::TLogoBlobID BlobId;
    ui32 BSGroupId;
    ui16 BlobOffset;
};

struct TBlobMarkOnBaseDisk {
    NKikimr::TLogoBlobID BlobId;
    ui64 BlockIndex;
    ui32 BSGroupId;
    ui16 BlobOffset;

    TBlobMarkOnBaseDisk(
            const NKikimr::TLogoBlobID& blobId,
            ui64 blockIndex,
            ui32 bSGroupId,
            ui16 blobOffset)
        : BlobId(blobId)
        , BlockIndex(blockIndex)
        , BSGroupId(bSGroupId)
        , BlobOffset(blobOffset)
    {}
};

using TBlockMark = std::variant<
    TEmptyMark,
    TFreshMark,
    TZeroMark,
    TBlobMark,
    TFreshMarkOnBaseDisk,
    TBlobMarkOnBaseDisk,
    TUsedMark>;

using TBlockMarks = TVector<TBlockMark>;

}   // namespace NCloud::NBlockStore::NStorage::NBlobMarkers
