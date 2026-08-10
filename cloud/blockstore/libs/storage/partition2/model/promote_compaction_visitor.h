#pragma once

#include <cloud/blockstore/libs/storage/partition_common/model/block.h>
#include <cloud/blockstore/libs/storage/partition_common/model/block_index.h>
#include <cloud/blockstore/libs/storage/model/public.h>

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

class TPromoteCompactionVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
{
public:
    struct TFreshBlockMark
    {
        TPartialBlobId BlobId;
        TStringBuf Content;
    };

    struct TBlobBlockMark
    {
        TPartialBlobId BlobId;
        ui16 BlobOffset = 0;
    };

    struct TBlockMark
    {
        ui64 CommitId = 0;
        std::variant<TFreshBlockMark, TBlobBlockMark> IndexSpecificMark;
    };

    struct TBlob
    {
        TVector<std::pair<ui64, TBlockMark>> BlockIndexToMark;
        TBlockBuffer BlobContent;
    };

    struct TReadBlobRequest
    {
        TPartialBlobId BlobId;
        TVector<ui16> BlobOffsets;
        TGuardedSgList Sglist;
    };

private:
    const ui32 BlockSize;
    const ui64 TargetRangeBlocksCount;
    const ui32 MaxBlocksInBlob;
    const bool AllowBlockDuplicates;

    TMap<ui64, TMap<ui64, TVector<TBlockMark>>> BlocksPerRange;

public:
    explicit TPromoteCompactionVisitor(
        ui64 targetRangeBlocksCount,
        ui32 blockSize,
        ui32 maxBlocksInBlob,
        bool allowBlockDuplicates);

    bool Visit(const TFreshBlock& block) override;

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override;

    TVector<TBlob> Finish();

    // The request sglists point into BlobContent, so blobs must outlive the
    // returned requests.
    static TVector<TReadBlobRequest> CollectReadBlobRequests(
        TVector<TBlob>& blobs);

private:
    void AddBlockMark(ui32 blockIndex, TBlockMark mark);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
