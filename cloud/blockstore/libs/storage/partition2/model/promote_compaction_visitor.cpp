#include "promote_compaction_visitor.h"

#include <util/generic/strbuf.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

TPromoteCompactionVisitor::TPromoteCompactionVisitor(
    ui64 targetRangeBlocksCount,
    ui32 blockSize,
    ui32 maxBlocksInBlob,
    bool allowBlockDuplicates)
    : BlockSize(blockSize)
    , TargetRangeBlocksCount(targetRangeBlocksCount)
    , MaxBlocksInBlob(maxBlocksInBlob)
    , AllowBlockDuplicates(allowBlockDuplicates)
{
    Y_ABORT_UNLESS(BlockSize);
    Y_ABORT_UNLESS(TargetRangeBlocksCount);
    Y_ABORT_UNLESS(MaxBlocksInBlob);
}

bool TPromoteCompactionVisitor::Visit(const TFreshBlock& block)
{
    auto mark = TBlockMark{
        .CommitId = block.Meta.CommitId,
        .IndexSpecificMark =
            TFreshBlockMark{.BlobId = block.BlobId, .Content = block.Content}};

    AddBlockMark(block.Meta.BlockIndex, std::move(mark));
    return true;
}

bool TPromoteCompactionVisitor::Visit(
    ui32 blockIndex,
    ui64 commitId,
    const TPartialBlobId& blobId,
    ui16 blobOffset)
{
    auto mark = TBlockMark{
        .CommitId = commitId,
        .IndexSpecificMark =
            TBlobBlockMark{.BlobId = blobId, .BlobOffset = blobOffset}};

    AddBlockMark(blockIndex, std::move(mark));

    return true;
}

auto TPromoteCompactionVisitor::Finish() -> TVector<TBlob>
{
    TVector<TBlob> blobs;
    for (auto& [targetRangeIndex, blocksForRange]: BlocksPerRange) {
        Y_UNUSED(targetRangeIndex);
        bool firstBlockInRange = true;

        for (auto& [blockIndex, marksForBlock]: blocksForRange) {
            Sort(
                marksForBlock,
                [](const auto& a, const auto& b)
                { return a.CommitId < b.CommitId; });

            for (const auto& mark: marksForBlock) {
                if (firstBlockInRange ||
                    blobs.back().BlockIndexToMark.size() >= MaxBlocksInBlob)
                {
                    blobs.emplace_back();
                    firstBlockInRange = false;
                }

                auto& currentBlob = blobs.back();

                if (std::holds_alternative<TFreshBlockMark>(
                        mark.IndexSpecificMark))
                {
                    const auto& freshBlockMark =
                        std::get<TFreshBlockMark>(mark.IndexSpecificMark);

                    if (freshBlockMark.Content.empty()) {
                        currentBlob.BlobContent.AddBlock(BlockSize, char{0});
                    } else {
                        currentBlob.BlobContent.AddBlock(
                            {freshBlockMark.Content.data(),
                             freshBlockMark.Content.size()});
                    }
                } else if (
                    std::holds_alternative<TBlobBlockMark>(
                        mark.IndexSpecificMark))
                {
                    currentBlob.BlobContent.AddBlock(BlockSize, char{0});
                }

                currentBlob.BlockIndexToMark.emplace_back(blockIndex, mark);
            }
        }
    }

    return blobs;
}

auto TPromoteCompactionVisitor::CollectReadBlobRequests(TVector<TBlob>& blobs)
    -> TVector<TReadBlobRequest>
{
    struct TRequestData
    {
        TVector<ui16> BlobOffsets;
        TSgList Sglist;
    };

    TMap<TPartialBlobId, TRequestData> requestsByBlobId;

    for (auto& blob: blobs) {
        const auto& blocks = blob.BlobContent.GetBlocks();
        Y_ABORT_UNLESS(blocks.size() == blob.BlockIndexToMark.size());

        for (size_t i = 0; i < blob.BlockIndexToMark.size(); ++i) {
            const auto& mark = blob.BlockIndexToMark[i].second;
            if (!std::holds_alternative<TBlobBlockMark>(mark.IndexSpecificMark))
            {
                continue;
            }

            const auto& blobBlockMark =
                std::get<TBlobBlockMark>(mark.IndexSpecificMark);

            auto& request = requestsByBlobId[blobBlockMark.BlobId];
            request.BlobOffsets.push_back(blobBlockMark.BlobOffset);
            request.Sglist.push_back(blocks[i]);
        }
    }

    TVector<TReadBlobRequest> requests(Reserve(requestsByBlobId.size()));
    for (auto& [blobId, request]: requestsByBlobId) {
        requests.push_back(
            {.BlobId = blobId,
             .BlobOffsets = std::move(request.BlobOffsets),
             .Sglist = TGuardedSgList(std::move(request.Sglist))});
    }

    return requests;
}

void TPromoteCompactionVisitor::AddBlockMark(ui32 blockIndex, TBlockMark mark)
{
    const ui64 targetRangeIndex = blockIndex / TargetRangeBlocksCount;

    auto& blocksForRange = BlocksPerRange[targetRangeIndex];

    auto& marksForBlock = blocksForRange[blockIndex];

    if (AllowBlockDuplicates || marksForBlock.empty()) {
        marksForBlock.emplace_back(mark);
        return;
    }

    Y_ABORT_UNLESS(marksForBlock.size() == 1);

    if (marksForBlock[0].CommitId < mark.CommitId) {
        marksForBlock[0] = std::move(mark);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
