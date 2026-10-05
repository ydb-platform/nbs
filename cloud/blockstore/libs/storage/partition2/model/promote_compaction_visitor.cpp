#include "promote_compaction_visitor.h"

#include <util/generic/strbuf.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

TPromoteCompactionVisitor::TPromoteCompactionVisitor(
    TVector<ui64> targetRangeBlocksCount,
    TVector<ui64> targetBlobSizesForPromote,
    ui32 blockSize,
    ui32 maxBlocksInBlob,
    bool allowBlockDuplicates,
    const TCleanupQueue& cleanupQueue)
    : BlockSize(blockSize)
    , TargetRangeBlocksCount(std::move(targetRangeBlocksCount))
    , TargetBlobSizesForPromote(std::move(targetBlobSizesForPromote))
    , MaxBlocksInBlob(maxBlocksInBlob)
    , AllowBlockDuplicates(allowBlockDuplicates)
    , CleanupQueue(cleanupQueue)
{
    Y_ABORT_UNLESS(!TargetRangeBlocksCount.empty());
    Y_ABORT_UNLESS(
        IsSorted(TargetRangeBlocksCount.begin(), TargetRangeBlocksCount.end()));
    Y_ABORT_UNLESS(
        TargetRangeBlocksCount.size() == TargetBlobSizesForPromote.size());
    Y_ABORT_UNLESS(BlockSize);
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
    Y_ABORT_UNLESS(!IsDeletionMarker(blobId));
    auto mark = TBlockMark{
        .CommitId = commitId,
        .IndexSpecificMark =
            TBlobBlockMark{.BlobId = blobId, .BlobOffset = blobOffset}};

    AddBlockMark(blockIndex, std::move(mark));

    return true;
}

bool TPromoteCompactionVisitor::Visit(
    const TPartialBlobId& blobId,
    const NProto::TBlobMeta2& blobMeta)
{
    if (CleanupQueue.HasBlob(blobId)) {
        return true;
    }

    AffectedBlobs[blobId] = blobMeta;
    return true;
}

namespace {

TMap<ui64, TPromoteCompactionVisitor::TBlockMark> ExtractLiveBlocks(
    TMap<ui64, TVector<TPromoteCompactionVisitor::TBlockMark>>& blocks)
{
    TMap<ui64, TPromoteCompactionVisitor::TBlockMark> liveBlocks;

    TVector<ui64> blocksToRemoveFromOverwrittenOnes;
    for (auto& [blockIndex, marks]: blocks) {
        size_t newestMarkIndex = 0;
        for (size_t j = 0; j < marks.size(); ++j) {
            if (marks[j].CommitId > marks[newestMarkIndex].CommitId) {
                newestMarkIndex = j;
            }
        }

        std::swap(marks.back(), marks[newestMarkIndex]);

        liveBlocks[blockIndex] = std::move(marks.back());
        marks.pop_back();
        if (marks.empty()) {
            blocksToRemoveFromOverwrittenOnes.push_back(blockIndex);
        }
    }

    for (auto blockIndex: blocksToRemoveFromOverwrittenOnes) {
        blocks.erase(blockIndex);
    }

    return liveBlocks;
}

class TBlocksVisitor
{
    using TBlockMark = TPromoteCompactionVisitor::TBlockMark;
    using TFreshBlockMark = TPromoteCompactionVisitor::TFreshBlockMark;
    using TBlobBlockMark = TPromoteCompactionVisitor::TBlobBlockMark;
    using TBlob = TPromoteCompactionVisitor::TBlob;

private:
    const ui64 TargetRangeBlocksCount;
    const ui64 TargetBlobSizeForPromote;
    const ui32 MaxBlocksInBlob;
    const ui32 BlockSize;
    const bool AcceptOnlyHugeBlobs;

    TBlob Blob;
    std::optional<ui64> LastRangeIndex;
    TVector<TBlob>& Blobs;

public:
    TBlocksVisitor(
        ui64 targetRangeBlocksCount,
        ui64 targetBlobSizeForPromote,
        ui32 maxBlocksInBlob,
        ui32 blockSize,
        bool acceptOnlyHugeBlobs,
        TVector<TBlob>& blobs)
        : TargetRangeBlocksCount(targetRangeBlocksCount)
        , TargetBlobSizeForPromote(targetBlobSizeForPromote)
        , MaxBlocksInBlob(maxBlocksInBlob)
        , BlockSize(blockSize)
        , AcceptOnlyHugeBlobs(acceptOnlyHugeBlobs)
        , Blobs(blobs)
    {}

    void Visit(ui32 blockIndex, const TBlockMark& mark)
    {
        if (blockIndex / TargetRangeBlocksCount != LastRangeIndex ||
            Blob.BlockIndexToMark.size() == MaxBlocksInBlob)
        {
            if (Blob.BlockIndexToMark.size() > 0) {
                if (!AcceptOnlyHugeBlobs || BlobIsHuge(Blob)) {
                    Blobs.emplace_back(std::move(Blob));
                }
            }

            Blob.BlockIndexToMark.clear();
            Blob.BlobContent.Clear();
            LastRangeIndex = blockIndex / TargetRangeBlocksCount;
        }

        Blob.BlockIndexToMark.emplace_back(blockIndex, mark);

        if (std::holds_alternative<TFreshBlockMark>(mark.IndexSpecificMark)) {
            const auto& freshBlockMark =
                std::get<TFreshBlockMark>(mark.IndexSpecificMark);

            if (freshBlockMark.Content.empty()) {
                Blob.BlobContent.AddBlock(BlockSize, char{0});
            } else {
                Blob.BlobContent.AddBlock(
                    {freshBlockMark.Content.data(),
                     freshBlockMark.Content.size()});
            }
        } else if (
            std::holds_alternative<TBlobBlockMark>(mark.IndexSpecificMark))
        {
            Blob.BlobContent.AddBlock(BlockSize, char{0});
        } else {
            Y_ABORT("Unexpected mark type");
        }
    }

    void Finish()
    {
        if (Blob.BlockIndexToMark.size() > 0) {
            if (!AcceptOnlyHugeBlobs || BlobIsHuge(Blob)) {
                Blobs.emplace_back(std::move(Blob));
            }
        }

        Blob.BlockIndexToMark.clear();
        Blob.BlobContent.Clear();
        LastRangeIndex = std::nullopt;
    }

private:
    [[nodiscard]] bool BlobIsHuge(const TBlob& blob) const
    {
        return blob.BlobContent.GetBlocksCount() >= TargetBlobSizeForPromote;
    }
};

}   // namespace

auto TPromoteCompactionVisitor::Finish() -> TScanResult
{
    TVector<TBlob> blobs;
    TVector<TBlob> alreadyOverwrittenBlobs;

    auto overwrittenBlocks = std::move(Blocks);
    auto liveBlocks = ExtractLiveBlocks(overwrittenBlocks);

    for (size_t i = 0; i < TargetRangeBlocksCount.size(); ++i) {
        const ui64 rangeSize = TargetRangeBlocksCount[i];
        const bool acceptOnlyHugeBlobs = i < TargetRangeBlocksCount.size() - 1;

        TBlocksVisitor visitor{
            rangeSize,
            TargetBlobSizesForPromote[i],
            MaxBlocksInBlob,
            BlockSize,
            acceptOnlyHugeBlobs,
            blobs};

        const size_t blobsCountBefore = blobs.size();

        for (auto& [blockIndex, mark]: liveBlocks) {
            visitor.Visit(blockIndex, mark);
        }
        visitor.Finish();

        for (size_t j = blobsCountBefore; j < blobs.size(); ++j) {
            for (const auto& [blockIndex, _]: blobs[j].BlockIndexToMark) {
                liveBlocks.erase(blockIndex);
            }
        }

        // No need to try to bypass levels for already garbage blobs.
        if (acceptOnlyHugeBlobs) {
            continue;
        }

        TBlocksVisitor garbageVisitor{
            rangeSize,
            TargetBlobSizesForPromote[i],
            MaxBlocksInBlob,
            BlockSize,
            false,   // acceptOnlyHugeBlobs
            alreadyOverwrittenBlobs};

        for (auto& [blockIndex, marks]: overwrittenBlocks) {
            for (const auto& mark: marks) {
                garbageVisitor.Visit(blockIndex, mark);
            }
        }
        garbageVisitor.Finish();

        // TODO: Check that no blocks was skipped.
    }

    return {
        .ResultedBlobs = std::move(blobs),
        .AlreadyOverwrittenBlobs = std::move(alreadyOverwrittenBlobs),
        .AffectedBlobs = std::move(AffectedBlobs),
        .MaxCommitId = MaxCommitId};
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
             .Sglist = std::move(request.Sglist)});
    }

    return requests;
}

void TPromoteCompactionVisitor::AddBlockMark(ui32 blockIndex, TBlockMark mark)
{
    auto& marksForBlock = Blocks[blockIndex];

    MaxCommitId = std::max(MaxCommitId, mark.CommitId);

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
