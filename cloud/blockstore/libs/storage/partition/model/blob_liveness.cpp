#include "blob_liveness.h"

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>

#include <tuple>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

void AppendBlobBlocks(const NProto::TBlobMeta& blobMeta, TVector<ui32>& blocks)
{
    if (blobMeta.HasMixedBlocks()) {
        const auto& mixedBlocks = blobMeta.GetMixedBlocks().GetBlocks();
        blocks.insert(blocks.end(), mixedBlocks.begin(), mixedBlocks.end());
    } else if (blobMeta.HasMergedBlocks()) {
        const auto& mergedBlocks = blobMeta.GetMergedBlocks();
        const auto range = TBlockRange32::MakeClosedInterval(
            mergedBlocks.GetStart(),
            mergedBlocks.GetEnd());
        for (ui32 blockIndex: xrange(range)) {
            blocks.push_back(blockIndex);
        }
    }
}

TVector<TBlockRange32> GroupContiguousBlocks(const TVector<ui32>& sortedBlocks)
{
    TVector<TBlockRange32> ranges;
    for (ui32 blockIndex: sortedBlocks) {
        if (!ranges.empty() && ranges.back().End + 1 == blockIndex) {
            ++ranges.back().End;
        } else {
            ranges.push_back(TBlockRange32::MakeOneBlock(blockIndex));
        }
    }
    return ranges;
}

TLiveBlobs FindLiveBlobs(
    TVector<TBlockVersion> versions,
    TVector<ui64> checkpointCommitIds)
{
    TVector<ui64> readerCommitIds = std::move(checkpointCommitIds);
    readerCommitIds.push_back(Max<ui64>());
    SortUnique(readerCommitIds, TGreater<ui64>());

    Sort(
        versions,
        [](const TBlockVersion& l, const TBlockVersion& r)
        {
            return std::tie(l.BlockIndex, r.CommitId) <
                   std::tie(r.BlockIndex, l.CommitId);
        });

    TLiveBlobs liveBlobs;

    auto blockBegin = versions.begin();
    while (blockBegin != versions.end()) {
        const ui32 blockIndex = blockBegin->BlockIndex;
        const auto blockEnd = FindIf(
            blockBegin,
            versions.end(),
            [&](const TBlockVersion& v) { return v.BlockIndex != blockIndex; });

        auto winner = blockBegin;
        for (ui64 readerCommitId: readerCommitIds) {
            winner = FindIf(
                winner,
                blockEnd,
                [&](const TBlockVersion& v)
                { return v.CommitId <= readerCommitId; });
            if (winner == blockEnd) {
                break;
            }

            // A reader takes one of the versions tied on commit. Keeping all
            // of them live costs at most an extra check.
            for (auto it = winner;
                 it != blockEnd && it->CommitId == winner->CommitId;
                 ++it)
            {
                liveBlobs.insert(it->BlobId);
            }
        }

        blockBegin = blockEnd;
    }

    return liveBlobs;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
