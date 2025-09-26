#include "part_tx.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

void TTxPartition::TAddBlobs::BuildFlushedCommitIdsFromChannel()
{
    TVector<ui64> commitIds;

    for (const auto& blob: FreshBlobs) {
        for (const auto& block: blob.Blocks) {
            if (!block.IsStoredInDb) {
                commitIds.push_back(block.CommitId);
            }
        }
    }

    if (!commitIds) {
        return;
    }

    Sort(commitIds);

    ui64 cur = commitIds.front();
    ui32 cnt = 0;

    for (const auto commitId: commitIds) {
        if (commitId == cur) {
            ++cnt;
        } else {
            FlushedCommitIdsFromChannel.emplace_back(cur, cnt);
            cur = commitId;
            cnt = 1;
        }
    }

    FlushedCommitIdsFromChannel.emplace_back(cur, cnt);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
