#include "rebase_logic.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TRebaseResult RebaseMixedBlocks(
    TVector<TBlock>& blocks,
    ui64 lastCommitId,
    TFindCheckpoint findCheckpoint,
    TFindBlock findBlock)
{
    TRebaseResult result;

    for (auto& block: blocks) {
        ui64 minCommitId = block.MinCommitId;
        ui64 maxCommitId = block.MaxCommitId;

        bool referencedByCheckpoint = false;

        if (minCommitId < maxCommitId) {
            // rebase MinCommitId
            minCommitId = findCheckpoint(block.NodeId, minCommitId);
            if (minCommitId == InvalidCommitId) {
                minCommitId = lastCommitId;
            }

            if (maxCommitId != InvalidCommitId) {
                // rebase MaxCommitId
                maxCommitId = findCheckpoint(block.NodeId, maxCommitId);
                if (maxCommitId == InvalidCommitId) {
                    maxCommitId = lastCommitId;
                }

                if (minCommitId < maxCommitId) {
                    // this version still referenced by checkpoint
                    ++result.CheckpointBlocks;

                    referencedByCheckpoint = true;
                    result.UsedCheckpoints.insert(minCommitId);
                }
            }
        }

        if (maxCommitId == InvalidCommitId || referencedByCheckpoint) {
            ++result.LiveBlocks;

            // we should not change relative order of the block versions
            if (!findBlock(block.NodeId, block.BlockIndex)) {
                block.MinCommitId = minCommitId;
                block.MaxCommitId = maxCommitId;
            }
        } else {
            ++result.GarbageBlocks;

            // block is not visible anymore and could be safely deleted
            block.MinCommitId = 0;
            block.MaxCommitId = 0;
        }
    }

    return result;
}

}   // namespace NCloud::NFileStore::NStorage
