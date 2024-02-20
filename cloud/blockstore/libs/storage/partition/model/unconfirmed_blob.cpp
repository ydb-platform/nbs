#include "unconfirmed_blob.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

bool Overlaps(
    const TUnconfirmedBlobs& blobs,
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange)
{
    for (const auto& [entryCommitId, entryBlobs]: blobs) {
        if (entryCommitId > highCommitId) {
            // entry is too new, thus does not affect this commit range
            continue;
        }

        if (entryCommitId < lowCommitId) {
            // entry is too old, thus does not affect this commit range
            continue;
        }

        for (const auto& blob: entryBlobs) {
            if (blob.BlockRange.Overlaps(blockRange)) {
                return true;
            }
        }
    }

    return false;
}

bool Overlaps(
    const TUnconfirmedBlobs& blobs,
    ui64 commitId,
    const TBlockRange32& blockRange)
{
    return Overlaps(blobs, 0, commitId, blockRange);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
