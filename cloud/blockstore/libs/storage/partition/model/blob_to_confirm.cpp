#include "blob_to_confirm.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

bool Overlaps(
    const TCommitIdToBlobsToConfirm& blobs,
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

}   // namespace NCloud::NBlockStore::NStorage::NPartition
