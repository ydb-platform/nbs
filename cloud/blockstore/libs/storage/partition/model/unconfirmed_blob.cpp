#include "unconfirmed_blob.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

bool Overlaps(
    const TUnconfirmedBlobs& blobs,
    ui64 commitId,
    const TBlockRange32& blockRange)
{
    for (const auto& entry: blobs) {
        if (entry.first > commitId) {
            // entry is too new, thus does not affect this commit
            continue;
        }

        for (const auto& blob: entry.second) {
            if (blob.BlockRange.Overlaps(blockRange)) {
                return true;
            }
        }
    }

    return false;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
