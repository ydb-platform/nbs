#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/storage/core/libs/common/compressed_bitmap.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * @details Class remember blocks, which were modified after making a checkpoint.
 * In the beginning state is all blocks are modified.
 * Example:
 * 1111 - begin state
 * 1111 - first checkpoint
 * 0101 - modified blocks after checkpoint
 * 0101 - second checkpoint (now first checkpoint doesn't exist)
 */
class TCheckpointLight
{
private:
    TString CheckpointId;
    TString PreviousCheckpointId;
    ui64 BlocksCount;

    TCompressedBitmap CurrentDirtyBlocks;
    TCompressedBitmap FutureDirtyBlocks;

    bool IsCheckpointEmptyOrExists(const TString& checkpointId) const;

public:
    TCheckpointLight(ui64 blocksCount);

    const TString& GetCheckpointId() const;
    const TString& GetPreviousCheckpointId() const;

    void CreateCheckpoint(const TString& checkpointId);

    void DeleteCheckpoint(const TString& checkpointId);

    /**
     * Returns S_OK if and only if input block range is valid.
     * If returns true, stores block mask of dirty blocks in the output parameter.
     * Returns mask of all '1' if either lowCheckpointId or highCheckpointId is irrelevant.
     */
    NProto::TError FindDirtyBlocksBetweenCheckpoints(
        const TString& lowCheckpointId,
        const TString& highCheckpointId,
        const TBlockRange64& blockRange,
        TString* mask) const;

    /**
     * @brief Mark blocks as modified
     * @param[in] blockRange block range to mark.
     */
    void Set(const TBlockRange64& blockRange);

    // Needed for tests
    const TCompressedBitmap& GetCurrentDirtyBlocks() const;
};

}   // namespace NCloud::NBlockStore::NStorage
