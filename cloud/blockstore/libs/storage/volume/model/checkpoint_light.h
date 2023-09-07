#pragma once

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/string.h>

#include <array>

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
    ui64 BlocksCount;
    std::array<TCompressedBitmap, 2> CheckpointsData;

    void CreateCheckpoint(TString checkpointId, bool isInitial) {
        if (checkpointId == CheckpointId) {
            return;
        }
        CheckpointId = std::move(checkpointId);

        if (!isInitial) {
            CheckpointsData[0] = std::move(CheckpointsData[1]);
        }
    }

public:
    TCheckpointLight(ui64 blocksCount)
        : BlocksCount{blocksCount}
        , CheckpointsData{
            TCompressedBitmap{blocksCount},
            TCompressedBitmap{blocksCount}}
    {
        CheckpointsData[0].Set(0, BlocksCount);
        CheckpointsData[1].Set(0, BlocksCount);
    }

    // All blocks should be marked as modified when first non-initial checkpoint is created.
    TCheckpointLight(ui64 blocksCount, TString initialCheckpointId)
        : TCheckpointLight(blocksCount)
    {
        CreateCheckpoint(initialCheckpointId, true);
    }

    TString GetCheckpointId() const
    {
        return CheckpointId;
    }

    const TCompressedBitmap& GetCheckpointData() const {
        return CheckpointsData[0];
    }

    void CreateCheckpoint(TString checkpointId) {
        CreateCheckpoint(checkpointId, false);
    }

    /**
     * @brief Mark blocks as modified
     * @param[in] beginBlock begin block to mark
     * @param[in] endBlock end block to mark. End block out of range.
     */
    void Set(ui64 beginBlock, ui64 endBlock)
    {
        CheckpointsData[1].Set(beginBlock, endBlock);
    }
};

}   // namespace NCloud::NBlockStore::NStorage
