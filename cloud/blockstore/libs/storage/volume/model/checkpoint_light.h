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

    TString GetCheckpointId() const
    {
        return CheckpointId;
    }

    const TCompressedBitmap& GetCheckpointData() const {
        return CheckpointsData[0];
    }

    void CreateCheckpoint(TString checkpointId) {
        if (checkpointId == CheckpointId) {
            return;
        }
        CheckpointId = std::move(checkpointId);
        CheckpointsData[0] = std::move(CheckpointsData[1]);
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
