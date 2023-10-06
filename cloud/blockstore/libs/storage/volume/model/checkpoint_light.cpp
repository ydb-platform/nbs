#include "checkpoint_light.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TCheckpointLight::TCheckpointLight(ui64 blocksCount)
    : BlocksCount{blocksCount}
    , CurrentDirtyBlocks{TCompressedBitmap{blocksCount}}
    , FutureDirtyBlocks{TCompressedBitmap{blocksCount}}
{
    CurrentDirtyBlocks.Set(0, BlocksCount);
    FutureDirtyBlocks.Set(0, BlocksCount);
}

const TString& TCheckpointLight::GetCheckpointId() const
{
    return CheckpointId;
}

void TCheckpointLight::CreateCheckpoint(TString checkpointId)
{
    if (checkpointId == CheckpointId) {
        return;
    }
    CheckpointId = std::move(checkpointId);

    CurrentDirtyBlocks = std::move(FutureDirtyBlocks);
    FutureDirtyBlocks.Clear();
}

NProto::TError TCheckpointLight::FindDirtyBlocksBetweenCheckpoints(
    const TBlockRange64& blockRange,
    TString* mask) const
{
    if (blockRange.End >= BlocksCount) {
        return MakeError(E_ARGUMENT, "Block range is out of bounds");
    }

    mask->clear();
    mask->reserve(blockRange.Size());

    for (auto i = blockRange.Start; i <= blockRange.End;) {
        ui8 bitData = 0;
        for (auto j = 0; j < 8 && i <= blockRange.End; ++j) {
            bitData |= CurrentDirtyBlocks.Test(i) << j;
            ++i;
        }
        mask->push_back(bitData);
    }

    return {};
}

void TCheckpointLight::Set(const TBlockRange64& blockRange)
{
    FutureDirtyBlocks.Set(blockRange.Start, blockRange.End + 1);
}

const TCompressedBitmap& TCheckpointLight::GetCurrentDirtyBlocks() const
{
    return CurrentDirtyBlocks;
}

}   // namespace NCloud::NBlockStore::NStorage
