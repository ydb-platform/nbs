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

const TString& TCheckpointLight::GetPreviousCheckpointId() const
{
    return PreviousCheckpointId;
}

void TCheckpointLight::CreateCheckpoint(TString checkpointId)
{
    if (checkpointId == CheckpointId) {
        return;
    }
    PreviousCheckpointId = std::move(CheckpointId);
    CheckpointId = std::move(checkpointId);

    CurrentDirtyBlocks = std::move(FutureDirtyBlocks);
    FutureDirtyBlocks.Clear();
}

void TCheckpointLight::DeleteCheckpoint(TString checkpointId)
{
    if (checkpointId == CheckpointId) {
        CheckpointId = "";
    }
    if (checkpointId == PreviousCheckpointId) {
        PreviousCheckpointId = "";
    }
}

NProto::TError TCheckpointLight::FindDirtyBlocksBetweenCheckpoints(
    TString lowCheckpointId,
    TString highCheckpointId,
    const TBlockRange64& blockRange,
    TString* mask) const
{
    if (blockRange.End >= BlocksCount) {
        return MakeError(E_ARGUMENT, "Block range is out of bounds");
    }

    // Pessimize diff if we don't know the correct diff.
    bool allOnes = false;
    if (highCheckpointId != "") {
       allOnes = lowCheckpointId == "" ||
            lowCheckpointId != PreviousCheckpointId ||
            highCheckpointId != CheckpointId;
    } else if (lowCheckpointId != "") {
        allOnes = lowCheckpointId != PreviousCheckpointId;
    } else {
        allOnes = true;
    }

    auto test = [&](ui64 i) {
        if (allOnes) {
            return true;
        }
        if (highCheckpointId != "") {
            return CurrentDirtyBlocks.Test(i);
        }
        return CurrentDirtyBlocks.Test(i) || FutureDirtyBlocks.Test(i);
    };

    mask->clear();
    mask->reserve(blockRange.Size());

    for (auto i = blockRange.Start; i <= blockRange.End;) {
        ui8 bitData = 0;
        for (auto j = 0; j < 8 && i <= blockRange.End; ++j) {
            bitData |= test(i) << j;
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
