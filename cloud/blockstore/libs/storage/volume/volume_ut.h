#pragma once

#include <cloud/blockstore/libs/storage/volume/testlib/test_env.h>

namespace NCloud::NBlockStore::NStorage::NTestVolumeHelpers {

////////////////////////////////////////////////////////////////////////////////

TBlockRange64 GetBlockRangeById(ui32 blockIndex);

template <uint32_t LineNumber>
void CheckBlockContent(
    NTestVolume::TVolumeClient& volume,
    const TString& clientId,
    const TString& checkpointId,
    const TBlockRange64 range,
    const TString& expectedValue)
{
    auto readResponse = volume.ReadBlocks(range, clientId, checkpointId);
    const auto& bufs = readResponse->Record.GetBlocks().GetBuffers();
    UNIT_ASSERT_VALUES_EQUAL(range.Size(), bufs.size());
    for (ui32 i = 0; i < range.Size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(expectedValue, bufs[i]);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NTestVolumeHelpers
