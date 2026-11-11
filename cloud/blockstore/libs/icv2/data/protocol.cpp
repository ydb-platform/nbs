#include "protocol.h"

#include <cloud/storage/core/libs/common/sglist.h>

#include <limits>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

ui64 GetDataReservationBytes(ui64 payloadBytes, ui32 blocksCount)
{
    return payloadBytes +
           (static_cast<ui64>(blocksCount) * sizeof(TBlockDataRef));
}

NProto::TError
ValidateBlockRange(const TString& diskId, ui64 startIndex, ui32 blocksCount)
{
    if (diskId.empty() || !blocksCount ||
        startIndex > std::numeric_limits<ui64>::max() - (blocksCount - 1))
    {
        return MakeError(E_ARGUMENT, "invalid IC V2 data range");
    }

    return {};
}

TResultOrError<ui64> ValidateDataRangeAndGetBytes(
    const TString& diskId,
    ui64 startIndex,
    ui32 blocksCount,
    ui32 blockSize)
{
    if (auto error = ValidateBlockRange(diskId, startIndex, blocksCount);
        HasError(error))
    {
        return error;
    }

    const ui64 bytes = static_cast<ui64>(blocksCount) * blockSize;
    if (!blockSize || bytes > std::numeric_limits<size_t>::max()) {
        return MakeError(E_ARGUMENT, "invalid IC V2 data size");
    }
    return bytes;
}

}   // namespace NCloud::NBlockStore::NIcV2
