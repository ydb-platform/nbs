#include "service_method.h"

#include "request_helpers.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// static
TBlockRange64 TBlockRangeHelper::GetRange(
    const NProto::TReadBlocksRequest& request,
    ui32 blockSize)
{
    Y_UNUSED(blockSize);

    return TBlockRange64::WithLength(
        request.GetStartIndex(),
        GetBlocksCount(request));
}

TBlockRange64 TBlockRangeHelper::GetRange(
    const NProto::TReadBlocksLocalRequest& request,
    ui32 blockSize)
{
    Y_UNUSED(blockSize);

    return TBlockRange64::WithLength(
        request.GetStartIndex(),
        GetBlocksCount(request));
}

// static
TBlockRange64 TBlockRangeHelper::GetRange(
    const NProto::TWriteBlocksRequest& request,
    ui32 blockSize)
{
    return TBlockRange64::WithLength(
        request.GetStartIndex(),
        CalculateWriteRequestBlockCount(request, blockSize));
}

// static
TBlockRange64 TBlockRangeHelper::GetRange(
    const NProto::TWriteBlocksLocalRequest& request,
    ui32 blockSize)
{
    return TBlockRange64::WithLength(
        request.GetStartIndex(),
        CalculateWriteRequestBlockCount(request, blockSize));
}

// static
TBlockRange64 TBlockRangeHelper::GetRange(
    const NProto::TZeroBlocksRequest& request,
    ui32 blockSize)
{
    Y_UNUSED(blockSize);
    return TBlockRange64::WithLength(
        request.GetStartIndex(),
        GetBlocksCount(request));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
