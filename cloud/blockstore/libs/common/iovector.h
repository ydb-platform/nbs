#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/io.pb.h>
#include <cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCopyStats
{
    ui32 TotalBlockCount = 0;
    ui32 VoidBlockCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

TSgList ResizeIOVector(NProto::TIOVector& iov, ui32 blockCount, ui32 blockSize);

TSgList GetSgList(const NProto::TWriteBlocksRequest& request);

TResultOrError<TSgList>
GetSgList(const NProto::TReadBlocksResponse& response, ui32 expectedBlockSize);

// Copy all data from iov to sglist. Skip first offsetInBlocks in sglist.
TCopyStats CopyToSgList(
    const NProto::TIOVector& src,
    const TSgList& dst,
    ui64 offsetInBlocks,
    ui32 blockSize);

// Check all buffers, and trim those buffers that contain only zeros.
void TrimVoidBuffers(NProto::TIOVector& iov);

// Creates buffers for all blocks and copies only those that contain non-zeros.
// Buffers whose data is all zeros remain of zero size.
size_t CopyAndTrimVoidBuffers(
    TBlockDataRef src,
    ui32 blockCount,
    ui32 blockSize,
    NProto::TIOVector* dst);

// Count how many buffers are void.
[[nodiscard]] ui32 CountVoidBuffers(const NProto::TIOVector& iov);

// Checks that the buffer contains only zeros.
[[nodiscard]] bool IsAllZeroes(const char* src, size_t size);
[[nodiscard]] bool IsAllZeroes(TBlockDataRef block);
}   // namespace NCloud::NBlockStore
