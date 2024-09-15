#pragma once

#include "cloud/blockstore/libs/common/block_range.h"

#include <cloud/blockstore/libs/service/device_handler.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TBlocksInfo
{
    TBlocksInfo() = default;
    TBlocksInfo(ui64 from, ui64 length, ui32 blockSize);
    TBlocksInfo(const TBlocksInfo&) = default;

    // The size of the buffer required to store data, considering that the data
    // may not be aligned.
    [[nodiscard]] size_t BufferSize() const;

    // The data may be misaligned for two reasons: if the start or end of the
    // block do not correspond to the block boundaries, or if the client buffers
    // are not a multiple of the block size.
    [[nodiscard]] bool IsAligned() const;

    // Creates an aligned TBlocksInfo.
    [[nodiscard]] TBlocksInfo MakeAligned() const;

    TBlockRange64 Range;
    // Offset relative to the beginning of the range.
    ui64 BeginOffset = 0;
    // Offset relative to the ending of the range.
    ui64 EndOffset = 0;
    const ui32 BlockSize = 0;
    bool SgListAligned = true;
};

////////////////////////////////////////////////////////////////////////////////

// The TAlignedDeviceHandler can only process requests that are aligned. If the
// size of a request exceeds the maximum size that the underlying layer can
// handle, the TAlignedDeviceHandler will break the request into smaller parts
// and execute them separately.
class TAlignedDeviceHandler
    : public IDeviceHandler
    , public std::enable_shared_from_this<TAlignedDeviceHandler>
{
private:
    const IStoragePtr Storage;
    const TString ClientId;
    const ui32 BlockSize;
    const ui32 MaxBlockCount;

public:
    TAlignedDeviceHandler(
        IStoragePtr storage,
        TString clientId,
        ui32 blockSize,
        ui32 maxBlockCount);

    // implements IDeviceHandler
    NThreading::TFuture<NProto::TReadBlocksLocalResponse> Read(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList,
        const TString& checkpointId) override;

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> Write(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList) override;

    NThreading::TFuture<NProto::TZeroBlocksResponse>
    Zero(TCallContextPtr ctx, ui64 from, ui64 length) override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

    NThreading::TFuture<NProto::TReadBlocksResponse> ExecuteReadRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo,
        TGuardedSgList sgList,
        TString checkpointId) const;

    NThreading::TFuture<NProto::TWriteBlocksResponse> ExecuteWriteRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo,
        TGuardedSgList sgList) const;

    NThreading::TFuture<NProto::TZeroBlocksResponse> ExecuteZeroRequest(
        TCallContextPtr ctx,
        ui64 startIndex,
        ui32 blockCount) const;
};

////////////////////////////////////////////////////////////////////////////////

// Normalizes the list of blocks such that each block has its own buffer, and
// the size of each buffer equals the size of the block. If this is not
// possible, an error message is returned with a description of the problem. If
// the operation succeeds, S_OK is returned.
NProto::TError TryToNormalize(
    TGuardedSgList& guardedSgList,
    TBlocksInfo& blocksInfo);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
