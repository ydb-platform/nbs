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
    // The request also unaligned if the sglist buffer sizes are not multiples
    // of the block size
    bool SgListAligned = true;
};

////////////////////////////////////////////////////////////////////////////////

// The TAlignedDeviceHandler can only process requests that are aligned. If the
// size of a request exceeds the maximum size that the underlying layer can
// handle, the TAlignedDeviceHandler will break the request into smaller parts
// and execute them separately. If a request contains unaligned data, the
// E_ARGUMENT error is returned.
class TAlignedDeviceHandler final
    : public IDeviceHandler
    , public std::enable_shared_from_this<TAlignedDeviceHandler>
{
private:
    const IStoragePtr Storage;
    const TString DiskId;
    const TString ClientId;
    const ui32 BlockSize;
    const ui32 MaxBlockCount;
    const ui32 MaxBlockCountForZeroBlocksRequest;
    const bool IsReliableMediaKind;

    std::atomic<bool> CriticalErrorReported = false;

public:
    TAlignedDeviceHandler(
        IStoragePtr storage,
        TString diskId,
        TString clientId,
        ui32 blockSize,
        ui32 maxSubRequestSize,
        ui32 maxZeroBlocksSubRequestSize,
        bool checkBufferModificationDuringWriting,
        bool isReliableMediaKind);

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

    // Performs a read. It can only be called for aligned data.
    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ExecuteReadRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo,
        TGuardedSgList sgList,
        TString checkpointId);

    // Performs a write. It can only be called for aligned data.
    NThreading::TFuture<NProto::TWriteBlocksResponse> ExecuteWriteRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo,
        TGuardedSgList sgList);

    // Performs a zeroes. It can only be called for aligned data.
    NThreading::TFuture<NProto::TZeroBlocksResponse> ExecuteZeroRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo);

private:
    void ReportCriticalError(
        const NProto::TError& error,
        const TString& operation,
        TBlockRange64 range);
};

////////////////////////////////////////////////////////////////////////////////

// Normalizes the SgList in guardedSgList. If the total size of the buffers does
// not match the request size, an error is returned. If it is not possible to
// normalize the number of buffers so that they correspond to the number of
// requested blocks and the size of each buffer is equal to the specified block
// size, the SgListAligned flag is set in the blocksInfo structure, but no error
// is returned. This indicates that the request is valid, but not aligned.
NProto::TError TryToNormalize(
    TGuardedSgList& guardedSgList,
    TBlocksInfo& blocksInfo);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
