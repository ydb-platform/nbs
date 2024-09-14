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

    [[nodiscard]] bool IsAligned() const;

    [[nodiscard]] TBlocksInfo MakeAligned() const;

    TBlockRange64 Range;
    ui64 BeginOffset = 0;
    ui64 EndOffset = 0;
    const ui32 BlockSize = 0;
    bool SgListAligned = true;
};

////////////////////////////////////////////////////////////////////////////////

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

TStorageBuffer AllocateStorageBuffer(IStorage& storage, size_t bytesCount);

NProto::TError TryToNormalize(
    TGuardedSgList& guardedSgList,
    TBlocksInfo& blocksInfo);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
