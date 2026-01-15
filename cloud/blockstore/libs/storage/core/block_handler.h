#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IWriteBlocksHandler
{
    virtual ~IWriteBlocksHandler() = default;

    virtual TGuardedSgList GetBlocks(const TBlockRange64& range) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IReadBlocksHandler
{
    virtual ~IReadBlocksHandler() = default;

    // TODO: split into multiple funcs:
    // * void SetUnencryptedBlockMaskBits(const TVector<ui64>& blockIndices, bool value)
    // * TGuardedSgList InitializeBuffers(const TVector<ui64>& blockIndices)
    virtual TGuardedSgList GetGuardedSgList(
        const TVector<ui64>& blockIndices,
        bool baseDisk) = 0;

    virtual bool SetBlock(
        ui64 blockIndex,
        TBlockDataRef blockContent,
        bool baseDisk) = 0;

    virtual void Clear() = 0;

    virtual void GetResponse(NProto::TReadBlocksResponse& response) = 0;

    virtual TGuardedSgList GetLocalResponse(
        NProto::TReadBlocksLocalResponse& response) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IWriteBlocksHandlerPtr CreateWriteBlocksHandler(
    const TBlockRange64& writeRange,
    std::unique_ptr<TEvService::TEvWriteBlocksRequest> request,
    ui32 blockSize);

IWriteBlocksHandlerPtr CreateWriteBlocksHandler(
    const TBlockRange64& writeRange,
    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> request);

IReadBlocksHandlerPtr CreateReadBlocksHandler(
    const TBlockRange64& readRange,
    ui32 blockSize,
    bool enableDataIntegrityValidation);

IReadBlocksHandlerPtr CreateReadBlocksHandler(
    const TBlockRange64& readRange,
    const TGuardedSgList& sglist,
    ui32 blockSize,
    bool enableDataIntegrityValidation);

IWriteBlocksHandlerPtr CreateMixedWriteBlocksHandler(
    TVector<std::pair<IWriteBlocksHandlerPtr, TBlockRange64>> parts);

}   // namespace NCloud::NBlockStore::NStorage
