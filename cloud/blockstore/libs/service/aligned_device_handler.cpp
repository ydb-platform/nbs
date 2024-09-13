#include "aligned_device_handler.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

// Removes the first blockCount elements from the sgList. Returns these removed
// items in TGuardedSgList.
TGuardedSgList TakeHeadBlocks(TGuardedSgList& sgList, ui32 blockCount)
{
    auto guard = sgList.Acquire();
    if (!guard) {
        return {};
    }

    const TSgList& blockList = guard.Get();
    auto result =
        sgList.Create({blockList.begin(), blockList.begin() + blockCount});
    sgList.SetSgList({blockList.begin() + blockCount, blockList.end()});
    return result;
}

}   // namespace

TResultOrError<bool> TryToNormalize(
    TGuardedSgList& guardedSgList,
    const TBlocksInfo& blocksInfo,
    ui64 length,
    ui32 blockSize)
{
    if (length == 0) {
        return MakeError(E_ARGUMENT, "Local request has zero length");
    }

    auto guard = guardedSgList.Acquire();
    if (!guard) {
        return MakeError(
            E_CANCELLED,
            "failed to acquire sglist in DeviceHandler");
    }

    auto bufferSize = SgListGetSize(guard.Get());
    if (bufferSize != length) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Invalid local request:" << " buffer size " << bufferSize
                << " not equal to length " << length);
    }

    if (blocksInfo.BeginOffset != 0 || blocksInfo.EndOffset != 0) {
        return false;
    }

    for (const auto& buffer: guard.Get()) {
        if (buffer.Size() % blockSize != 0) {
            return false;
        }
    }

    auto sgListOrError = SgListNormalize(guard.Get(), blockSize);
    if (HasError(sgListOrError)) {
        return sgListOrError.GetError();
    }

    guardedSgList.SetSgList(sgListOrError.ExtractResult());
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TStorageBuffer AllocateStorageBuffer(IStorage& storage, size_t bytesCount)
{
    auto buffer = storage.AllocateBuffer(bytesCount);
    if (!buffer) {
        buffer = std::shared_ptr<char>(
            new char[bytesCount],
            std::default_delete<char[]>());
    }
    return buffer;
}

////////////////////////////////////////////////////////////////////////////////

TBlocksInfo::TBlocksInfo(ui64 from, ui64 length, ui32 blockSize)
{
    ui64 startIndex = from / blockSize;
    ui64 beginOffset = from - startIndex * blockSize;

    auto realLength = beginOffset + length;
    ui64 blocksCount = realLength / blockSize;

    if (blocksCount * blockSize < realLength) {
        ++blocksCount;
    }

    ui64 endOffset = blocksCount * blockSize - realLength;

    Range = TBlockRange64::WithLength(startIndex, blocksCount);
    BeginOffset = beginOffset;
    EndOffset = endOffset;
}

////////////////////////////////////////////////////////////////////////////////

TAlignedDeviceHandler::TAlignedDeviceHandler(
        IStoragePtr storage,
        TString clientId,
        ui32 blockSize,
        ui32 maxBlockCount)
    : Storage(std::move(storage))
    , ClientId(std::move(clientId))
    , BlockSize(blockSize)
    , MaxBlockCount(maxBlockCount)
{
    Y_ABORT_UNLESS(MaxBlockCount > 0);
}

TFuture<NProto::TReadBlocksLocalResponse> TAlignedDeviceHandler::Read(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList,
    const TString& checkpointId)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);

    auto aligned = TryToNormalize(sgList, blocksInfo, length, BlockSize);
    if (HasError(aligned)) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            TErrorResponse(aligned.GetError()));
    }

    if (!aligned.GetResult()) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            TErrorResponse(E_ARGUMENT, "Request is not aligned"));
    }

    return ExecuteReadRequest(
        std::move(ctx),
        blocksInfo,
        std::move(sgList),
        checkpointId);
}

TFuture<NProto::TWriteBlocksLocalResponse> TAlignedDeviceHandler::Write(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);

    auto aligned = TryToNormalize(sgList, blocksInfo, length, BlockSize);
    if (HasError(aligned)) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse(aligned.GetError()));
    }

    if (!aligned.GetResult()) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse(E_ARGUMENT, "Request is not aligned"));
    }

    return ExecuteWriteRequest(std::move(ctx), blocksInfo, std::move(sgList));
}

TFuture<NProto::TZeroBlocksResponse>
TAlignedDeviceHandler::Zero(TCallContextPtr ctx, ui64 from, ui64 length)
{
    if (length == 0) {
        return MakeFuture<NProto::TZeroBlocksResponse>(
            TErrorResponse(E_ARGUMENT, "Local request has zero length"));
    }

    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    if (blocksInfo.BeginOffset != 0 || blocksInfo.EndOffset != 0) {
        return MakeFuture<NProto::TZeroBlocksResponse>(
            TErrorResponse(E_ARGUMENT, "Request is not aligned"));
    }

    return ExecuteZeroRequest(
        std::move(ctx),
        blocksInfo.Range.Start,
        blocksInfo.Range.Size());
}

TStorageBuffer TAlignedDeviceHandler::AllocateBuffer(size_t bytesCount)
{
    return AllocateStorageBuffer(*Storage, bytesCount);
}

TFuture<NProto::TReadBlocksLocalResponse>
TAlignedDeviceHandler::ExecuteReadRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo,
    TGuardedSgList sgList,
    TString checkpointId) const
{
    auto requestBlockCount =
        std::min<ui32>(blocksInfo.Range.Size(), MaxBlockCount);

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->MutableHeaders()->SetRequestId(ctx->RequestId);
    request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetCheckpointId(checkpointId);
    request->SetStartIndex(blocksInfo.Range.Start);
    request->SetBlocksCount(requestBlockCount);
    request->BlockSize = BlockSize;

    if (requestBlockCount == blocksInfo.Range.Size()) {
        // The request size is quite small. We do all work at once.
        request->Sglist = std::move(sgList);
        return Storage->ReadBlocksLocal(std::move(ctx), std::move(request));
    }

    // Take the list of blocks that we will execute in the first
    // sub-request and leave the rest in original sgList.
    request->Sglist = TakeHeadBlocks(sgList, requestBlockCount);
    if (request->Sglist.Empty()) {
        return MakeFuture<NProto::TReadBlocksResponse>(TErrorResponse(
            E_CANCELLED,
            "failed to acquire sglist in DeviceHandler"));
    }

    auto result = Storage->ReadBlocksLocal(ctx, std::move(request));

    blocksInfo.Range = TBlockRange64::WithLength(
        blocksInfo.Range.Start + requestBlockCount,
        blocksInfo.Range.Size() - requestBlockCount);
    Y_DEBUG_ABORT_UNLESS(blocksInfo.Range.Size());

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         blocksInfo = blocksInfo,
         sgList = std::move(sgList),
         checkpointId = std::move(checkpointId)](const auto& future) mutable
        {
            auto response = future.GetValue();
            if (HasError(response)) {
                return MakeFuture(response);
            }

            if (auto self = weakPtr.lock()) {
                return self->ExecuteReadRequest(
                    std::move(ctx),
                    blocksInfo,
                    std::move(sgList),
                    std::move(checkpointId));
            }
            return MakeFuture<NProto::TReadBlocksResponse>(
                TErrorResponse(E_CANCELLED));
        });
}

TFuture<NProto::TWriteBlocksResponse>
TAlignedDeviceHandler::ExecuteWriteRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo,
    TGuardedSgList sgList) const
{
    auto requestBlockCount =
        std::min<ui32>(blocksInfo.Range.Size(), MaxBlockCount);

    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->MutableHeaders()->SetRequestId(ctx->RequestId);
    request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetStartIndex(blocksInfo.Range.Start);
    request->BlocksCount = requestBlockCount;
    request->BlockSize = BlockSize;

    if (requestBlockCount == blocksInfo.Range.Size()) {
        // The request size is quite small. We do all work at once.
        request->Sglist = std::move(sgList);
        return Storage->WriteBlocksLocal(std::move(ctx), std::move(request));
    }

    // Take the list of blocks that we will execute in the first
    // sub-request and leave the rest in original sgList.
    request->Sglist = TakeHeadBlocks(sgList, requestBlockCount);
    if (request->Sglist.Empty()) {
        return MakeFuture<NProto::TWriteBlocksResponse>(TErrorResponse(
            E_CANCELLED,
            "failed to acquire sglist in DeviceHandler"));
    }

    auto result = Storage->WriteBlocksLocal(ctx, std::move(request));

    blocksInfo.Range = TBlockRange64::WithLength(
        blocksInfo.Range.Start + requestBlockCount,
        blocksInfo.Range.Size() - requestBlockCount);
    Y_DEBUG_ABORT_UNLESS(blocksInfo.Range.Size());

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         blocksInfo = blocksInfo,
         sgList = std::move(sgList)](const auto& future) mutable
        {
            auto response = future.GetValue();
            if (HasError(response)) {
                return MakeFuture(response);
            }

            if (auto self = weakPtr.lock()) {
                return self->ExecuteWriteRequest(
                    std::move(ctx),
                    blocksInfo,
                    std::move(sgList));
            }
            return MakeFuture<NProto::TWriteBlocksResponse>(
                TErrorResponse(E_CANCELLED));
        });
}

TFuture<NProto::TZeroBlocksResponse> TAlignedDeviceHandler::ExecuteZeroRequest(
    TCallContextPtr ctx,
    ui64 startIndex,
    ui32 blockCount) const
{
    auto requestBlockCount = std::min(blockCount, MaxBlockCount);

    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->MutableHeaders()->SetRequestId(ctx->RequestId);
    request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(requestBlockCount);

    if (requestBlockCount == blockCount) {
        // The request size is quite small. We do all work at once.
        return Storage->ZeroBlocks(std::move(ctx), std::move(request));
    }

    auto result = Storage->ZeroBlocks(ctx, std::move(request));

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         startIndex = startIndex + requestBlockCount,
         blocksCount =
             blockCount - requestBlockCount](const auto& future) mutable
        {
            // Only part of the request was completed. Continue doing the
            // rest of the work

            auto response = future.GetValue();
            if (HasError(response)) {
                return MakeFuture(response);
            }

            if (auto self = weakPtr.lock()) {
                return self->ExecuteZeroRequest(
                    std::move(ctx),
                    startIndex,
                    blocksCount);
            }
            return MakeFuture<NProto::TZeroBlocksResponse>(
                TErrorResponse(E_CANCELLED));
        });
}

}   // namespace NCloud::NBlockStore
