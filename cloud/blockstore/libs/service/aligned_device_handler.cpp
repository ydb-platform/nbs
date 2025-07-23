#include "aligned_device_handler.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/checksum_storage_wrapper.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/media.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

TErrorResponse CreateErrorAcquireResponse()
{
    return {E_CANCELLED, "failed to acquire sglist in DeviceHandler"};
}

TErrorResponse CreateRequestNotAlignedResponse()
{
    return {E_ARGUMENT, "Request is not aligned"};
}

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

NProto::TError TryToNormalize(
    TGuardedSgList& guardedSgList,
    TBlocksInfo& blocksInfo)
{
    const auto length = blocksInfo.BufferSize();
    if (length == 0) {
        return MakeError(E_ARGUMENT, "Local request has zero length");
    }

    auto guard = guardedSgList.Acquire();
    if (!guard) {
        return CreateErrorAcquireResponse();
    }

    auto bufferSize = SgListGetSize(guard.Get());
    if (bufferSize != length) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Invalid local request: buffer size " << bufferSize
                << " not equal to length " << length);
    }

    if (!blocksInfo.IsAligned()) {
        return MakeError(S_OK);
    }

    bool allBuffersAligned = AllOf(
        guard.Get(),
        [blockSize = blocksInfo.BlockSize](const auto& buffer)
        { return buffer.Size() % blockSize == 0; });

    if (!allBuffersAligned) {
        blocksInfo.SgListAligned = false;
        return MakeError(S_OK);
    }

    auto sgListOrError = SgListNormalize(guard.Get(), blocksInfo.BlockSize);
    if (HasError(sgListOrError)) {
        return sgListOrError.GetError();
    }

    guardedSgList.SetSgList(sgListOrError.ExtractResult());
    return MakeError(S_OK);
}

////////////////////////////////////////////////////////////////////////////////

TAlignedDeviceHandler::TAlignedDeviceHandler(
        IStoragePtr storage,
        TString diskId,
        TString clientId,
        ui32 blockSize,
        ui32 maxSubRequestSize,
        ui32 maxZeroBlocksSubRequestSize,
        bool checkBufferModificationDuringWriting,
        NProto::EStorageMediaKind storageMediaKind)
    : Storage(
          checkBufferModificationDuringWriting
              ? CreateChecksumStorageWrapper(std::move(storage), diskId)
              : std::move(storage))
    , DiskId(std::move(diskId))
    , ClientId(std::move(clientId))
    , BlockSize(blockSize)
    , MaxBlockCount(maxSubRequestSize / BlockSize)
    , MaxBlockCountForZeroBlocksRequest(maxZeroBlocksSubRequestSize / BlockSize)
    , StorageMediaKind(storageMediaKind)
{
    Y_ABORT_UNLESS(MaxBlockCount > 0);
    Y_ABORT_UNLESS(MaxBlockCountForZeroBlocksRequest > 0);
}

TFuture<NProto::TReadBlocksLocalResponse> TAlignedDeviceHandler::Read(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList,
    const TString& checkpointId)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    auto normalizeError = TryToNormalize(sgList, blocksInfo);
    if (HasError(normalizeError)) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            TErrorResponse(normalizeError));
    }

    if (!blocksInfo.IsAligned()) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            CreateRequestNotAlignedResponse());
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

    auto normalizeError = TryToNormalize(sgList, blocksInfo);
    if (HasError(normalizeError)) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse(normalizeError));
    }

    if (!blocksInfo.IsAligned()) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            CreateRequestNotAlignedResponse());
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
    if (!blocksInfo.IsAligned()) {
        return MakeFuture<NProto::TZeroBlocksResponse>(
            CreateRequestNotAlignedResponse());
    }

    return ExecuteZeroRequest(std::move(ctx), blocksInfo);
}

TStorageBuffer TAlignedDeviceHandler::AllocateBuffer(size_t bytesCount)
{
    auto buffer = Storage->AllocateBuffer(bytesCount);
    if (!buffer) {
        buffer = std::shared_ptr<char>(
            new char[bytesCount],
            std::default_delete<char[]>());
    }
    return buffer;
}

TFuture<NProto::TReadBlocksLocalResponse>
TAlignedDeviceHandler::ExecuteReadRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo,
    TGuardedSgList sgList,
    TString checkpointId)
{
    Y_DEBUG_ABORT_UNLESS(blocksInfo.IsAligned());

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
        auto result =
            Storage->ReadBlocksLocal(std::move(ctx), std::move(request));
        return result.Subscribe(
            [weakPtr = weak_from_this(), range = blocksInfo.Range](
                const TFuture<NProto::TReadBlocksLocalResponse>& future)
            {
                const auto& response = future.GetValue();
                if (HasError(response)) {
                    if (auto self = weakPtr.lock()) {
                        self->ReportCriticalError(
                            response.GetError(),
                            "Read",
                            range);
                    }
                }
            });
    }

    // Take the list of blocks that we will execute in the first
    // sub-request and leave the rest in original sgList.
    request->Sglist = TakeHeadBlocks(sgList, requestBlockCount);
    if (request->Sglist.Empty()) {
        return MakeFuture<NProto::TReadBlocksLocalResponse>(
            CreateErrorAcquireResponse());
    }

    auto result = Storage->ReadBlocksLocal(ctx, std::move(request));

    auto originalRange = blocksInfo.Range;
    blocksInfo.Range = TBlockRange64::WithLength(
        blocksInfo.Range.Start + requestBlockCount,
        blocksInfo.Range.Size() - requestBlockCount);
    Y_DEBUG_ABORT_UNLESS(blocksInfo.Range.Size());

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         blocksInfo = blocksInfo,
         sgList = std::move(sgList),
         checkpointId = std::move(checkpointId),
         originalRange = originalRange](
            const TFuture<NProto::TReadBlocksLocalResponse>& future) mutable
        {
            const auto& response = future.GetValue();
            if (HasError(response)) {
                if (auto self = weakPtr.lock()) {
                    self->ReportCriticalError(
                        response.GetError(),
                        "Read",
                        originalRange);
                }
                return future;
            }

            if (auto self = weakPtr.lock()) {
                return self->ExecuteReadRequest(
                    std::move(ctx),
                    blocksInfo,
                    std::move(sgList),
                    std::move(checkpointId));
            }
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse(E_CANCELLED));
        });
}

TFuture<NProto::TWriteBlocksResponse>
TAlignedDeviceHandler::ExecuteWriteRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo,
    TGuardedSgList sgList)
{
    Y_DEBUG_ABORT_UNLESS(blocksInfo.IsAligned());

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
        auto result =
            Storage->WriteBlocksLocal(std::move(ctx), std::move(request));
        return result.Subscribe(
            [weakPtr = weak_from_this(), range = blocksInfo.Range](
                const TFuture<NProto::TWriteBlocksResponse>& future)
            {
                const auto& response = future.GetValue();
                if (HasError(response)) {
                    if (auto self = weakPtr.lock()) {
                        self->ReportCriticalError(
                            response.GetError(),
                            "Write",
                            range);
                    }
                }
            });
    }

    // Take the list of blocks that we will execute in the first
    // sub-request and leave the rest in original sgList.
    request->Sglist = TakeHeadBlocks(sgList, requestBlockCount);
    if (request->Sglist.Empty()) {
        return MakeFuture<NProto::TWriteBlocksResponse>(
            CreateErrorAcquireResponse());
    }

    auto result = Storage->WriteBlocksLocal(ctx, std::move(request));

    auto originalRange = blocksInfo.Range;
    blocksInfo.Range = TBlockRange64::WithLength(
        blocksInfo.Range.Start + requestBlockCount,
        blocksInfo.Range.Size() - requestBlockCount);
    Y_DEBUG_ABORT_UNLESS(blocksInfo.Range.Size());

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         blocksInfo = blocksInfo,
         sgList = std::move(sgList),
         originalRange = originalRange](
            const TFuture<NProto::TWriteBlocksResponse>& future) mutable
        {
            const auto& response = future.GetValue();
            if (HasError(response)) {
                if (auto self = weakPtr.lock()) {
                    self->ReportCriticalError(
                        response.GetError(),
                        "Write",
                        originalRange);
                }
                return future;
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
    TBlocksInfo blocksInfo)
{
    Y_DEBUG_ABORT_UNLESS(blocksInfo.IsAligned());

    auto requestBlockCount = std::min<ui32>(
        blocksInfo.Range.Size(),
        MaxBlockCountForZeroBlocksRequest);

    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->MutableHeaders()->SetRequestId(ctx->RequestId);
    request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
    request->MutableHeaders()->SetClientId(ClientId);
    request->SetStartIndex(blocksInfo.Range.Start);
    request->SetBlocksCount(requestBlockCount);

    if (requestBlockCount == blocksInfo.Range.Size()) {
        // The request size is quite small. We do all work at once.
        auto result = Storage->ZeroBlocks(std::move(ctx), std::move(request));
        return result.Subscribe(
            [weakPtr = weak_from_this(),
             range = blocksInfo.Range](const TFuture<NProto::TZeroBlocksResponse>& future)
            {
                const auto& response = future.GetValue();
                if (HasError(response)) {
                    if (auto self = weakPtr.lock()) {
                        self->ReportCriticalError(
                            response.GetError(),
                            "Zero",
                            range);
                    }
                }
            });
    }

    auto result = Storage->ZeroBlocks(ctx, std::move(request));

    auto originalRange = blocksInfo.Range;
    blocksInfo.Range.Start += requestBlockCount;

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         blocksInfo = blocksInfo,
         originalRange = originalRange](
            const TFuture<NProto::TZeroBlocksResponse>& future) mutable
        {
            // Only part of the request was completed. Continue doing the
            // rest of the work

            const auto& response = future.GetValue();
            if (HasError(response)) {
                if (auto self = weakPtr.lock()) {
                    self->ReportCriticalError(
                        response.GetError(),
                        "Zero",
                        originalRange);
                }
                return future;
            }

            if (auto self = weakPtr.lock()) {
                return self->ExecuteZeroRequest(std::move(ctx), blocksInfo);
            }
            return MakeFuture<NProto::TZeroBlocksResponse>(
                TErrorResponse(E_CANCELLED));
        });
}

void TAlignedDeviceHandler::ReportCriticalError(
    const NProto::TError& error,
    const TString& operation,
    TBlockRange64 range)
{
    if (error.GetCode() == E_CANCELLED) {
        // Do not raise crit event when client disconnected.
        // Keep the logic synchronized with blockstore/libs/vhost/server.cpp.
        return;
    }

    if (error.GetCode() == E_IO_SILENT &&
        (error.GetMessage().Contains(
             "Request WriteBlocks is not allowed for client") ||
         error.GetMessage().Contains(
             "Request WriteBlocksLocal is not allowed for client") ||
         error.GetMessage().Contains(
             "Request ZeroBlocks is not allowed for client")))
    {
        // Don't raise crit event when client try to write with read-only mount.
        // See cloud/blockstore/libs/storage/volume/model/client_state.cpp
        return;
    }

    if (IsReliableMediaKind(StorageMediaKind)) {
        CriticalErrorReported.store(true);
    } else {
        // For non-reliable disks report crit event only once.
        bool old = CriticalErrorReported.load();
        if (old) {
            return;
        }
        bool ok = CriticalErrorReported.compare_exchange_strong(old, true);
        if (!ok) {
            return;
        }
    }

    auto message = TStringBuilder()
                   << "disk: " << DiskId.Quote() << ", op: " << operation
                   << ", range: " << range << ", error: " << FormatError(error);
    if (IsReliableMediaKind(StorageMediaKind)) {
        ReportErrorWasSentToTheGuestForReliableDisk(message);
    } else {
        ReportErrorWasSentToTheGuestForNonReliableDisk(message);
    }
}

}   // namespace NCloud::NBlockStore
