#include "memory_safe_storage.h"

#include "storage.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>

using namespace NThreading;

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TErrorResponse CreateBackendDestroyedResponse()
{
    return {E_CANCELLED, "backend destroyed"};
}

TErrorResponse CreateRequestDestroyedResponse()
{
    return {E_CANCELLED, "request destroyed"};
}

ui32 CalcChecksum(const TGuardedSgList& sgList)
{
    auto guard = sgList.Acquire();
    if (!guard) {
        return 0;
    }

    const TSgList& blockList = guard.Get();

    TBlockChecksum checksum;
    for (const auto& block: blockList) {
        checksum.Extend(block.Data(), block.Size());
    }

    return checksum.GetValue();
}

TSgList RebaseSgList(const TSgList& src, TBlockDataRef newPtr)
{
    TSgList result;
    result.reserve(src.size());
    size_t offset = 0;
    for (const auto& block: src) {
        result.push_back(TBlockDataRef{newPtr.Data() + offset, block.Size()});
        offset += block.Size();
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TMemorySafeStorageWrapper final
    : public std::enable_shared_from_this<TMemorySafeStorageWrapper>
    , public IStorage
{
    const IStoragePtr Storage;

public:
    explicit TMemorySafeStorageWrapper(IStoragePtr storage)
        : Storage(std::move(storage))
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Storage->ZeroBlocks(std::move(callContext), std::move(request));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Storage->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        return Storage->EraseDevice(method);
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

    void ReportIOError() override
    {
        Storage->ReportIOError();
    }

    TFuture<NProto::TWriteBlocksLocalResponse> RetryWriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request);
};

TFuture<NProto::TWriteBlocksLocalResponse>
TMemorySafeStorageWrapper::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    auto requestCopy =
        std::make_shared<NProto::TWriteBlocksLocalRequest>(*request);
    ui32 checksum = CalcChecksum(request->Sglist);
    auto result = Storage->WriteBlocksLocal(callContext, std::move(request));
    return result.Apply(
        [callContext = std::move(callContext),
         checksum = checksum,
         requestCopy = std::move(requestCopy),
         weakSelf = weak_from_this()](const auto& future) mutable
        {
            auto response = future.GetValue();
            if (HasError(response)) {
                return MakeFuture(std::move(response));
            }

            ui32 checksumAfter =
                checksum ? CalcChecksum(requestCopy->Sglist) : 0;

            if (checksum != checksumAfter && checksumAfter) {
                if (auto self = weakSelf.lock()) {
                    return self->RetryWriteBlocksLocal(
                        std::move(callContext),
                        std::move(requestCopy));
                }
                return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                    CreateBackendDestroyedResponse());
            }
            return MakeFuture(std::move(response));
        });
}

TStorageBuffer TMemorySafeStorageWrapper::AllocateBuffer(size_t bytesCount)
{
    auto buffer = Storage->AllocateBuffer(bytesCount);
    if (!buffer) {
        buffer = std::shared_ptr<char>(
            new char[bytesCount],
            std::default_delete<char[]>());
    }
    return buffer;
}

TFuture<NProto::TWriteBlocksLocalResponse>
TMemorySafeStorageWrapper::RetryWriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            CreateRequestDestroyedResponse());
    }

    const TSgList& originalBlockList = guard.Get();
    const auto dataSize = SgListGetSize(originalBlockList);
    auto buffer = AllocateBuffer(dataSize);
    TBlockDataRef newData(buffer.get(), dataSize);
    SgListCopy(originalBlockList, newData);
    request->Sglist.SetSgList(RebaseSgList(originalBlockList, newData));

    auto result =
        Storage->WriteBlocksLocal(std::move(callContext), std::move(request));
    return result.Apply(
        [buffer = std::move(buffer)](const auto& future) mutable
        {
            Y_UNUSED(buffer);   // Extending the buffer lifetime
            return future;
        });
}

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateMemorySafeStorageWrapper(IStoragePtr storage)
{
    return std::make_shared<TMemorySafeStorageWrapper>(std::move(storage));
}

}   // namespace NCloud::NBlockStore
