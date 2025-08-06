#include "checksum_storage_wrapper.h"

#include "storage.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
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

std::optional<ui32> CalcChecksum(const TGuardedSgList& sgList)
{
    auto guard = sgList.Acquire();
    if (!guard) {
        return std::nullopt;
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

class TChecksumStorageWrapper final
    : public std::enable_shared_from_this<TChecksumStorageWrapper>
    , public IStorage
{
    const IStoragePtr Storage;
    const TString DiskId;

    std::atomic<bool> CriticalErrorReported = false;

public:
    TChecksumStorageWrapper(IStoragePtr storage, TString diskId)
        : Storage(std::move(storage))
        , DiskId(std::move(diskId))
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

private:
    void ReportChecksumMismatch(TBlockRange64 range);
};

TFuture<NProto::TWriteBlocksLocalResponse>
TChecksumStorageWrapper::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    auto requestCopy =
        std::make_shared<NProto::TWriteBlocksLocalRequest>(*request);
    auto checksumBefore = CalcChecksum(request->Sglist);
    if (!checksumBefore) {
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
            CreateRequestDestroyedResponse());
    }

    auto result = Storage->WriteBlocksLocal(callContext, std::move(request));

    return result.Apply(
        [callContext = std::move(callContext),
         checksumBefore = *checksumBefore,
         requestCopy = std::move(requestCopy),
         weakSelf = weak_from_this()](const auto& future) mutable
        {
            auto response = future.GetValue();
            if (HasError(response)) {
                return MakeFuture(std::move(response));
            }

            auto checksumAfter = CalcChecksum(requestCopy->Sglist);

            if (checksumAfter && checksumBefore != *checksumAfter) {
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

TStorageBuffer TChecksumStorageWrapper::AllocateBuffer(size_t bytesCount)
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
TChecksumStorageWrapper::RetryWriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    const auto range = TBlockRange64::WithLength(
        request->GetStartIndex(),
        request->BlocksCount);

    ReportChecksumMismatch(range);

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

void TChecksumStorageWrapper::ReportChecksumMismatch(TBlockRange64 range)
{
    // Report critical event only once.
    bool old = CriticalErrorReported.load();
    if (old) {
        return;
    }
    bool ok = CriticalErrorReported.compare_exchange_strong(old, true);
    if (!ok) {
        return;
    }

    ReportMirroredDiskChecksumMismatchUponWrite(
        {{"disk", DiskId}, {"range", range}});
}

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateChecksumStorageWrapper(IStoragePtr storage, TString diskId)
{
    return std::make_shared<TChecksumStorageWrapper>(
        std::move(storage),
        std::move(diskId));
}

}   // namespace NCloud::NBlockStore
