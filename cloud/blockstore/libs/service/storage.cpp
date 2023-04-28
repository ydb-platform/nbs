#include "storage.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/sglist.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageStub final
    : public IStorage
{
public:
    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture<NProto::TZeroBlocksResponse>();
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture<NProto::TReadBlocksResponse>();
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture<NProto::TWriteBlocksResponse>();
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return MakeFuture<NProto::TError>();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateStorageStub()
{
    return std::make_shared<TStorageStub>();
}

////////////////////////////////////////////////////////////////////////////////

class TStorageAdapter::TImpl
{
private:
    const IStoragePtr Storage;
    const ui32 StorageBlockSize;
    const bool Normalize;
    const ui32 MaxRequestSize;

public:
    TImpl(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,
        ui32 maxRequestSize);

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        ui32 requestBlockSize) const;

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        ui32 requestBlockSize) const;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        ui32 requestBlockSize) const;

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) const;

private:
    void VerifyBlockSize(ui32 blockSize) const;

    ui32 VerifyRequestSize(const NProto::TIOVector& iov) const;
    ui32 VerifyRequestSize(ui32 blocksCount, ui32 blockSize) const;
};

////////////////////////////////////////////////////////////////////////////////

TStorageAdapter::TImpl::TImpl(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,
        ui32 maxRequestSize)
    : Storage(std::move(storage))
    , StorageBlockSize(storageBlockSize)
    , Normalize(normalize)
    , MaxRequestSize(maxRequestSize)
{}

TFuture<NProto::TReadBlocksResponse> TStorageAdapter::TImpl::ReadBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    ui32 requestBlockSize) const
{
    const auto bytesCount = VerifyRequestSize(
        request->GetBlocksCount(),
        requestBlockSize);

    ui64 localStartIndex;
    ui32 localBlocksCount;

    if (requestBlockSize == StorageBlockSize) {
        localStartIndex = request->GetStartIndex();
        localBlocksCount = request->GetBlocksCount();
    } else {
        VerifyBlockSize(requestBlockSize);
        localStartIndex =
            request->GetStartIndex() * (requestBlockSize / StorageBlockSize);
        localBlocksCount = bytesCount / StorageBlockSize;
    }

    auto localRequest = std::make_shared<NProto::TReadBlocksLocalRequest>();
    *localRequest->MutableHeaders() = request->GetHeaders();
    localRequest->SetDiskId(request->GetDiskId());
    localRequest->SetStartIndex(localStartIndex);
    localRequest->SetBlocksCount(localBlocksCount);
    localRequest->SetFlags(request->GetFlags());
    localRequest->SetCheckpointId(request->GetCheckpointId());
    localRequest->SetSessionId(request->GetSessionId());
    localRequest->BlockSize = StorageBlockSize;

    auto response = std::make_shared<NProto::TReadBlocksResponse>();

    auto buffer = Storage->AllocateBuffer(bytesCount);

    TSgList sgList;

    if (buffer) {
        sgList = {{ buffer.get(), bytesCount }};
    } else {
        sgList = ResizeIOVector(
            *response->MutableBlocks(),
            request->GetBlocksCount(),
            requestBlockSize);
    }

    if (Normalize) {
        if (buffer || requestBlockSize != StorageBlockSize) {
            // not normalized yet
            auto sgListOrError = SgListNormalize(
                std::move(sgList),
                StorageBlockSize);

            if (HasError(sgListOrError)) {
                return MakeFuture<NProto::TReadBlocksResponse>(
                    TErrorResponse(sgListOrError.GetError()));
            }

            sgList = sgListOrError.ExtractResult();
        }
    }

    localRequest->Sglist = TGuardedSgList(std::move(sgList));

    auto requestBlocksCount = request->GetBlocksCount();
    auto guardedSgList = localRequest->Sglist;

    auto future = Storage->ReadBlocksLocal(
        std::move(callContext),
        std::move(localRequest));

    return future.Apply(
        [=, guardedSgList = std::move(guardedSgList)] (const auto& f) mutable {
        const auto& localResponse = f.GetValue();
        guardedSgList.Destroy();

        if (HasError(localResponse)) {
            return localResponse;
        }

        if (buffer) {
            auto sgList = ResizeIOVector(
                *response->MutableBlocks(),
                requestBlocksCount,
                requestBlockSize);

            size_t bytesCopied = SgListCopy(
                { buffer.get(), bytesCount },
                sgList);
            Y_VERIFY(bytesCopied == bytesCount);
        }

        response->MergeFrom(localResponse);
        return *response;
    });
}

TFuture<NProto::TWriteBlocksResponse> TStorageAdapter::TImpl::WriteBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    ui32 requestBlockSize) const
{
    VerifyBlockSize(requestBlockSize);

    const auto bytesCount = VerifyRequestSize(request->GetBlocks());
    const ui32 localBlocksCount = bytesCount / StorageBlockSize;
    const ui64 localStartIndex = requestBlockSize == StorageBlockSize
        ? request->GetStartIndex()
        : request->GetStartIndex() * (requestBlockSize / StorageBlockSize);

    auto localRequest = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    *localRequest->MutableHeaders() = request->GetHeaders();
    localRequest->SetDiskId(request->GetDiskId());
    localRequest->SetStartIndex(localStartIndex);
    localRequest->SetFlags(request->GetFlags());
    localRequest->SetSessionId(request->GetSessionId());
    localRequest->BlocksCount = localBlocksCount;
    localRequest->BlockSize = StorageBlockSize;

    auto sgList = GetSgList(*request);
    auto buffer = Storage->AllocateBuffer(bytesCount);

    if (buffer) {
        TSgList bufferSgList = {{ buffer.get(), bytesCount }};
        size_t bytesCopied = SgListCopy(sgList, bufferSgList);
        Y_VERIFY(bytesCopied == bytesCount);
        sgList = std::move(bufferSgList);
    }

    if (Normalize && sgList.size() != localBlocksCount) {
        // not normalized yet
        auto sgListOrError = SgListNormalize(
            std::move(sgList),
            StorageBlockSize);

        if (HasError(sgListOrError)) {
            return MakeFuture<NProto::TWriteBlocksResponse>(
                TErrorResponse(sgListOrError.GetError()));
        }

        sgList = sgListOrError.ExtractResult();
    }

    localRequest->Sglist = TGuardedSgList(std::move(sgList));
    auto guardedSgList = localRequest->Sglist;

    auto future = Storage->WriteBlocksLocal(
        std::move(callContext),
        std::move(localRequest));

    return future.Subscribe(
        [request = std::move(request),
         buffer = std::move(buffer),
         guardedSgList = std::move(guardedSgList)] (const auto& f) mutable {
        Y_UNUSED(f);

        guardedSgList.Destroy();
        Y_UNUSED(request);
        Y_UNUSED(buffer);
    });
}

TFuture<NProto::TZeroBlocksResponse> TStorageAdapter::TImpl::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    ui32 requestBlockSize) const
{
    if (requestBlockSize == StorageBlockSize) {
        return Storage->ZeroBlocks(std::move(callContext), std::move(request));
    }

    const auto bytesCount = VerifyRequestSize(
        request->GetBlocksCount(),
        requestBlockSize);

    VerifyBlockSize(requestBlockSize);
    auto localStartIndex =
        request->GetStartIndex() * (requestBlockSize / StorageBlockSize);
    auto localBlocksCount = bytesCount / StorageBlockSize;

    auto localRequest = std::make_shared<NProto::TZeroBlocksRequest>();
    *localRequest->MutableHeaders() = request->GetHeaders();
    localRequest->SetDiskId(request->GetDiskId());
    localRequest->SetStartIndex(localStartIndex);
    localRequest->SetBlocksCount(localBlocksCount);
    localRequest->SetFlags(request->GetFlags());
    localRequest->SetSessionId(request->GetSessionId());

    return Storage->ZeroBlocks(std::move(callContext), std::move(localRequest));
}

TFuture<NProto::TError> TStorageAdapter::TImpl::EraseDevice(
    NProto::EDeviceEraseMethod method) const
{
    return Storage->EraseDevice(method);
}

void TStorageAdapter::TImpl::VerifyBlockSize(ui32 blockSize) const
{
    if (blockSize < StorageBlockSize || blockSize % StorageBlockSize != 0) {
        ythrow TServiceError(E_ARGUMENT)
            << "invalid block size: " << blockSize
            << " (storage block size = " << StorageBlockSize << ")";
    }
}

ui32 TStorageAdapter::TImpl::VerifyRequestSize(ui32 blocksCount, ui32 blockSize) const
{
    ui64 bytesCount = static_cast<ui64>(blocksCount) * blockSize;
    if (MaxRequestSize > 0 && bytesCount > MaxRequestSize) {
        ythrow TServiceError(E_ARGUMENT)
            << "invalid request size: " << bytesCount
            << " (max request size = " << MaxRequestSize << ")";
    }
    return static_cast<ui32>(bytesCount);
}

ui32 TStorageAdapter::TImpl::VerifyRequestSize(const NProto::TIOVector& iov) const
{
    ui64 bytesCount = 0;
    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer.size() == 0 || buffer.size() % StorageBlockSize != 0) {
            ythrow TServiceError(E_ARGUMENT)
                << "buffer size (" << buffer.size() << ") is not a multiple of storage block size"
                << " (storage block size = " << StorageBlockSize << ")";
        }

        bytesCount += buffer.size();
    }

    if (MaxRequestSize > 0 && bytesCount > MaxRequestSize) {
        ythrow TServiceError(E_ARGUMENT)
            << "invalid request size: " << bytesCount
            << " (max request size = " << MaxRequestSize << ")";
    }

    return static_cast<ui32>(bytesCount);
}

////////////////////////////////////////////////////////////////////////////////

TStorageAdapter::TStorageAdapter(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,
        ui32 maxRequestSize)
    : Impl(std::make_unique<TImpl>(
        std::move(storage),
        storageBlockSize,
        normalize,
        maxRequestSize))
{}

TStorageAdapter::~TStorageAdapter()
{}

TFuture<NProto::TReadBlocksResponse> TStorageAdapter::ReadBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    ui32 requestBlockSize) const
{
    return Impl->ReadBlocks(
        std::move(callContext),
        std::move(request),
        requestBlockSize);
}

TFuture<NProto::TWriteBlocksResponse> TStorageAdapter::WriteBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    ui32 requestBlockSize) const
{
    return Impl->WriteBlocks(
        std::move(callContext),
        std::move(request),
        requestBlockSize);
}

TFuture<NProto::TZeroBlocksResponse> TStorageAdapter::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    ui32 requestBlockSize) const
{
    return Impl->ZeroBlocks(
        std::move(callContext),
        std::move(request),
        requestBlockSize);
}

TFuture<NProto::TError> TStorageAdapter::EraseDevice(
    NProto::EDeviceEraseMethod method) const
{
    return Impl->EraseDevice(method);
}

}   // namespace NCloud::NBlockStore
