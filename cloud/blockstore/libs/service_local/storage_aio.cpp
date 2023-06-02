#include "storage_aio.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/sglist.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/thread.h>

#include <util/generic/array_ref.h>
#include <util/generic/noncopyable.h>
#include <util/string/builder.h>
#include <util/system/align.h>
#include <util/system/file.h>
#include <util/system/rwlock.h>
#include <util/system/sanitizers.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

#include <limits>
#include <memory>

#if defined(__linux__)
#   include <malloc.h>
#endif

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace NNvme;

namespace {

////////////////////////////////////////////////////////////////////////////////
// This wrapper will Release (and not Close) TFileHandle on scope exit

class TFileHandleRef
    : TNonCopyable
{
private:
    TFileHandle Handle;

public:
    explicit TFileHandleRef(const TFile& file)
        : Handle(file.GetHandle())
    {}

    ~TFileHandleRef() noexcept
    {
        Handle.Release();
    }

    operator TFileHandle& () noexcept
    {
        return Handle;
    }
};

////////////////////////////////////////////////////////////////////////////////

using TAlignedBuffer = std::shared_ptr<char>;

TAlignedBuffer AllocateAligned(size_t byteCount, bool zeroInit)
{
    void* p = memalign(DefaultBlockSize, byteCount);
    Y_VERIFY(p);
    Y_VERIFY_DEBUG((uintptr_t)p % DefaultBlockSize == 0);

    if (zeroInit) {
        memset(p, 0, byteCount);
    }

    return { static_cast<char*>(p), [] (auto* p) { free(p); }};
}

TAlignedBuffer AllocateZero(size_t byteCount)
{
    return AllocateAligned(byteCount, true);
}

TAlignedBuffer AllocateUninitialized(size_t byteCount)
{
    return AllocateAligned(byteCount, false);
}

void UnpoisonSgList(TArrayRef<const TBlockDataRef> sglist, ui32 byteCount)
{
    for (auto buf: sglist) {
        auto size = Min<ui32>(byteCount, buf.Size());
        NSan::Unpoison(buf.Data(), size);
        byteCount -= size;
    }
}

TArrayRef<char> GetNextArrayRef(
    TArrayRef<const TBlockDataRef> sglist,
    ui64 byteCount,
    ui64 offset)
{
    for (auto buf: sglist) {
        if (offset < buf.Size()) {
            return {
                const_cast<char*>(buf.Data() + offset),
                Min<ui64>(byteCount, buf.Size() - offset)
            };
        }

        byteCount -= buf.Size();
        offset -= buf.Size();
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TZeroBuffer
{
public:
    static constexpr ui32 DefaultBufferSize = 64_KB;
    static constexpr ui32 MaxBufferSize = 8_MB;

private:
    ui32 CurrentByteCount = DefaultBufferSize;
    TAlignedBuffer Buffer = AllocateZero(DefaultBufferSize);
    TAdaptiveLock Lock;

public:
    TAlignedBuffer GetCached(ui32 byteCount)
    {
        with_lock (Lock) {
            if (byteCount <= CurrentByteCount) {
                return Buffer;
            }

            if (byteCount <= MaxBufferSize) {
                byteCount = AlignUp<ui32>(byteCount, 4_KB);
                Buffer = AllocateZero(byteCount);
                CurrentByteCount = byteCount;
                return Buffer;
            }
        }

        return AllocateZero(byteCount);
    }
};

TAlignedBuffer PrepareZeroBuffer(ui32 byteCount)
{
    return Singleton<TZeroBuffer>()->GetCached(byteCount);
}

////////////////////////////////////////////////////////////////////////////////

struct TStorageContext
    : TNonCopyable
    , std::enable_shared_from_this<TStorageContext>
{
    const IFileIOServicePtr FileIOService;
    const INvmeManagerPtr NvmeManager;

    const ui32 BlockSize;
    const ui64 StorageStartIndex;
    const ui64 StorageBlockCount;

    TFile File;

    TStorageContext(
            IFileIOServicePtr fileIO,
            INvmeManagerPtr nvmeManager,
            ui32 blockSize,
            ui64 startIndex,
            ui64 blockCount,
            TFile file)
        : FileIOService(std::move(fileIO))
        , NvmeManager(std::move(nvmeManager))
        , BlockSize(blockSize)
        , StorageStartIndex(startIndex)
        , StorageBlockCount(blockCount)
        , File(std::move(file))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TAsyncMethod
{
    struct TRead
    {
        using TResponse = NProto::TReadBlocksLocalResponse;

        static void Execute(
            IFileIOService& fileIO,
            TFileHandle& handle,
            i64 offset,
            TArrayRef<char> buffer,
            TFileIOCompletion* completion)
        {
            fileIO.AsyncRead(
                handle,
                offset,
                buffer,
                completion);
        }
    };

    struct TWrite
    {
        using TResponse = NProto::TWriteBlocksLocalResponse;

        static void Execute(
            IFileIOService& fileIO,
            TFileHandle& handle,
            i64 offset,
            TArrayRef<const char> buffer,
            TFileIOCompletion* completion)
        {
            fileIO.AsyncWrite(
                handle,
                offset,
                buffer,
                completion);
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
static void Complete(
    TFileIOCompletion* completion,
    const NProto::TError& error,
    ui32 bytes)
{
    std::unique_ptr<TRequest> request {static_cast<TRequest*>(completion)};

    ProcessResponse(std::move(request), error, bytes);
}

template <typename TRequest>
void ProcessRequest(std::unique_ptr<TRequest> request)
{
    Y_UNUSED(request->CallContext);  // TODO

    if (auto context = request->Context.lock()) {
        auto& service = *context->FileIOService;
        const ui64 offset = request->FileOffset + request->BytesTransferred;
        const auto buffer = request->GetNextBuffer();
        const auto& file = context->File;

        TFileHandleRef handle(file);

        TRequest::TMethod::Execute(
            service,
            handle,
            offset,
            buffer,
            request.get());

        Y_UNUSED(request.release());    // ownership transferred
    }
}

template <typename TRequest>
void ProcessResponse(
    std::unique_ptr<TRequest> request,
    const NProto::TError& error,
    ui32 bytesTransferred)
{
    if (HasError(error)) {
        CompleteRequest(std::move(request), error);
        return;
    }

    request->BytesTransferred += bytesTransferred;
    if (request->BytesTransferred < request->TotalByteCount) {
        ProcessRequest(std::move(request));
        return;
    }

    Y_VERIFY(request->BytesTransferred == request->TotalByteCount);

    CompleteRequest(std::move(request));
}

template <typename TRequest>
void CompleteRequest(
    std::unique_ptr<TRequest> request,
    const NProto::TError& error = {})
{
    auto p = std::move(request->Response);
    request.reset();

    typename TRequest::TResponse response;
    *response.MutableError() = error;

    p.SetValue(std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

struct TGenericRequest
    : TFileIOCompletion
{
    std::weak_ptr<TStorageContext> Context;

    const TCallContextPtr CallContext;
    const ui64 FileOffset;
    const ui64 TotalByteCount;

    ui64 BytesTransferred = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TReadOrWriteRequest
    : public TGenericRequest
{
    using TMethod = T;
    using TResponse = typename T::TResponse;

    const TGuardedSgList SgList;
    const TGuardedSgList::TGuard Guard;

    TPromise<TResponse> Response;

    TReadOrWriteRequest(
            std::weak_ptr<TStorageContext> context,
            TCallContextPtr callContext,
            TGuardedSgList sglist,
            ui64 fileOffset,
            ui64 byteCount,
            TPromise<TResponse> response)
        : TGenericRequest {
            {.Func = &Complete<TReadOrWriteRequest>},
            std::move(context),
            std::move(callContext),
            fileOffset,
            byteCount
        }
        , SgList(std::move(sglist))
        , Guard(SgList.Acquire())
        , Response(std::move(response))
    {
        UnpoisonSgList(Guard.Get(), byteCount);
    }

    TArrayRef<char> GetNextBuffer()
    {
        auto buffer = GetNextArrayRef(
            Guard.Get(),
            this->TotalByteCount,
            this->BytesTransferred);

        Y_VERIFY(buffer);

        return buffer;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteZeroes
    : TGenericRequest
{
    using TMethod = TAsyncMethod::TWrite;
    using TResponse = NProto::TZeroBlocksResponse;

    TAlignedBuffer Buffer;
    TPromise<TResponse> Response;

    TWriteZeroes(
            std::weak_ptr<TStorageContext> context,
            TCallContextPtr callContext,
            ui64 fileOffset,
            ui64 byteCount,
            TPromise<TResponse> response)
        : TGenericRequest {
            {.Func = &Complete<TWriteZeroes>},
            std::move(context),
            std::move(callContext),
            fileOffset,
            byteCount
        }
        , Response(std::move(response))
    {}

    TArrayRef<char> GetNextBuffer()
    {
        const ui32 byteCount = Min<ui64>(
            TotalByteCount - BytesTransferred,
            TZeroBuffer::MaxBufferSize);

        Buffer = PrepareZeroBuffer(static_cast<ui32>(byteCount));

        return { Buffer.get(), byteCount };
    }
};

using TAsyncReadRequest  = TReadOrWriteRequest<TAsyncMethod::TRead>;
using TAsyncWriteRequest = TReadOrWriteRequest<TAsyncMethod::TWrite>;

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
auto SendAsyncRequest(
    std::weak_ptr<TStorageContext> context,
    TCallContextPtr callContext,
    TGuardedSgList sglist,
    ui64 fileOffset,
    ui32 byteCount)
{
    using TResponse = typename TRequest::TResponse;

    auto response = NewPromise<TResponse>();
    auto request = std::make_unique<TRequest>(
        std::move(context),
        std::move(callContext),
        std::move(sglist),
        fileOffset,
        byteCount,
        response);

    if (!request->Guard) {
        TResponse proto;
        *proto.MutableError() = MakeError(
            E_CANCELLED,
            "failed to acquire sglist in AioStorage");
        response.SetValue(std::move(proto));
    } else {
        ProcessRequest(std::move(request));
    }

    return response;
}

TFuture<NProto::TZeroBlocksResponse> WriteZeroes(
    std::weak_ptr<TStorageContext> context,
    TCallContextPtr callContext,
    ui64 startIndex,
    ui64 blockCount,
    ui32 blockSize)
{
    const ui64 fileOffset { startIndex * blockSize };
    const ui64 byteCount  { blockCount * blockSize };

    auto response = NewPromise<NProto::TZeroBlocksResponse>();

    ProcessRequest(std::make_unique<TWriteZeroes>(
        std::move(context),
        std::move(callContext),
        fileOffset,
        byteCount,
        response));

    return response;
}

////////////////////////////////////////////////////////////////////////////////

class TAioStorage final
    : public IStorage
    , public TStorageContext
{
private:
    static constexpr ui32 MaxRequestSize = 32_MB;

    // Request ordering is not defined by blockstore service, so individual request order is not enforced.
    // However, ordering is controlled by clients and clients need to rely on request _submission_ atomicity.
    // This lock makes sure that async write submissions are done atomically in relation to
    // other write request submissions and read request submissions.
    TRWMutex WriteSubmissionLock;

public:
    using TStorageContext::TStorageContext;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TStorageBuffer AllocateBuffer(size_t byteCount) override;

    TFuture<NProto::TError> EraseDevice(NProto::EDeviceEraseMethod method) override;
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeIOBeyondBoundaryError(
    const char* op,
    ui64 startIndex,
    ui32 blockCount,
    ui64 storageBlockCount)
{
    return MakeError(
        E_ARGUMENT, TStringBuilder() << op <<
            " beyond storage boundary. StartIndex=" << startIndex <<
            " BlockCount=" << blockCount <<
            " StorageBlockCount=" << storageBlockCount);
}

NProto::TError MakeTooBigRequestError(
    const char* op,
    ui64 blockCount,
    ui32 maxBlockCount)
{
    return MakeError(
        E_ARGUMENT, TStringBuilder() << op <<
            " request is too big. BlockCount=" << blockCount <<
            " StorageBlockCount=" << maxBlockCount);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TReadBlocksLocalResponse> TAioStorage::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    if (request->GetStartIndex() + request->GetBlocksCount() > StorageBlockCount) {
        NProto::TReadBlocksLocalResponse response;
        *response.MutableError() = MakeIOBeyondBoundaryError(
            "read",
            request->GetStartIndex(),
            request->GetBlocksCount(),
            StorageBlockCount);
        return MakeFuture(response);
    }

    if (request->GetBlocksCount() > MaxRequestSize / BlockSize) {
        NProto::TReadBlocksLocalResponse response;
        *response.MutableError() = MakeTooBigRequestError(
            "read",
            request->GetBlocksCount(),
            MaxRequestSize / BlockSize);
        return MakeFuture(response);
    }

    const ui64 fileOffset = StorageStartIndex * BlockSize
        + request->GetStartIndex() * BlockSize;
    const ui32 byteCount = request->GetBlocksCount() * BlockSize;

    TReadGuard readGuard(WriteSubmissionLock);

    return SendAsyncRequest<TAsyncReadRequest>(
        this->weak_from_this(),
        std::move(callContext),
        std::move(request->Sglist),
        fileOffset,
        byteCount);
}

TFuture<NProto::TWriteBlocksLocalResponse> TAioStorage::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    if (request->GetStartIndex() + request->BlocksCount > StorageBlockCount) {
        NProto::TWriteBlocksLocalResponse response;
        *response.MutableError() = MakeIOBeyondBoundaryError(
            "write",
            request->GetStartIndex(),
            request->BlocksCount,
            StorageBlockCount);
        return MakeFuture(response);
    }

    if (request->BlocksCount > MaxRequestSize / BlockSize) {
        NProto::TWriteBlocksLocalResponse response;
        *response.MutableError() = MakeTooBigRequestError(
            "write",
            request->BlocksCount,
            MaxRequestSize / BlockSize);
        return MakeFuture(response);
    }

    const ui64 fileOffset = StorageStartIndex * BlockSize
        + request->GetStartIndex() * BlockSize;
    const ui32 byteCount = request->BlocksCount * BlockSize;

    TWriteGuard writeGuard(WriteSubmissionLock);

    return SendAsyncRequest<TAsyncWriteRequest>(
        this->weak_from_this(),
        std::move(callContext),
        std::move(request->Sglist),
        fileOffset,
        byteCount);
}

TFuture<NProto::TZeroBlocksResponse> TAioStorage::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    Y_UNUSED(callContext);

    if (request->GetStartIndex() + request->GetBlocksCount() > StorageBlockCount) {
        NProto::TZeroBlocksResponse response;
        *response.MutableError() = MakeIOBeyondBoundaryError(
            "zero",
            request->GetStartIndex(),
            request->GetBlocksCount(),
            StorageBlockCount);
        return MakeFuture(response);
    }

    TWriteGuard writeGuard(WriteSubmissionLock);

    return WriteZeroes(
        this->weak_from_this(),
        std::move(callContext),
        StorageStartIndex + request->GetStartIndex(),
        request->GetBlocksCount(),
        BlockSize);
}

TFuture<NProto::TError> TAioStorage::EraseDevice(NProto::EDeviceEraseMethod method)
{
    switch (method) {
    case NProto::DEVICE_ERASE_METHOD_ZERO_FILL: {
        TWriteGuard writeGuard(WriteSubmissionLock);

        auto future = WriteZeroes(
            this->weak_from_this(),
            MakeIntrusive<TCallContext>(),  // TODO
            StorageStartIndex,
            StorageBlockCount,
            BlockSize);

        return future.Apply([=] (auto& future) {
            return future.GetValue().GetError();
        });
    }

    case NProto::DEVICE_ERASE_METHOD_USER_DATA_ERASE:
        return NvmeManager->Format(
            File.GetName(),
            NVME_FMT_NVM_SES_USER_DATA_ERASE);

    case NProto::DEVICE_ERASE_METHOD_CRYPTO_ERASE:
        return NvmeManager->Format(
            File.GetName(),
            NVME_FMT_NVM_SES_CRYPTO_ERASE);

    case NProto::DEVICE_ERASE_METHOD_NONE:
        return {};
    }
}

TStorageBuffer TAioStorage::AllocateBuffer(size_t byteCount)
{
    return AllocateUninitialized(byteCount);
}

////////////////////////////////////////////////////////////////////////////////

class TAioStorageProvider final
    : public IStorageProvider
{
private:
    IFileIOServicePtr FileIOService;
    INvmeManagerPtr NvmeManager;
    const bool DirectIO;

public:
    explicit TAioStorageProvider(
            IFileIOServicePtr fileIO,
            INvmeManagerPtr nvmeManager,
            bool directIO)
        : FileIOService(std::move(fileIO))
        , NvmeManager(std::move(nvmeManager))
        , DirectIO(directIO)
    {}

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        Y_UNUSED(clientId);

        const TString& filePath = volume.GetDiskId();
        bool write = IsReadWriteMode(accessMode);

        ui32 blockSize = volume.GetBlockSize();
        if (!blockSize) {
            blockSize = DefaultBlockSize;
        }

        const EOpenMode flags = EOpenModeFlag::OpenExisting
                | (write
                    ? EOpenModeFlag::RdWr
                    : EOpenModeFlag::RdOnly)
                | (DirectIO
                    ? EOpenModeFlag::DirectAligned | EOpenModeFlag::Sync
                    : EOpenModeFlag());

        auto storage = std::make_shared<TAioStorage>(
            FileIOService,
            NvmeManager,
            blockSize,
            volume.GetStartIndex(),
            volume.GetBlocksCount(),
            TFile {filePath, flags});

        return MakeFuture<IStoragePtr>(storage);
    };
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateAioStorageProvider(
    IFileIOServicePtr fileIO,
    INvmeManagerPtr nvmeManager,
    bool directIO)
{
    return std::make_shared<TAioStorageProvider>(
        std::move(fileIO),
        std::move(nvmeManager),
        directIO);
}

}   // namespace NCloud::NBlockStore::NServer
