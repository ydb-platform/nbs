#include "storage_local.h"

#include "file_io_service_provider.h"
#include "safe_deallocator.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <util/generic/array_ref.h>
#include <util/generic/noncopyable.h>
#include <util/generic/size_literals.h>
#include <util/random/fast.h>
#include <util/string/builder.h>
#include <util/system/align.h>
#include <util/system/file.h>
#include <util/system/rwlock.h>
#include <util/system/sanitizers.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

#include <google/protobuf/util/message_differencer.h>

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
    Y_ABORT_UNLESS(p);
    Y_DEBUG_ABORT_UNLESS((uintptr_t)p % DefaultBlockSize == 0);

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
    const ITaskQueuePtr SubmitQueue;
    const IFileIOServicePtr FileIOService;
    const INvmeManagerPtr NvmeManager;

    const ui32 BlockSize;
    const ui64 StorageStartIndex;
    const ui64 StorageBlockCount;

    TFile File;
    const bool DirectIO;
    const bool EnableDataIntegrityValidation;

    TStorageContext(
            ITaskQueuePtr submitQueue,
            IFileIOServicePtr fileIO,
            INvmeManagerPtr nvmeManager,
            ui32 blockSize,
            ui64 startIndex,
            ui64 blockCount,
            TFile file,
            bool directIO,
            bool enableChecksumValidation)
        : SubmitQueue(std::move(submitQueue))
        , FileIOService(std::move(fileIO))
        , NvmeManager(std::move(nvmeManager))
        , BlockSize(blockSize)
        , StorageStartIndex(startIndex)
        , StorageBlockCount(blockCount)
        , File(std::move(file))
        , DirectIO(directIO)
        , EnableDataIntegrityValidation(enableChecksumValidation)
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

    Y_ABORT_UNLESS(request->BytesTransferred == request->TotalByteCount);

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

        Y_ABORT_UNLESS(buffer);

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
            "failed to acquire sglist in Local Storage");
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

class TLocalStorage final
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

    void ReportIOError() override;

    TFuture<NProto::TError> EraseDevice(NProto::EDeviceEraseMethod method) override;

private:
    TFuture<NProto::TZeroBlocksResponse> DoZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request);

    TFuture<NProto::TReadBlocksLocalResponse> DoReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request);

    TFuture<NProto::TWriteBlocksLocalResponse> DoWriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request);

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

TFuture<NProto::TReadBlocksLocalResponse> TLocalStorage::DoReadBlocksLocal(
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

    auto sglist = request->Sglist;
    TFuture<NProto::TReadBlocksLocalResponse> future =
        SendAsyncRequest<TAsyncReadRequest>(
            this->weak_from_this(),
            std::move(callContext),
            std::move(request->Sglist),
            fileOffset,
            byteCount)
            .GetFuture();
    if (!EnableDataIntegrityValidation) {
        return future;
    }

    return future.Apply(
        [sglist = std::move(sglist)](auto future)
        {
            NProto::TReadBlocksLocalResponse response =
                future.ExtractValueSync();
            if (HasError(response.GetError())) {
                return response;
            }

            if (auto guard = sglist.Acquire()) {
                *response.MutableChecksum() = CalculateChecksum(guard.Get());
            }
            return response;
        });
}

TFuture<NProto::TWriteBlocksLocalResponse> TLocalStorage::DoWriteBlocksLocal(
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

    if (EnableDataIntegrityValidation && request->ChecksumsSize() > 0) {
        if (request->ChecksumsSize() != 1) {
            NProto::TWriteBlocksLocalResponse response;
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
            *response.MutableError() = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Invalid checksum count: " << request->ChecksumsSize(),
                flags);
            return MakeFuture(response);
        }

        auto guard = request->Sglist.Acquire();
        if (!guard) {
            NProto::TWriteBlocksLocalResponse response;
            *response.MutableError() = MakeError(
                E_CANCELLED,
                "failed to acquire sglist in Local Storage");
            return MakeFuture(response);
        }

        auto calculatedChecksum = CalculateChecksum(guard.Get());
        if (!google::protobuf::util::MessageDifferencer::Equals(
                request->GetChecksums(0),
                calculatedChecksum))
        {
            NProto::TWriteBlocksLocalResponse response;
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
            *response.MutableError() = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Data integrity violation. Current checksum: "
                    << calculatedChecksum.ShortUtf8DebugString().Quote()
                    << "; Incoming checksum: "
                    << request->GetChecksums(0).ShortUtf8DebugString().Quote(),
                flags);
            return MakeFuture(response);
        }
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

TFuture<NProto::TZeroBlocksResponse> TLocalStorage::DoZeroBlocks(
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

TFuture<NProto::TZeroBlocksResponse> TLocalStorage::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    return SubmitQueue->Execute(
        [this, ctx = std::move(callContext), req = std::move(request)] () mutable {
            return DoZeroBlocks(std::move(ctx), std::move(req));
        });
}

TFuture<NProto::TReadBlocksLocalResponse> TLocalStorage::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    return SubmitQueue->Execute(
        [this, ctx = std::move(callContext), req = std::move(request)] () mutable {
            return DoReadBlocksLocal(std::move(ctx), std::move(req));
        });
}

TFuture<NProto::TWriteBlocksLocalResponse> TLocalStorage::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    return SubmitQueue->Execute(
        [this, ctx = std::move(callContext), req = std::move(request)] () mutable {
            return DoWriteBlocksLocal(std::move(ctx), std::move(req));
        });
}

TFuture<NProto::TError> TLocalStorage::EraseDevice(
    NProto::EDeviceEraseMethod method)
{
    if (method != NProto::DEVICE_ERASE_METHOD_NONE &&
        method != NProto::DEVICE_ERASE_METHOD_ZERO_FILL)
    {
        auto isSsdOrError = NvmeManager->IsSsd(File.GetName());
        if (HasError(isSsdOrError) || !isSsdOrError.GetResult() || !DirectIO) {
            // fallback to zero erase for mechanical disk
            // if we don't use DirectIO there is chance that os cache
            // will return deallocated data so it's not safe to use deallocate
            method = NProto::DEVICE_ERASE_METHOD_ZERO_FILL;
        }
    }

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

    case NProto::DEVICE_ERASE_METHOD_DEALLOCATE: {
        const EOpenMode flags =
            EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly |
            (DirectIO ? EOpenModeFlag::DirectAligned | EOpenModeFlag::Sync
                      : EOpenModeFlag());

        return SafeDeallocateDevice(
            File.GetName(),
            TFileHandle{File.GetName(), flags},
            FileIOService,
            StorageStartIndex,
            StorageBlockCount,
            BlockSize,
            NvmeManager);
    }

    case NProto::DEVICE_ERASE_METHOD_NONE:
        return {};
    }
}

TStorageBuffer TLocalStorage::AllocateBuffer(size_t byteCount)
{
    return AllocateUninitialized(byteCount);
}

void TLocalStorage::ReportIOError()
{}

////////////////////////////////////////////////////////////////////////////////

class TLocalStorageProvider final
    : public IStorageProvider
{
private:
    ITaskQueuePtr SubmitQueue;
    IFileIOServiceProviderPtr FileIOServiceProvider;
    INvmeManagerPtr NvmeManager;
    const bool DirectIO;
    const bool EnableDataIntegrityValidation;

public:
    explicit TLocalStorageProvider(
            ITaskQueuePtr submitQueue,
            IFileIOServiceProviderPtr fileIOProvider,
            INvmeManagerPtr nvmeManager,
            bool directIO,
            bool enableChecksumValidation)
        : SubmitQueue(std::move(submitQueue))
        , FileIOServiceProvider(std::move(fileIOProvider))
        , NvmeManager(std::move(nvmeManager))
        , DirectIO(directIO)
        , EnableDataIntegrityValidation(enableChecksumValidation)
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

        auto storage = std::make_shared<TLocalStorage>(
            SubmitQueue,
            FileIOServiceProvider->CreateFileIOService(filePath),
            NvmeManager,
            blockSize,
            volume.GetStartIndex(),
            volume.GetBlocksCount(),
            TFile {filePath, flags},
            DirectIO,
            EnableDataIntegrityValidation);

        return MakeFuture<IStoragePtr>(storage);
    };
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateLocalStorageProvider(
    IFileIOServiceProviderPtr fileIOProvider,
    INvmeManagerPtr nvmeManager,
    TLocalStorageProviderParams params)
{
    ITaskQueuePtr submitQueue =
        params.UseSubmissionThread
            ? CreateThreadPool(params.SubmissionThreadName, 1)
            : CreateTaskQueueStub();
    submitQueue->Start();

    return std::make_shared<TLocalStorageProvider>(
        std::move(submitQueue),
        std::move(fileIOProvider),
        std::move(nvmeManager),
        params.DirectIO,
        params.EnableDataIntegrityValidation);
}

}   // namespace NCloud::NBlockStore::NServer
