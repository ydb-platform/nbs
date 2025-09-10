#include "storage.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

#include <atomic>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxRequestSize = 32_MB;

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
        return MakeFuture<NProto::TReadBlocksLocalResponse>();
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

    void ReportIOError() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateStorageStub()
{
    return std::make_shared<TStorageStub>();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
class TInflightTracker
{
public:
    enum TInflightRequestId: ui64;

    struct TInflightRequest
    {
        TInstant Ts;
        TPromise<TResponse> Promise;
    };

private:
    const TDuration MaxRequestDuration;
    THashMap<TInflightRequestId, TInflightRequest> Inflight;
    TAdaptiveLock Lock;
    std::atomic<ui64> RequestId = 0;

public:
    explicit TInflightTracker(TDuration maxRequestDuration)
        : MaxRequestDuration(maxRequestDuration)
    {}

    TInflightRequestId RegisterRequest(TInstant ts, TPromise<TResponse> promise)
    {
        const auto id = static_cast<TInflightRequestId>(RequestId.fetch_add(1));
        if (MaxRequestDuration != TDuration::Zero()) {
            TGuard guard(Lock);

            Inflight[id] = TInflightRequest{ts, std::move(promise)};
        }
        return id;
    }

    void UnregisterRequest(TInflightRequestId requestId)
    {
        if (MaxRequestDuration != TDuration::Zero()) {
            TGuard guard(Lock);

            Inflight.erase(requestId);
        }
    }

    [[nodiscard]] bool IsEmpty() const {
        TGuard guard(Lock);
        return Inflight.empty();
    }

    [[nodiscard]] size_t Size() const {
        TGuard guard(Lock);
        return Inflight.size();
    }

    TVector<TInflightRequest> ExtractTimedOut(TInstant now)
    {
        TVector<TInflightRequest> result;
        if (MaxRequestDuration == TDuration::Zero()) {
            return result;
        }

        TGuard guard(Lock);
        TVector<TInflightRequestId> toDelete;
        for (auto& [id, request]: Inflight) {
            const bool isTimedOut = request.Ts + MaxRequestDuration < now;
            if (isTimedOut) {
                Y_DEBUG_ABORT_UNLESS(
                    !request.Promise.HasValue() &&
                    !request.Promise.HasException());

                result.push_back(std::move(request));
                toDelete.push_back(id);
            }
        }
        for (auto id: toDelete) {
            Inflight.erase(id);
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStorageAdapter::TImpl
{
    using TInflightReads = TInflightTracker<NProto::TReadBlocksResponse>;
    using TInflightWrites = TInflightTracker<NProto::TWriteBlocksResponse>;
    using TInflightZeros = TInflightTracker<NProto::TZeroBlocksResponse>;

private:
    const IStoragePtr Storage;
    const ui32 StorageBlockSize;
    const bool Normalize;
    const TDuration MaxRequestDuration;

    std::shared_ptr<TInflightReads> InflightReads{
        std::make_shared<TInflightReads>(MaxRequestDuration)};

    std::shared_ptr<TInflightWrites> InflightWrites{
        std::make_shared<TInflightWrites>(MaxRequestDuration)};

    std::shared_ptr<TInflightZeros> InflightZeros{
        std::make_shared<TInflightZeros>(MaxRequestDuration)};

public:
    TImpl(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,
        TDuration maxRequestDuration);

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        ui32 requestBlockSize,
        TStringBuf dataBuffer) const;

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        ui32 requestBlockSize,
        TStringBuf dataBuffer) const;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        ui32 requestBlockSize) const;

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) const;

    void CheckIOTimeouts(TInstant now);

    void ReportIOError();

    size_t Shutdown(ITimerPtr timer, TDuration duration);

private:
    void VerifyBlockSize(ui32 blockSize) const;

    ui32 VerifyRequestSize(const NProto::TIOVector& iov) const;
    ui32 VerifyRequestSize(ui32 blocksCount, ui32 blockSize) const;
    ui32 VerifyRequestSize(ui32 bytesCount) const;

    template <typename TResponse>
    void CheckIOTimeouts(TInflightTracker<TResponse>& inflights, TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. Public methods can be called from any thread.
TStorageAdapter::TImpl::TImpl(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,
        TDuration maxRequestDuration)
    : Storage(std::move(storage))
    , StorageBlockSize(storageBlockSize)
    , Normalize(normalize)
    , MaxRequestDuration(maxRequestDuration)
{}

TFuture<NProto::TReadBlocksResponse> TStorageAdapter::TImpl::ReadBlocks(
    TInstant now,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    ui32 requestBlockSize,
    TStringBuf dataBuffer) const
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

    TSgList sgList;
    TStorageBuffer buffer;

    if (dataBuffer) {
        sgList = {{dataBuffer.data(), dataBuffer.size()}};
    } else {
        // We are trying to allocate memory for request using Storage. If the memory
        // is not allocated, then we will read immediately to the buffers in the
        // protobuf.
        buffer = Storage->AllocateBuffer(bytesCount);

        if (buffer) {
            sgList = {{ buffer.get(), bytesCount }};
        } else {
            sgList = ResizeIOVector(
                *response->MutableBlocks(),
                request->GetBlocksCount(),
                requestBlockSize);
        }

    }

    if (Normalize) {
        if (dataBuffer || buffer || requestBlockSize != StorageBlockSize) {
            // not normalized yet
            auto sgListOrError =
                SgListNormalize(std::move(sgList), StorageBlockSize);

            if (HasError(sgListOrError)) {
                return MakeFuture<NProto::TReadBlocksResponse>(
                    TErrorResponse(sgListOrError.GetError()));
            }

            sgList = sgListOrError.ExtractResult();
        }
    }

    localRequest->Sglist = TGuardedSgList(std::move(sgList));

    auto guardedSgList = localRequest->Sglist;

    auto future = Storage->ReadBlocksLocal(
        std::move(callContext),
        std::move(localRequest));

    auto promise = NewPromise<NProto::TReadBlocksResponse>();
    const auto id =  InflightReads->RegisterRequest(now, promise);

    future.Subscribe(
        [inflightReads = InflightReads,
         id,
         promise,
         response = std::move(response),
         buffer = std::move(buffer),
         dataBuffer,
         guardedSgList = std::move(guardedSgList),
         requestBlocksCount = request->GetBlocksCount(),
         requestBlockSize,
         bytesCount,
         optimizeNetworkTransfer =
             request->GetHeaders().GetOptimizeNetworkTransfer()](
            const TFuture<NProto::TReadBlocksLocalResponse>& f) mutable
        {
            inflightReads->UnregisterRequest(id);

            const auto& localResponse = f.GetValue();
            guardedSgList.Close();

            if (HasError(localResponse)) {
                // We don't touch the data in response if an error has occurred.
            } else if (buffer) {
                // If we allocated a buffer, then we transfer data from it.
                if (optimizeNetworkTransfer ==
                    NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS)
                {
                    size_t bytesCopied = CopyAndTrimVoidBuffers(
                        {buffer.get(), bytesCount},
                        requestBlocksCount,
                        requestBlockSize,
                        response->MutableBlocks());
                    Y_ABORT_UNLESS(bytesCopied == bytesCount);
                } else {
                    auto sgList = ResizeIOVector(
                        *response->MutableBlocks(),
                        requestBlocksCount,
                        requestBlockSize);

                    size_t bytesCopied =
                        SgListCopy({buffer.get(), bytesCount}, sgList);
                    Y_ABORT_UNLESS(bytesCopied == bytesCount);
                }
            } else {
                if (dataBuffer.empty() &&
                    optimizeNetworkTransfer ==
                        NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS)
                {
                    // If we read the data directly to the final destination,
                    // then we clean out the void buffers where the data
                    // contains only zeros
                    TrimVoidBuffers(*response->MutableBlocks());
                }
            }

            response->MergeFrom(localResponse);
            promise.TrySetValue(std::move(*response));
        });

    return promise;
}

TFuture<NProto::TWriteBlocksResponse> TStorageAdapter::TImpl::WriteBlocks(
    TInstant now,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    ui32 requestBlockSize,
    TStringBuf dataBuffer) const
{
    VerifyBlockSize(requestBlockSize);

    ui32 bytesCount = 0;
    if (dataBuffer) {
        bytesCount = VerifyRequestSize(dataBuffer.size());
    } else {
        bytesCount = VerifyRequestSize(request->GetBlocks());
    }

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
    if (request->ChecksumsSize() > 0) {
        localRequest->MutableChecksums()->CopyFrom(request->GetChecksums());
    }

    TStorageBuffer buffer;
    TSgList sgList;

    if (dataBuffer) {
        sgList = TSgList{{dataBuffer.data(), dataBuffer.size()}};
    } else {
        sgList = GetSgList(*request);
        buffer = Storage->AllocateBuffer(bytesCount);

        if (buffer) {
            TSgList bufferSgList = {{ buffer.get(), bytesCount }};
            size_t bytesCopied = SgListCopy(sgList, bufferSgList);
            Y_ABORT_UNLESS(bytesCopied == bytesCount);
            sgList = std::move(bufferSgList);
        }

    }

    if (Normalize && sgList.size() != localBlocksCount) {
        // not normalized yet
        auto sgListOrError =
            SgListNormalize(std::move(sgList), StorageBlockSize);

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

    auto promise = NewPromise<NProto::TWriteBlocksResponse>();
    const auto id = InflightWrites->RegisterRequest(now, promise);

    future.Subscribe(
        [inflightWrites = InflightWrites,
         id,
         promise,
         request = std::move(request),
         buffer = std::move(buffer),
         guardedSgList = std::move(guardedSgList)](const auto& f) mutable
        {
            inflightWrites->UnregisterRequest(id);

            const auto& localResponse = f.GetValue();

            guardedSgList.Close();
            // It is required to transfer buffer and request here to extend the
            // lifetime of the request and its memory while the Storage is doing
            // its job.
            Y_UNUSED(request);
            Y_UNUSED(buffer);

            NProto::TWriteBlocksResponse response;
            response.MergeFrom(localResponse);

            promise.TrySetValue(std::move(response));
        });

    return promise;
}

TFuture<NProto::TZeroBlocksResponse> TStorageAdapter::TImpl::ZeroBlocks(
    TInstant now,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    ui32 requestBlockSize) const
{
    if (requestBlockSize == StorageBlockSize) {
        auto promise = NewPromise<NProto::TZeroBlocksResponse>();
        const auto id = InflightZeros->RegisterRequest(now, promise);

        auto future =
            Storage->ZeroBlocks(std::move(callContext), std::move(request));

        future.Subscribe(
            [inflightZeros = InflightZeros, id, promise](const auto& f) mutable
            {
                inflightZeros->UnregisterRequest(id);
                promise.TrySetValue(std::move(f.GetValue()));
            });

        return promise;
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

    auto future = Storage->ZeroBlocks(
        std::move(callContext),
        std::move(localRequest));

    auto promise = NewPromise<NProto::TZeroBlocksResponse>();
    const auto id = InflightZeros->RegisterRequest(now, promise);

    future.Subscribe(
        [inflightZeros = InflightZeros, id, promise](const auto& f) mutable
        {
            inflightZeros->UnregisterRequest(id);
            promise.TrySetValue(std::move(f.GetValue()));
        });

    return promise;
}

TFuture<NProto::TError> TStorageAdapter::TImpl::EraseDevice(
    NProto::EDeviceEraseMethod method) const
{
    return Storage->EraseDevice(method);
}

template <typename TResponse>
void TStorageAdapter::TImpl::CheckIOTimeouts(
    TInflightTracker<TResponse>& inflights,
    TInstant now)
{
    for (auto& inflight: inflights.ExtractTimedOut(now)) {
        TResponse response;
        *response.MutableError() = MakeError(E_IO, "io timeout");
        inflight.Promise.TrySetValue(std::move(response));
        Storage->ReportIOError();
    }
}

void TStorageAdapter::TImpl::CheckIOTimeouts(TInstant now)
{
    CheckIOTimeouts(*InflightReads, now);
    CheckIOTimeouts(*InflightWrites, now);
    CheckIOTimeouts(*InflightZeros, now);
}

void TStorageAdapter::TImpl::ReportIOError()
{
    Storage->ReportIOError();
}

size_t TStorageAdapter::TImpl::Shutdown(ITimerPtr timer, TDuration duration)
{
    const auto startAt = timer->Now();
    while (true) {
        if (InflightReads->IsEmpty() && InflightWrites->IsEmpty() &&
            InflightZeros->IsEmpty())
        {
            return 0;
        }
        if (startAt + duration < timer->Now()) {
            break;
        }
        Sleep(duration / 100);
    }

    return InflightReads->Size() + InflightWrites->Size() +
           InflightZeros->Size();
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

ui32 TStorageAdapter::TImpl::VerifyRequestSize(ui32 bytesCount) const
{
    if (bytesCount == 0 || bytesCount % StorageBlockSize != 0) {
        ythrow TServiceError(E_ARGUMENT)
            << "buffer size (" << bytesCount << ") is not a multiple of storage block size"
            << " (storage block size = " << StorageBlockSize << ")";
    }

    if (MaxRequestSize > 0 && bytesCount > MaxRequestSize) {
        ythrow TServiceError(E_ARGUMENT)
            << "invalid request size: " << bytesCount
            << " (max request size = " << MaxRequestSize << ")";
    }

    return bytesCount;
}

ui32 TStorageAdapter::TImpl::VerifyRequestSize(const NProto::TIOVector& iov) const
{
    ui64 bytesCount = 0;
    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer.empty() || buffer.size() % StorageBlockSize != 0) {
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
        TDuration maxRequestDuration,
        TDuration shutdownTimeout)
    : Impl(std::make_unique<TImpl>(
        std::move(storage),
        storageBlockSize,
        normalize,
        maxRequestDuration))
    , ShutdownTimeout(shutdownTimeout)
{}

TStorageAdapter::~TStorageAdapter()
{
    Shutdown(CreateWallClockTimer());
}

TFuture<NProto::TReadBlocksResponse> TStorageAdapter::ReadBlocks(
    TInstant now,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    ui32 requestBlockSize,
    TStringBuf dataBuffer) const
{
    return Impl->ReadBlocks(
        now,
        std::move(callContext),
        std::move(request),
        requestBlockSize,
        dataBuffer);
}

TFuture<NProto::TWriteBlocksResponse> TStorageAdapter::WriteBlocks(
    TInstant now,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    ui32 requestBlockSize,
    TStringBuf dataBuffer) const
{
    return Impl->WriteBlocks(
        now,
        std::move(callContext),
        std::move(request),
        requestBlockSize,
        dataBuffer);
}

TFuture<NProto::TZeroBlocksResponse> TStorageAdapter::ZeroBlocks(
    TInstant now,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    ui32 requestBlockSize) const
{
    return Impl->ZeroBlocks(
        now,
        std::move(callContext),
        std::move(request),
        requestBlockSize);
}

TFuture<NProto::TError> TStorageAdapter::EraseDevice(
    NProto::EDeviceEraseMethod method) const
{
    return Impl->EraseDevice(method);
}

void TStorageAdapter::CheckIOTimeouts(TInstant now)
{
    Impl->CheckIOTimeouts(now);
}

void TStorageAdapter::ReportIOError()
{
    Impl->ReportIOError();
}

size_t TStorageAdapter::Shutdown(ITimerPtr timer)
{
    return Impl->Shutdown(std::move(timer), ShutdownTimeout);
}

}   // namespace NCloud::NBlockStore
