#include "storage.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/sglist.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

#include <atomic>

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

template <typename TPromise>
class TInflightTracker
{
public:
    struct TInflightRequest
    {
        TInstant Ts;
        TPromise Promise;
    };

private:
    const TDuration MaxRequestDuration;
    THashMap<ui64, TInflightRequest> Inflight;
    TAdaptiveLock Lock;
    std::atomic<ui64> RequestId = 0;

public:
    TInflightTracker(TDuration maxRequestDuration)
        : MaxRequestDuration(maxRequestDuration)
    {}

    ui64 RegisterRequest(TInstant ts, TPromise promise)
    {
        const auto id = RequestId.fetch_add(1);
        if (MaxRequestDuration != TDuration::Zero()) {
            TGuard guard(Lock);

            Inflight[id] = TInflightRequest{ts, std::move(promise)};
        }
        return id;
    }

    void UnregisterRequest(ui64 requestId)
    {
        if (MaxRequestDuration != TDuration::Zero()) {
            TGuard guard(Lock);

            Inflight.erase(requestId);
        }
    }

    TVector<TInflightRequest> ExtractTimedOut(TInstant now)
    {
        TVector<TInflightRequest> result;
        if (MaxRequestDuration == TDuration::Zero()) {
            return result;
        }

        TGuard guard(Lock);
        TVector<ui64> toDelete;
        for (auto& [id, request]: Inflight) {
            const bool isTimedOut = request.Ts + MaxRequestDuration < now;
            if (isTimedOut) {
                Y_VERIFY_DEBUG(
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
private:
    const IStoragePtr Storage;
    const ui32 StorageBlockSize;
    const bool Normalize;
    const ui32 MaxRequestSize;
    const TDuration MaxRequestDuration;

    using TReadPromise = NThreading::TPromise<NProto::TReadBlocksResponse>;
    mutable TInflightTracker<TReadPromise> InflightReads{MaxRequestDuration};

    using TWritePromise = NThreading::TPromise<NProto::TWriteBlocksResponse>;
    mutable TInflightTracker<TWritePromise> InflightWrites{MaxRequestDuration};

    using TZeroPromise = NThreading::TPromise<NProto::TZeroBlocksResponse>;
    mutable TInflightTracker<TZeroPromise> InflightZeros{MaxRequestDuration};

public:
    TImpl(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,
        ui32 maxRequestSize,
        TDuration maxRequestDuration);

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        ui32 requestBlockSize) const;

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        ui32 requestBlockSize) const;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        ui32 requestBlockSize) const;

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) const;

    void CheckIOTimeouts(TInstant now);

    void ReportIOError();

private:
    void VerifyBlockSize(ui32 blockSize) const;

    ui32 VerifyRequestSize(const NProto::TIOVector& iov) const;
    ui32 VerifyRequestSize(ui32 blocksCount, ui32 blockSize) const;

    template <typename TResponse, typename TInflightRequest>
    void CheckIOTimeouts(TVector<TInflightRequest> timeouted);
};

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. Public methods can be called from any thread.
TStorageAdapter::TImpl::TImpl(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,
        ui32 maxRequestSize,
        TDuration maxRequestDuration)
    : Storage(std::move(storage))
    , StorageBlockSize(storageBlockSize)
    , Normalize(normalize)
    , MaxRequestSize(maxRequestSize)
    , MaxRequestDuration(maxRequestDuration)
{}

TFuture<NProto::TReadBlocksResponse> TStorageAdapter::TImpl::ReadBlocks(
    TInstant now,
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

    auto promise = NewPromise<NProto::TReadBlocksResponse>();
    const auto id =  InflightReads.RegisterRequest(now, promise);

    future.Subscribe(
        [=, guardedSgList = std::move(guardedSgList)] (const auto& f) mutable {
        InflightReads.UnregisterRequest(id);

        const auto& localResponse = f.GetValue();
        guardedSgList.Destroy();

        if (!HasError(localResponse) && buffer) {
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
        promise.TrySetValue(std::move(*response));
    });

    return promise;
}

TFuture<NProto::TWriteBlocksResponse> TStorageAdapter::TImpl::WriteBlocks(
    TInstant now,
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

    auto promise = NewPromise<NProto::TWriteBlocksResponse>();
    const auto id = InflightWrites.RegisterRequest(now, promise);

    future.Subscribe(
        [this,
         id,
         promise,
         request = std::move(request),
         buffer = std::move(buffer),
         guardedSgList = std::move(guardedSgList)] (const auto& f) mutable {
        InflightWrites.UnregisterRequest(id);

        auto localResponse = f.GetValue();

        guardedSgList.Destroy();
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
        const auto id = InflightZeros.RegisterRequest(now, promise);

        auto future =
            Storage->ZeroBlocks(std::move(callContext), std::move(request));

        future.Subscribe([this, id, promise] (const auto& f) mutable {
            InflightZeros.UnregisterRequest(id);
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
    const auto id = InflightZeros.RegisterRequest(now, promise);

    future.Subscribe([this, id, promise] (const auto& f) mutable {
        InflightZeros.UnregisterRequest(id);
        promise.TrySetValue(std::move(f.GetValue()));
    });

    return promise;
}

TFuture<NProto::TError> TStorageAdapter::TImpl::EraseDevice(
    NProto::EDeviceEraseMethod method) const
{
    return Storage->EraseDevice(method);
}

template <typename TResponse, typename TInflightRequest>
void TStorageAdapter::TImpl::CheckIOTimeouts(
    TVector<TInflightRequest> timedOut)
{
    for (auto& inflight: timedOut) {
        TResponse response;
        *response.MutableError() = MakeError(E_IO, "io timeout");
        inflight.Promise.TrySetValue(std::move(response));
        Storage->ReportIOError();
    }
}

void TStorageAdapter::TImpl::CheckIOTimeouts(TInstant now)
{
    CheckIOTimeouts<NProto::TReadBlocksResponse>(
        InflightReads.ExtractTimedOut(now));

    CheckIOTimeouts<NProto::TWriteBlocksResponse>(
        InflightWrites.ExtractTimedOut(now));

    CheckIOTimeouts<NProto::TZeroBlocksResponse>(
        InflightZeros.ExtractTimedOut(now));
}

void TStorageAdapter::TImpl::ReportIOError()
{
    Storage->ReportIOError();
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
        ui32 maxRequestSize,
        TDuration maxRequestDuration)
    : Impl(std::make_unique<TImpl>(
        std::move(storage),
        storageBlockSize,
        normalize,
        maxRequestSize,
        maxRequestDuration))
{}

TStorageAdapter::~TStorageAdapter()
{}

TFuture<NProto::TReadBlocksResponse> TStorageAdapter::ReadBlocks(
    TInstant now,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    ui32 requestBlockSize) const
{
    return Impl->ReadBlocks(
        now,
        std::move(callContext),
        std::move(request),
        requestBlockSize);
}

TFuture<NProto::TWriteBlocksResponse> TStorageAdapter::WriteBlocks(
    TInstant now,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    ui32 requestBlockSize) const
{
    return Impl->WriteBlocks(
        now,
        std::move(callContext),
        std::move(request),
        requestBlockSize);
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

}   // namespace NCloud::NBlockStore
