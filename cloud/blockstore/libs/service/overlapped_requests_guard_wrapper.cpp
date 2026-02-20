#include "overlapped_requests_guard_wrapper.h"

#include "storage.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/block_range_map.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>

using namespace NThreading;

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TInflight;

class TOverlappedRequestsGuardStorageWrapper final
    : public IStorage
    , public std::enable_shared_from_this<
          TOverlappedRequestsGuardStorageWrapper>
{
private:
    const IStoragePtr Storage;

    TAdaptiveLock Lock;
    ui64 RequestIdGenerator = 0;
    TBlockRangeMap<ui64, std::unique_ptr<TInflight>> InflightRequests;

public:
    explicit TOverlappedRequestsGuardStorageWrapper(IStoragePtr storage);
    ~TOverlappedRequestsGuardStorageWrapper() override;

    // implements IStorage
    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override;

    // nullptr means client could use own buffer
    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

    void ReportIOError() override;

private:
    void OnRequestFinished(ui64 requestId, const NProto::TError& error);
};

template <typename TRequest, typename TResponse>
struct TCoveredRequest
{
    TPromise<TResponse> Promise = NewPromise<TResponse>();
    std::shared_ptr<TRequest> Request;
};

using TCoveredWriteRequest = TCoveredRequest<
    NProto::TWriteBlocksLocalRequest,
    NProto::TWriteBlocksLocalResponse>;
using TCoveredZeroRequest =
    TCoveredRequest<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>;

template <typename TRequest, typename TResponse>
struct TDelayedRequest
{
    TCallContextPtr CallContext;
    TPromise<TResponse> Promise = NewPromise<TResponse>();
    std::shared_ptr<TRequest> Request;
};

using TDelayedWriteRequest = TDelayedRequest<
    NProto::TWriteBlocksLocalRequest,
    NProto::TWriteBlocksLocalResponse>;
using TDelayedZeroRequest =
    TDelayedRequest<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>;

struct TInflight
{
    TVector<TCoveredWriteRequest> CoveredWrites;
    TVector<TCoveredZeroRequest> CoveredZeroes;
    TVector<TDelayedWriteRequest> DelayedWrites;
    TVector<TDelayedZeroRequest> DelayedZeroes;

    void OnRequestExecuted(IStorage* storage, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

void TInflight::OnRequestExecuted(
    IStorage* storage,
    const NProto::TError& error)
{
    for (auto& [promise, _]: CoveredWrites) {
        NProto::TWriteBlocksLocalResponse response;
        response.MutableError()->CopyFrom(error);
        promise.SetValue(std::move(response));
    }

    for (auto& [promise, _]: CoveredZeroes) {
        NProto::TZeroBlocksResponse response;
        response.MutableError()->CopyFrom(error);
        promise.SetValue(std::move(response));
    }

    for (auto& delayed: DelayedWrites) {
        auto result = storage->WriteBlocksLocal(
            std::move(delayed.CallContext),
            std::move(delayed.Request));

        result.Apply(
            [promise = std::move(delayed.Promise)](
                const TFuture<NProto::TWriteBlocksLocalResponse>& f) mutable
            {
                promise.SetValue(f.GetValue());   //
            });
    }

    for (auto& delayed: DelayedZeroes) {
        auto result = storage->ZeroBlocks(
            std::move(delayed.CallContext),
            std::move(delayed.Request));

        result.Apply(
            [promise = std::move(delayed.Promise)](
                const TFuture<NProto::TZeroBlocksResponse>& f) mutable
            {
                promise.SetValue(f.GetValue());   //
            });
    }
}

////////////////////////////////////////////////////////////////////////////////

TOverlappedRequestsGuardStorageWrapper::TOverlappedRequestsGuardStorageWrapper(
    IStoragePtr storage)
    : Storage(std::move(storage))
{}

TOverlappedRequestsGuardStorageWrapper::
    ~TOverlappedRequestsGuardStorageWrapper() = default;

TFuture<NProto::TZeroBlocksResponse>
TOverlappedRequestsGuardStorageWrapper::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    const auto range = TBlockRange64::WithLength(
        request->GetStartIndex(),
        request->GetBlocksCount());

    auto guard = Guard(Lock);

    const auto* overlaps = InflightRequests.FindFirstOverlapping(range);

    if (!overlaps) {
        const ui64 requestId = ++RequestIdGenerator;
        InflightRequests.AddRange(
            requestId,
            range,
            nullptr   // will create TInflight when we find the first
                      // intersection with the request.
        );
        guard.Release();

        auto result =
            Storage->ZeroBlocks(std::move(callContext), std::move(request));
        result.Subscribe(
            [weakSelf = weak_from_this(),
             requestId](const TFuture<NProto::TZeroBlocksResponse>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnRequestFinished(requestId, f.GetValue().GetError());
                }
            });
        return result;
    }

    std::unique_ptr<TInflight>& inflightPtr = overlaps->AccessValue();
    if (!inflightPtr) {
        inflightPtr = std::make_unique<TInflight>();
    }

    if (overlaps->Range.Contains(range)) {
        inflightPtr->CoveredZeroes.push_back({.Request = std::move(request)});
        return inflightPtr->CoveredZeroes.back().Promise;
    }

    inflightPtr->DelayedZeroes.push_back(
        {.CallContext = std::move(callContext), .Request = std::move(request)});
    return inflightPtr->DelayedZeroes.back().Promise;
}

TFuture<NProto::TReadBlocksLocalResponse>
TOverlappedRequestsGuardStorageWrapper::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    return Storage->ReadBlocksLocal(std::move(callContext), std::move(request));
}

TFuture<NProto::TWriteBlocksLocalResponse>
TOverlappedRequestsGuardStorageWrapper::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    const auto range = TBlockRange64::WithLength(
        request->GetStartIndex(),
        request->BlocksCount);

    auto guard = Guard(Lock);

    const auto* overlaps = InflightRequests.FindFirstOverlapping(range);

    if (!overlaps) {
        const ui64 requestId = ++RequestIdGenerator;
        InflightRequests.AddRange(
            requestId,
            range,
            nullptr   // will create TInflight when we find the first
                      // intersection with the request.
        );
        guard.Release();   // Access to InflightRequests completed.

        auto result = Storage->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
        result.Subscribe(
            [weakSelf = weak_from_this(),
             requestId](const TFuture<NProto::TWriteBlocksLocalResponse>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnRequestFinished(requestId, f.GetValue().GetError());
                }
            });
        return result;
    }

    std::unique_ptr<TInflight>& inflightPtr = overlaps->AccessValue();
    if (!inflightPtr) {
        inflightPtr = std::make_unique<TInflight>();
    }

    if (overlaps->Range.Contains(range)) {
        // The new request is fully covered by the executing one.
        inflightPtr->CoveredWrites.push_back({.Request = std::move(request)});
        return inflightPtr->CoveredWrites.back().Promise;
    }

    inflightPtr->DelayedWrites.push_back(
        {.CallContext = std::move(callContext), .Request = std::move(request)});
    return inflightPtr->DelayedWrites.back().Promise;
}

TFuture<NProto::TError> TOverlappedRequestsGuardStorageWrapper::EraseDevice(
    NProto::EDeviceEraseMethod method)
{
    return Storage->EraseDevice(method);
}

TStorageBuffer TOverlappedRequestsGuardStorageWrapper::AllocateBuffer(
    size_t bytesCount)
{
    return Storage->AllocateBuffer(bytesCount);
}

void TOverlappedRequestsGuardStorageWrapper::ReportIOError()
{
    Storage->ReportIOError();
}

void TOverlappedRequestsGuardStorageWrapper::OnRequestFinished(
    ui64 requestId,
    const NProto::TError& error)
{
    Lock.Acquire();
    auto inflight = InflightRequests.ExtractRange(requestId);
    Lock.Release();

    if (inflight && inflight->Value) {
        inflight->Value->OnRequestExecuted(this, error);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateOverlappedRequestsGuardStorageWrapper(IStoragePtr storage)
{
    return std::make_shared<TOverlappedRequestsGuardStorageWrapper>(storage);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
