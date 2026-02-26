#include "overlapping_requests_guard.h"

#include "service_method.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/block_range_map.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/error.h>

using namespace NThreading;

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum class ERequestOverlapping
{
    // The range of the new request is completely covered by the range of the
    // inflight request.
    // We can skip execution of such request and respond to it at the completion
    // of the inflight request.
    Covered,

    // The range of the new request partially overlaps with the range of the
    // inflight request.
    // Such a request should be postponed until the completion of the inflight
    // request.
    Overlapped
};

class TInflight;

class TOverlappingRequestsGuard final
    : public TBlockStoreImpl<TOverlappingRequestsGuard, IBlockStore>
    , public std::enable_shared_from_this<TOverlappingRequestsGuard>
{
private:
    const IBlockStorePtr Service;

    // Set before receiving requests, so used without lock.
    ui32 BlockSize = 0;
    TString DiskId;

    TAdaptiveLock Lock;
    ui64 RequestIdGenerator = 0;
    TBlockRangeMap<ui64, std::unique_ptr<TInflight>> InflightRequests;

public:
    explicit TOverlappingRequestsGuard(IBlockStorePtr service);
    ~TOverlappingRequestsGuard() override;

    // implements IBlockStore

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override;

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override;

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        return TBlockStoreAdapter::Execute(
            Service.get(),
            std::move(callContext),
            std::move(request));
    }

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> DoExecuteRequest(
        TBlockRange64 range,
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request);

    void OnRequestFinished(ui64 requestId, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
struct TCoveredRequest
{
    TPromise<TResponse> Promise = NewPromise<TResponse>();
    std::shared_ptr<TRequest> Request;
};

using TCoveredWriteBlocksRequest =
    TCoveredRequest<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>;
using TCoveredWriteBlocksLocalRequest = TCoveredRequest<
    NProto::TWriteBlocksLocalRequest,
    NProto::TWriteBlocksLocalResponse>;
using TCoveredZeroBlocksRequest =
    TCoveredRequest<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>;

template <typename TRequest, typename TResponse>
struct TDelayedRequest
{
    TCallContextPtr CallContext;
    TPromise<TResponse> Promise = NewPromise<TResponse>();
    std::shared_ptr<TRequest> Request;
};

using TDelayedWriteBlocksRequest =
    TDelayedRequest<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>;
using TDelayedWriteBlocksLocalRequest = TDelayedRequest<
    NProto::TWriteBlocksLocalRequest,
    NProto::TWriteBlocksLocalResponse>;
using TDelayedZeroBlocksRequest =
    TDelayedRequest<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>;

class TInflight
{
private:
    TVector<TCoveredWriteBlocksRequest> CoveredWriteBlocks;
    TVector<TCoveredWriteBlocksLocalRequest> CoveredWriteBlocksLocal;
    TVector<TCoveredZeroBlocksRequest> CoveredZeroBlocks;

    TVector<TDelayedWriteBlocksRequest> DelayedWriteBlocks;
    TVector<TDelayedWriteBlocksLocalRequest> DelayedWriteBlocksLocal;
    TVector<TDelayedZeroBlocksRequest> DelayedZeroBlocks;

public:
    TFuture<NProto::TWriteBlocksResponse> AddRequest(
        ERequestOverlapping overlapping,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request);

    TFuture<NProto::TWriteBlocksLocalResponse> AddRequest(
        ERequestOverlapping overlapping,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request);

    TFuture<NProto::TZeroBlocksResponse> AddRequest(
        ERequestOverlapping overlapping,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request);

    void OnRequestExecuted(IBlockStore* service, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TWriteBlocksResponse> TInflight::AddRequest(
    ERequestOverlapping overlapping,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request)
{
    switch (overlapping) {
        case ERequestOverlapping::Covered: {
            CoveredWriteBlocks.push_back({.Request = std::move(request)});
            return CoveredWriteBlocks.back().Promise;
        }
        case ERequestOverlapping::Overlapped: {
            DelayedWriteBlocks.push_back(
                {.CallContext = std::move(callContext),
                 .Request = std::move(request)});
            return DelayedWriteBlocks.back().Promise;
        }
    }
}

TFuture<NProto::TWriteBlocksLocalResponse> TInflight::AddRequest(
    ERequestOverlapping overlapping,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    switch (overlapping) {
        case ERequestOverlapping::Covered: {
            CoveredWriteBlocksLocal.push_back({.Request = std::move(request)});
            return CoveredWriteBlocksLocal.back().Promise;
        }
        case ERequestOverlapping::Overlapped: {
            DelayedWriteBlocksLocal.push_back(
                {.CallContext = std::move(callContext),
                 .Request = std::move(request)});
            return DelayedWriteBlocksLocal.back().Promise;
        }
    }
}

TFuture<NProto::TZeroBlocksResponse> TInflight::AddRequest(
    ERequestOverlapping overlapping,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    switch (overlapping) {
        case ERequestOverlapping::Covered: {
            CoveredZeroBlocks.push_back({.Request = std::move(request)});
            return CoveredZeroBlocks.back().Promise;
        }
        case ERequestOverlapping::Overlapped: {
            DelayedZeroBlocks.push_back(
                {.CallContext = std::move(callContext),
                 .Request = std::move(request)});
            return DelayedZeroBlocks.back().Promise;
        }
    }
}

void TInflight::OnRequestExecuted(
    IBlockStore* service,
    const NProto::TError& error)
{
    for (auto& [promise, _]: CoveredWriteBlocks) {
        NProto::TWriteBlocksResponse response;
        response.MutableError()->CopyFrom(error);
        promise.SetValue(std::move(response));
    }

    for (auto& [promise, _]: CoveredWriteBlocksLocal) {
        NProto::TWriteBlocksLocalResponse response;
        response.MutableError()->CopyFrom(error);
        promise.SetValue(std::move(response));
    }

    for (auto& [promise, _]: CoveredZeroBlocks) {
        NProto::TZeroBlocksResponse response;
        response.MutableError()->CopyFrom(error);
        promise.SetValue(std::move(response));
    }

    for (auto& delayed: DelayedWriteBlocks) {
        auto result = service->WriteBlocks(
            std::move(delayed.CallContext),
            std::move(delayed.Request));

        result.Subscribe(
            [promise = std::move(delayed.Promise)](
                const TFuture<NProto::TWriteBlocksResponse>& f) mutable
            {
                promise.SetValue(UnsafeExtractValue(f));   //
            });
    }

    for (auto& delayed: DelayedWriteBlocksLocal) {
        auto result = service->WriteBlocksLocal(
            std::move(delayed.CallContext),
            std::move(delayed.Request));

        result.Subscribe(
            [promise = std::move(delayed.Promise)](
                const TFuture<NProto::TWriteBlocksLocalResponse>& f) mutable
            {
                promise.SetValue(UnsafeExtractValue(f));   //
            });
    }

    for (auto& delayed: DelayedZeroBlocks) {
        auto result = service->ZeroBlocks(
            std::move(delayed.CallContext),
            std::move(delayed.Request));
        result.Subscribe(
            [promise = std::move(delayed.Promise)](
                const TFuture<NProto::TZeroBlocksResponse>& f) mutable
            {
                promise.SetValue(UnsafeExtractValue(f));   //
            });
    }
}

////////////////////////////////////////////////////////////////////////////////

TOverlappingRequestsGuard::TOverlappingRequestsGuard(IBlockStorePtr service)
    : Service(std::move(service))
{}

TOverlappingRequestsGuard::~TOverlappingRequestsGuard() = default;

TFuture<NProto::TWriteBlocksResponse> TOverlappingRequestsGuard::WriteBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request)
{
    if (!BlockSize) {
        NProto::TWriteBlocksResponse error;
        *error.MutableError() = MakeError(
            E_REJECTED,
            "Need to mount volume before execute WriteBlocks");
        return MakeFuture<NProto::TWriteBlocksResponse>(std::move(error));
    }

    const auto range = TBlockRange64::WithLength(
        request->GetStartIndex(),
        CalculateWriteRequestBlockCount(*request, BlockSize));

    return DoExecuteRequest<NProto::TWriteBlocksLocalResponse>(
        range,
        std::move(callContext),
        std::move(request));
}

TFuture<NProto::TWriteBlocksLocalResponse>
TOverlappingRequestsGuard::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    const auto range = TBlockRange64::WithLength(
        request->GetStartIndex(),
        request->BlocksCount);

    return DoExecuteRequest<NProto::TWriteBlocksLocalResponse>(
        range,
        std::move(callContext),
        std::move(request));
}

TFuture<NProto::TZeroBlocksResponse> TOverlappingRequestsGuard::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    const auto range = TBlockRange64::WithLength(
        request->GetStartIndex(),
        request->GetBlocksCount());

    return DoExecuteRequest<NProto::TZeroBlocksResponse>(
        range,
        std::move(callContext),
        std::move(request));
}

TFuture<NProto::TMountVolumeResponse> TOverlappingRequestsGuard::MountVolume(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TMountVolumeRequest> request)
{
    auto result =
        Service->MountVolume(std::move(callContext), std::move(request));
    result.Subscribe(
        [weakSelf = weak_from_this()]   //
        (const TFuture<NProto::TMountVolumeResponse>& f)
        {
            const NProto::TMountVolumeResponse& response = f.GetValue();
            if (HasError(response)) {
                return;
            }
            if (auto self = weakSelf.lock()) {
                self->BlockSize = response.GetVolume().GetBlockSize();
                self->DiskId = response.GetVolume().GetDiskId();
            }
        });
    return result;
}

template <typename TResponse, typename TRequest>
TFuture<TResponse> TOverlappingRequestsGuard::DoExecuteRequest(
    TBlockRange64 range,
    TCallContextPtr callContext,
    std::shared_ptr<TRequest> request)
{
    auto guard = Guard(Lock);

    Y_ABORT_UNLESS(DiskId == request->GetDiskId());

    auto overlapping = InflightRequests.FindFirstOverlapping(range);

    if (!overlapping) {
        const ui64 requestId = ++RequestIdGenerator;
        InflightRequests.AddRange(
            requestId,
            range,
            nullptr   // will create TInflight when we find the first
                      // intersection with the request.
        );
        guard.Release();

        auto result = TBlockStoreAdapter::Execute(
            Service.get(),
            std::move(callContext),
            std::move(request));
        result.Subscribe(
            [weakSelf = weak_from_this(),
             requestId](const TFuture<TResponse>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnRequestFinished(requestId, f.GetValue().GetError());
                }
            });
        return result;
    }

    std::unique_ptr<TInflight>& inflightPtr = overlapping->Value;
    if (!inflightPtr) {
        inflightPtr = std::make_unique<TInflight>();
    }

    return inflightPtr->AddRequest(
        overlapping->Range.Contains(range) ? ERequestOverlapping::Covered
                                           : ERequestOverlapping::Overlapped,
        std::move(callContext),
        std::move(request));
}

void TOverlappingRequestsGuard::OnRequestFinished(
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

IBlockStorePtr CreateOverlappingRequestsGuard(IBlockStorePtr service)
{
    return std::make_shared<TOverlappingRequestsGuard>(std::move(service));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
