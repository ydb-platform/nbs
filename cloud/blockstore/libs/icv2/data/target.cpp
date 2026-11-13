#include "target.h"

#include "request_tracker.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

using NCloud::NIcV2::IIncomingRequest;
using NCloud::NIcV2::IIncomingRequestPtr;

////////////////////////////////////////////////////////////////////////////////

using NStorage::TEvService;
using TReservationPtr = std::shared_ptr<TRequestTracker::TReservation>;

template <typename TResponse>
void ReplyError(IIncomingRequest& request, const NProto::TError& error)
{
    request.Reply(std::make_unique<TResponse>(error));
}

// Payload size has already been checked. Compact a fragmented
// payload once so each storage block has a contiguous data reference.
TSgList GetBlockSgList(TRope& payload, ui32 blockSize)
{
    const auto data = payload.GetContiguousSpan();
    TSgList result(data.GetSize() / blockSize);
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = TBlockDataRef(data.GetData() + (i * blockSize), blockSize);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
struct TOperation
{
    using TResponseEvent = TResponse;

    // Released last, after request/response buffers and SG access are gone.
    TReservationPtr Reservation;
    IBlockStorePtr Service;
    std::shared_ptr<TRequest> Request = std::make_shared<TRequest>();
    TRope Payload;
    TGuardedSgList Sglist;
    IIncomingRequestPtr Incoming;
    TCallContextPtr Context;

    ~TOperation()
    {
        // Storage SG lists point into Payload. Close access before releasing
        // its backing buffer, even if storage retained the local request.
        if (!Sglist.Empty()) {
            Sglist.Close();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TOperation>
void SetStorageSgList(TOperation& op)
{
    op.Sglist =
        TGuardedSgList(GetBlockSgList(op.Payload, op.Request->GetBlockSize()));
    op.Request->Sglist = op.Sglist;
}

template <typename TResponse>
NProto::TError PrepareStorageRequest(
    TOperation<NProto::TReadBlocksLocalRequest, TResponse>& op,
    const TEvService::TEvReadBlocksRequest& event)
{
    if (event.GetPayloadCount()) {
        return MakeError(E_ARGUMENT, "unexpected IC V2 read payload");
    }
    if (!op.Request->GetBlockSize()) {
        return MakeError(E_ARGUMENT, "empty IC V2 block size");
    }
    const auto bytes =
        CalculateBytesCount(*op.Request, op.Request->GetBlockSize());
    op.Payload = TRope(TString(bytes, '\0'));
    SetStorageSgList(op);
    return {};
}

template <typename TResponse>
NProto::TError PrepareStorageRequest(
    TOperation<NProto::TWriteBlocksLocalRequest, TResponse>& op,
    const TEvService::TEvWriteBlocksRequest& event)
{
    const auto blockSize = op.Request->GetBlockSize();
    if (!blockSize || op.Request->GetBlocks().BuffersSize() ||
        event.GetPayloadCount() != 1)
    {
        return MakeError(E_ARGUMENT, "invalid IC V2 write payload");
    }
    op.Payload = event.GetPayload(0);
    const ui64 size = op.Payload.GetSize();
    const ui64 count = size / blockSize;
    if (!size || size % blockSize || count > Max<ui32>()) {
        return MakeError(E_ARGUMENT, "unaligned IC V2 write size");
    }
    op.Request->BlocksCount = count;
    SetStorageSgList(op);
    return {};
}

template <typename TResponse>
NProto::TError PrepareStorageRequest(
    TOperation<NProto::TZeroBlocksRequest, TResponse>& op,
    const TEvService::TEvZeroBlocksRequest& event)
{
    Y_UNUSED(op);
    if (event.GetPayloadCount()) {
        return MakeError(E_ARGUMENT, "unexpected IC V2 zero payload");
    }
    return {};
}

template <typename TEvent, typename TResponse>
void PrepareStorageResponse(TEvent& event, TResponse& response, TRope& payload)
{
    Y_UNUSED(payload);
    event.Record.Swap(&response);
}

void PrepareStorageResponse(
    TEvService::TEvReadBlocksResponse& event,
    NProto::TReadBlocksLocalResponse& response,
    TRope& payload)
{
    response.ClearBlocks();
    if (!HasError(response.GetError())) {
        event.AddPayload(std::move(payload));
    }
    event.Record.Swap(&response);
}

////////////////////////////////////////////////////////////////////////////////

class TDataTarget final
    : public IDataTarget
    , public std::enable_shared_from_this<TDataTarget>
{
    const IBlockStorePtr Service;
    const ITaskQueuePtr Executor;
    const std::shared_ptr<TRequestTracker> Tracker;

public:
    TDataTarget(
        IBlockStorePtr service,
        ITaskQueuePtr executor,
        TDataConfig config)
        : Service(std::move(service))
        , Executor(std::move(executor))
        , Tracker(std::make_shared<TRequestTracker>(config))
    {}

    void RequestStop() override
    {
        Tracker->RequestStop();
    }

    void Stop() override
    {
        Tracker->Stop();
    }

    void HandleRequest(IIncomingRequestPtr request) override
    {
        Y_DEBUG_ABORT_UNLESS(request);

        switch (request->GetEvent().Type) {
            case TEvService::EvReadBlocksRequest:
                HandleDataRequest<
                    TEvService::TReadBlocksMethod,
                    TBlockStoreReadBlocksLocalMethod>(std::move(request));
                break;
            case TEvService::EvWriteBlocksRequest:
                HandleDataRequest<
                    TEvService::TWriteBlocksMethod,
                    TBlockStoreWriteBlocksLocalMethod>(std::move(request));
                break;
            case TEvService::EvZeroBlocksRequest:
                HandleDataRequest<
                    TEvService::TZeroBlocksMethod,
                    TBlockStoreZeroBlocksMethod>(std::move(request));
                break;
            default:
                request->CloseChannel();
        }
    }

private:
    template <typename TEventMethod, typename TMethod>
    void HandleDataRequest(IIncomingRequestPtr request)
    {
        auto [reservation, error] = Tracker->Acquire(0);
        if (HasError(error)) {
            ReplyError<typename TEventMethod::TResponse>(*request, error);
            return;
        }
        Executor->ExecuteSimple(
            [weak = weak_from_this(),
             request = std::move(request),
             reservation = std::move(reservation)]() mutable
            {
                auto self = weak.lock();
                if (!self) {
                    ReplyError<typename TEventMethod::TResponse>(
                        *request,
                        MakeError(E_CANCELLED, "IC V2 data target is gone"));
                    return;
                }
                NProto::TError error;
                try {
                    error = self->Tracker->CheckRunning();
                    if (!HasError(error)) {
                        error = self->ExecuteRequest<TEventMethod, TMethod>(
                            request,
                            std::move(reservation));
                    }
                } catch (...) {
                    if (request) {
                        ReplyError<typename TEventMethod::TResponse>(
                            *request,
                            MakeError(E_FAIL, CurrentExceptionMessage()));
                    }
                    return;
                }
                if (HasError(error)) {
                    ReplyError<typename TEventMethod::TResponse>(
                        *request,
                        error);
                }
            });
    }

    template <typename TEventMethod, typename TMethod>
    NProto::TError ExecuteRequest(
        IIncomingRequestPtr& incoming,
        TReservationPtr reservation)
    {
        auto* event =
            incoming->GetEvent().Get<typename TEventMethod::TRequest>();
        auto op = std::make_shared<TOperation<
            typename TMethod::TRequest,
            typename TEventMethod::TResponse>>();
        op->Reservation = std::move(reservation);
        op->Service = Service;
        op->Request->Swap(&event->Record);

        if (auto error = PrepareStorageRequest(*op, *event); HasError(error)) {
            return error;
        }
        op->Incoming = std::move(incoming);
        ExecuteStorageRequest<TMethod>(std::move(op));
        return {};
    }

    template <typename TMethod, typename TOperation>
    void ExecuteStorageRequest(std::shared_ptr<TOperation> op)
    {
        op->Context = MakeIntrusive<TCallContext>(
            op->Request->GetHeaders().GetRequestId());
        NThreading::TFuture<typename TMethod::TResponse> future;
        try {
            future =
                TMethod::Execute(op->Service.get(), op->Context, op->Request);
        } catch (...) {
            future = NThreading::MakeFuture<typename TMethod::TResponse>(
                TErrorResponse(E_FAIL, CurrentExceptionMessage()));
        }
        future.Subscribe(
            [executor = Executor, op = std::move(op)]   //
            (const auto& future) mutable
            {
                executor->ExecuteSimple(
                    [op = std::move(op), future]() mutable
                    {
                        auto response = ExtractResponse(future);
                        if (!op->Sglist.Empty()) {
                            op->Sglist.Close();
                        }
                        auto& info =
                            *response.MutableHeaders()->MutableThrottler();
                        info.SetDelay(
                            op->Context->Time(EProcessingStage::Postponed)
                                .MicroSeconds());
                        info.SetShapingDelay(
                            op->Context->Time(EProcessingStage::Shaping)
                                .MicroSeconds());
                        auto event = std::make_unique<
                            typename TOperation::TResponseEvent>();
                        PrepareStorageResponse(*event, response, op->Payload);
                        op->Incoming->Reply(std::move(event));
                        op.reset();
                    });
            });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDataTarget::~IDataTarget() = default;

TResultOrError<IDataTargetPtr> CreateDataTarget(
    IBlockStorePtr service,
    ITaskQueuePtr executor,
    TDataConfig config)
{
    if (!service || !executor || !config.IsValid()) {
        return MakeError(E_ARGUMENT, "invalid IC V2 data target configuration");
    }
    return IDataTargetPtr(
        std::make_shared<TDataTarget>(
            std::move(service),
            std::move(executor),
            config));
}

}   // namespace NCloud::NBlockStore::NIcV2
