#include "target.h"

#include "protocol.h"
#include "request_tracker.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_method.h>

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
    auto event = std::make_unique<TResponse>(error);
    if (HasError(request.Reply(std::move(event)))) {
        request.CloseChannel();
    }
}

void ReplyError(
    IIncomingRequest& request,
    ui32 type,
    const NProto::TError& error)
{
    switch (type) {
        case TEvService::EvReadBlocksRequest:
            return ReplyError<TEvService::TEvReadBlocksResponse>(
                request,
                error);
        case TEvService::EvWriteBlocksRequest:
            return ReplyError<TEvService::TEvWriteBlocksResponse>(
                request,
                error);
        case TEvService::EvZeroBlocksRequest:
            return ReplyError<TEvService::TEvZeroBlocksResponse>(
                request,
                error);
        default:
            request.CloseChannel();
    }
}

template <typename TRequest, typename TResponse>
struct TOperation
{
    // Released last, after request/response buffers and SG access are gone.
    TReservationPtr Reservation;
    std::shared_ptr<TRequest> Request = std::make_shared<TRequest>();
    std::unique_ptr<TResponse> Response = std::make_unique<TResponse>();
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

// Range and payload size have already been validated. Compact a fragmented
// payload once so each storage block has a contiguous data reference.
TSgList GetBlockSgList(TRope& payload, ui32 blockSize)
{
    const auto data = payload.GetContiguousSpan();
    TSgList result(data.GetSize() / blockSize);
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = TBlockDataRef(data.GetData() + i * blockSize, blockSize);
    }
    return result;
}

class TDataTarget final
    : public IDataTarget
    , public std::enable_shared_from_this<TDataTarget>
{
    const IBlockStorePtr Service;
    const ITaskQueuePtr Executor;
    const TDataConfig Config;
    const std::shared_ptr<TRequestTracker> Tracker;

public:
    TDataTarget(
        IBlockStorePtr service,
        ITaskQueuePtr executor,
        TDataConfig config)
        : Service(std::move(service))
        , Executor(std::move(executor))
        , Config(config)
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
        if (!request) {
            return;
        }

        const auto& event = request->GetEvent();
        const ui64 bytes = event.GetSize();
        if (bytes > Config.MaxRequestBytes) {
            ReplyError(
                *request,
                event.Type,
                MakeError(E_ARGUMENT, "IC V2 request exceeds size limit"));
            return;
        }
        auto [reservation, admissionError] = Tracker->Acquire(2 * bytes);
        if (HasError(admissionError)) {
            ReplyError(*request, event.Type, admissionError);
            return;
        }
        Executor->ExecuteSimple(
            [self = shared_from_this(),
             request = std::move(request),
             reservation = std::move(reservation)]() mutable
            {
                NProto::TError error;
                try {
                    error = self->ExecuteRequest(request, reservation);
                } catch (...) {
                    if (request) {
                        request->CloseChannel();
                    }
                    return;
                }
                if (HasError(error)) {
                    ReplyError(*request, request->GetEvent().Type, error);
                }
            });
    }

private:
    NProto::TError ExecuteRequest(
        IIncomingRequestPtr& incoming,
        TReservationPtr reservation)
    {
        auto& request = incoming->GetEvent();
        if (auto error = reservation->AddBytes(0); HasError(error)) {
            return error;
        }
        switch (request.Type) {
            case TEvService::EvReadBlocksRequest: {
                auto* event = request.Get<TEvService::TEvReadBlocksRequest>();
                if (event->GetPayloadCount()) {
                    return MakeError(
                        E_ARGUMENT,
                        "unexpected IC V2 read payload");
                }
                auto& record = event->Record;
                auto [bytes, error] = ValidateDataRangeAndGetBytes(
                    record.GetDiskId(),
                    record.GetStartIndex(),
                    record.GetBlocksCount(),
                    record.GetBlockSize(),
                    Config);
                if (HasError(error)) {
                    return error;
                }
                if (auto error = reservation->AddBytes(GetDataReservationBytes(
                        bytes,
                        record.GetBlocksCount()));
                    HasError(error))
                {
                    return error;
                }
                auto op = CreateOperation<
                    NProto::TReadBlocksLocalRequest,
                    TEvService::TEvReadBlocksResponse>(
                    std::move(incoming),
                    std::move(reservation));
                op->Request->CopyFrom(record);
                op->Payload = TRope(TString(bytes, '\0'));
                op->Sglist = TGuardedSgList(
                    GetBlockSgList(op->Payload, record.GetBlockSize()));
                op->Request->Sglist = op->Sglist;
                ExecuteStorageRequest<TBlockStoreReadBlocksLocalMethod>(
                    std::move(op));
                return {};
            }
            case TEvService::EvWriteBlocksRequest: {
                auto* event = request.Get<TEvService::TEvWriteBlocksRequest>();
                auto& record = event->Record;
                if (!record.GetBlockSize() ||
                    record.GetBlocks().BuffersSize() ||
                    event->GetPayloadCount() != 1)
                {
                    return MakeError(E_ARGUMENT, "invalid IC V2 write payload");
                }
                const auto& payload = event->GetPayload(0);
                const ui64 size = payload.GetSize();
                const ui64 count = size / record.GetBlockSize();
                if (size % record.GetBlockSize() || count > Max<ui32>()) {
                    return MakeError(E_ARGUMENT, "unaligned IC V2 write size");
                }
                auto [bytes, error] = ValidateDataRangeAndGetBytes(
                    record.GetDiskId(),
                    record.GetStartIndex(),
                    count,
                    record.GetBlockSize(),
                    Config);
                if (HasError(error)) {
                    return error;
                }
                if (auto error = reservation->AddBytes(
                        GetDataReservationBytes(0, count));
                    HasError(error))
                {
                    return error;
                }
                auto op = CreateOperation<
                    NProto::TWriteBlocksLocalRequest,
                    TEvService::TEvWriteBlocksResponse>(
                    std::move(incoming),
                    std::move(reservation));
                static_cast<NProto::TWriteBlocksRequest&>(*op->Request) =
                    std::move(record);
                op->Request->BlocksCount = count;
                op->Payload = payload;
                op->Sglist = TGuardedSgList(
                    GetBlockSgList(op->Payload, op->Request->GetBlockSize()));
                op->Request->Sglist = op->Sglist;
                ExecuteStorageRequest<TBlockStoreWriteBlocksLocalMethod>(
                    std::move(op));
                return {};
            }
            case TEvService::EvZeroBlocksRequest: {
                auto* event = request.Get<TEvService::TEvZeroBlocksRequest>();
                if (event->GetPayloadCount()) {
                    return MakeError(
                        E_ARGUMENT,
                        "unexpected IC V2 zero payload");
                }
                auto& record = event->Record;
                if (auto error = ValidateBlockRange(
                        record.GetDiskId(),
                        record.GetStartIndex(),
                        record.GetBlocksCount());
                    HasError(error))
                {
                    return error;
                }
                auto op = CreateOperation<
                    NProto::TZeroBlocksRequest,
                    TEvService::TEvZeroBlocksResponse>(
                    std::move(incoming),
                    std::move(reservation));
                *op->Request = std::move(record);
                ExecuteStorageRequest<TBlockStoreZeroBlocksMethod>(
                    std::move(op));
                return {};
            }
            default:
                return MakeError(E_BADMSG, "unsupported IC V2 data event");
        }
    }

    template <typename TRequest, typename TResponse>
    std::shared_ptr<TOperation<TRequest, TResponse>> CreateOperation(
        IIncomingRequestPtr incoming,
        TReservationPtr reservation)
    {
        auto op = std::make_shared<TOperation<TRequest, TResponse>>();
        op->Incoming = std::move(incoming);
        op->Reservation = std::move(reservation);
        return op;
    }

    template <typename TMethod, typename TOperation>
    void ExecuteStorageRequest(std::shared_ptr<TOperation> op)
    {
        op->Context = MakeIntrusive<TCallContext>(
            op->Request->GetHeaders().GetRequestId());
        NThreading::TFuture<typename TMethod::TResponse> future;
        try {
            future = TMethod::Execute(Service.get(), op->Context, op->Request);
        } catch (...) {
            future = NThreading::MakeFuture<typename TMethod::TResponse>(
                TErrorResponse(E_FAIL, CurrentExceptionMessage()));
        }
        future.Subscribe(
            [self = shared_from_this(),
             op = std::move(op)](const auto& future) mutable
            {
                self->Executor->ExecuteSimple(
                    [op = std::move(op), future]() mutable
                    {
                        auto response =
                            SafeExecute<typename TMethod::TResponse>(
                                [&] { return future.GetValue(); });
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
                        if constexpr (
                            std::is_same_v<
                                TMethod,
                                TBlockStoreReadBlocksLocalMethod>)
                        {
                            response.ClearBlocks();
                            if (!HasError(response.GetError())) {
                                op->Response->AddPayload(
                                    std::move(op->Payload));
                            }
                        }
                        op->Response->Record.Swap(&response);
                        if (HasError(
                                op->Incoming->Reply(std::move(op->Response)))) {
                            op->Incoming->CloseChannel();
                        }
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
