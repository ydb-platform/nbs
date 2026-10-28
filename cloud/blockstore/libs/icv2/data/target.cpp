#include "data.h"
#include "protocol.h"
#include "request_tracker.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_method.h>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TEvents = NStorage::TEvService;
using TReservation = std::shared_ptr<TRequestTracker::TReservation>;

template <typename TResponse>
void ReplyError(const IRequestChannelPtr& channel, ui64 cookie,
                const NProto::TError& error)
{
    auto event = std::make_unique<TResponse>(error);
    if (!channel->Reply(cookie, std::move(event))) {
        channel->Close();
    }
}

void ReplyError(const IRequestChannelPtr& channel, ui32 type, ui64 cookie,
                const NProto::TError& error)
{
    switch (type) {
        case TEvents::EvReadBlocksRequest:
            return ReplyError<TEvents::TEvReadBlocksResponse>(channel, cookie,
                                                              error);
        case TEvents::EvWriteBlocksRequest:
            return ReplyError<TEvents::TEvWriteBlocksResponse>(channel, cookie,
                                                               error);
        case TEvents::EvZeroBlocksRequest:
            return ReplyError<TEvents::TEvZeroBlocksResponse>(channel, cookie,
                                                              error);
        default:
            channel->Close();
    }
}

template <typename TRequest, typename TResponse>
struct TOperation
{
    // Released last, after request/response buffers and SG access are gone.
    TReservation Reservation;
    std::shared_ptr<TRequest> Request = std::make_shared<TRequest>();
    std::unique_ptr<TResponse> Response = std::make_unique<TResponse>();
    TRope Payload;
    TGuardedSgList Sglist;
    std::weak_ptr<IRequestChannel> Channel;
    ui64 Cookie;
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
    TDataTarget(IBlockStorePtr service, ITaskQueuePtr executor,
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

    void HandleRequest(const IRequestChannelPtr& channel,
                       std::unique_ptr<NActors::IEventHandle> request) override
    {
        if (!request || !request->Cookie) {
            channel->Close();
            return;
        }
        const ui64 bytes = request->GetSize();
        if (bytes > Config.MaxRequestBytes) {
            ReplyError(
                channel, request->Type, request->Cookie,
                MakeError(E_ARGUMENT, "IC V2 request exceeds size limit"));
            return;
        }
        // Account for received serialization and decoding before enqueueing.
        auto [reservation, admissionError] = Tracker->Acquire(2 * bytes);
        if (HasError(admissionError)) {
            ReplyError(channel, request->Type, request->Cookie, admissionError);
            return;
        }
        Executor->ExecuteSimple(
            [self = shared_from_this(),
             weak = std::weak_ptr<IRequestChannel>(channel),
             request = std::move(request),
             reservation = std::move(reservation)]() mutable
            {
                if (auto channel = weak.lock()) {
                    NProto::TError error;
                    try {
                        error = self->Execute(channel, *request, reservation);
                    } catch (...) {
                        channel->Close();
                        return;
                    }
                    if (HasError(error)) {
                        ReplyError(channel, request->Type, request->Cookie,
                                   error);
                    }
                }
            });
    }

private:
    NProto::TError Execute(
        const IRequestChannelPtr& channel,
        NActors::IEventHandle& request, TReservation reservation)
    {
        if (auto error = reservation->AddBytes(0); HasError(error)) {
            return error;
        }
        switch (request.Type) {
            case TEvents::EvReadBlocksRequest: {
                auto* event = request.Get<TEvents::TEvReadBlocksRequest>();
                if (event->GetPayloadCount()) {
                    return MakeError(
                        E_ARGUMENT,
                        "unexpected IC V2 read payload");
                }
                auto& record = event->Record;
                auto [bytes, error] = ValidateDataRange(
                    record.GetDiskId(), record.GetStartIndex(),
                    record.GetBlocksCount(), record.GetBlockSize(), Config);
                if (HasError(error)) {
                    return error;
                }
                if (auto error = reservation->AddBytes(GetDataReservationBytes(
                        bytes, record.GetBlocksCount()));
                    HasError(error))
                {
                    return error;
                }
                auto op = Prepare<
                    NProto::TReadBlocksLocalRequest,
                    TEvents::TEvReadBlocksResponse>(
                    channel, request.Cookie, std::move(reservation));
                op->Request->CopyFrom(record);
                op->Payload = TRope(TString(bytes, '\0'));
                op->Sglist = TGuardedSgList(
                    GetBlockSgList(op->Payload, record.GetBlockSize()));
                op->Request->Sglist = op->Sglist;
                Invoke<TBlockStoreReadBlocksLocalMethod>(std::move(op));
                return {};
            }
            case TEvents::EvWriteBlocksRequest: {
                auto* event = request.Get<TEvents::TEvWriteBlocksRequest>();
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
                auto [bytes, error] = ValidateDataRange(
                    record.GetDiskId(), record.GetStartIndex(), count,
                    record.GetBlockSize(), Config);
                if (HasError(error)) {
                    return error;
                }
                if (auto error = reservation->AddBytes(
                        GetDataReservationBytes(0, count));
                    HasError(error))
                {
                    return error;
                }
                auto op = Prepare<NProto::TWriteBlocksLocalRequest,
                                  TEvents::TEvWriteBlocksResponse>(
                    channel, request.Cookie, std::move(reservation));
                static_cast<NProto::TWriteBlocksRequest&>(*op->Request) =
                    std::move(record);
                op->Request->BlocksCount = count;
                op->Payload = payload;
                op->Sglist = TGuardedSgList(
                    GetBlockSgList(op->Payload, op->Request->GetBlockSize()));
                op->Request->Sglist = op->Sglist;
                Invoke<TBlockStoreWriteBlocksLocalMethod>(std::move(op));
                return {};
            }
            case TEvents::EvZeroBlocksRequest: {
                auto* event = request.Get<TEvents::TEvZeroBlocksRequest>();
                if (event->GetPayloadCount()) {
                    return MakeError(
                        E_ARGUMENT,
                        "unexpected IC V2 zero payload");
                }
                auto& record = event->Record;
                if (auto error = ValidateRange(record.GetDiskId(),
                                               record.GetStartIndex(),
                                               record.GetBlocksCount());
                    HasError(error))
                {
                    return error;
                }
                auto op = Prepare<NProto::TZeroBlocksRequest,
                                  TEvents::TEvZeroBlocksResponse>(
                    channel, request.Cookie, std::move(reservation));
                *op->Request = std::move(record);
                Invoke<TBlockStoreZeroBlocksMethod>(std::move(op));
                return {};
            }
            default:
                return MakeError(E_BADMSG, "unsupported IC V2 data event");
        }
    }

    template <typename TRequest, typename TResponse>
    std::shared_ptr<TOperation<TRequest, TResponse>> Prepare(
        const IRequestChannelPtr& channel, ui64 cookie,
        TReservation reservation)
    {
        auto op = std::make_shared<TOperation<TRequest, TResponse>>();
        op->Channel = channel;
        op->Cookie = cookie;
        op->Reservation = std::move(reservation);
        return op;
    }

    template <typename TMethod, typename TOperation>
    void Invoke(std::shared_ptr<TOperation> op)
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
                            std::is_same_v<TMethod,
                                           TBlockStoreReadBlocksLocalMethod>)
                        {
                            response.ClearBlocks();
                            if (!HasError(response.GetError())) {
                                op->Response->AddPayload(
                                    std::move(op->Payload));
                            }
                        }
                        op->Response->Record.Swap(&response);
                        if (auto channel = op->Channel.lock()) {
                            if (!channel->Reply(op->Cookie,
                                                std::move(op->Response))) {
                                channel->Close();
                            }
                        }
                        op.reset();
                    });
            });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IDataTargetPtr> CreateDataTarget(
    IBlockStorePtr service, ITaskQueuePtr executor, TDataConfig config)
{
    if (!service || !executor || !ValidateDataConfig(config)) {
        return MakeError(E_ARGUMENT, "invalid IC V2 data target configuration");
    }
    return IDataTargetPtr(std::make_shared<TDataTarget>(
        std::move(service), std::move(executor), config));
}

}   // namespace NCloud::NBlockStore::NIcV2
