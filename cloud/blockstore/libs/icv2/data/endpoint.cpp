#include "data.h"
#include "protocol.h"
#include "request_tracker.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_method.h>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
NThreading::TFuture<TResponse> Fail(const NProto::TError& error)
{
    return NThreading::MakeFuture<TResponse>(TErrorResponse(error));
}

template <typename TResponse>
void ProcessThrottleTime(const TCallContextPtr& context, TResponse& response)
{
    auto& info = *response.MutableHeaders()->MutableThrottler();
    context->AddTime(EProcessingStage::Postponed,
                     TDuration::MicroSeconds(info.GetDelay()));
    context->AddTime(EProcessingStage::Shaping,
                     TDuration::MicroSeconds(info.GetShapingDelay()));
    context->SetPossiblePostponeDuration(TDuration::Zero());
    info.SetDelay(0);
    info.SetShapingDelay(0);
}

TSgList GetPayloadSgList(const TRope& payload)
{
    TSgList result;
    for (auto it = payload.Begin(); it != payload.End();
         it.AdvanceToNextContiguousBlock())
    {
        result.emplace_back(it.ContiguousData(), it.ContiguousSize());
    }
    return result;
}

class TDataEndpoint final: public TBlockStoreImpl<TDataEndpoint, IDataEndpoint>
{
    const IRequestChannelPtr Channel;
    const ITimerPtr Timer;
    const TDataConfig Config;
    const std::shared_ptr<TRequestTracker> Tracker;

public:
    TDataEndpoint(IRequestChannelPtr channel, ITimerPtr timer,
                  TDataConfig config)
        : Channel(std::move(channel))
        , Timer(std::move(timer))
        , Config(config)
        , Tracker(std::make_shared<TRequestTracker>(config))
    {}

    ~TDataEndpoint() override
    {
        RequestStop();
    }

    void Start() override
    {}

    void RequestStop() override
    {
        Tracker->RequestStop();
        Channel->Close();
    }

    void Stop() override
    {
        RequestStop();
        Tracker->Stop();
        Channel->WhenClosed().GetValueSync();
    }

    TStorageBuffer AllocateBuffer(size_t) override
    {
        return nullptr;
    }

    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr, std::shared_ptr<typename TMethod::TRequest>)
    {
        return Fail<typename TMethod::TResponse>(
            MakeError(E_NOT_IMPLEMENTED, "unsupported IC V2 data operation"));
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr context,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        using TResponse = NProto::TReadBlocksLocalResponse;
        const auto started = Timer->Now();
        if (!request || !context) {
            return Fail<TResponse>(
                MakeError(E_ARGUMENT, "empty IC V2 read request"));
        }
        auto [bytes, error] = ValidateDataRange(
            request->GetDiskId(), request->GetStartIndex(),
            request->GetBlocksCount(), request->GetBlockSize(), Config);
        if (HasError(error)) {
            return Fail<TResponse>(error);
        }
        auto [reservation, admissionError] = Tracker->Acquire(
            GetDataReservationBytes(bytes, request->GetBlocksCount()) +
            request->ByteSizeLong());
        if (HasError(admissionError)) {
            return Fail<TResponse>(admissionError);
        }
        auto event =
            std::make_unique<NStorage::TEvService::TEvReadBlocksRequest>();
        event->Record.CopyFrom(*request);
        const auto deadline =
            started +
            TDuration::MilliSeconds(request->GetHeaders().GetRequestTimeout());
        return Send<TResponse>(
            std::move(event),
            deadline,
            std::move(reservation),
            [request = std::move(request),
             context = std::move(context),
             bytes](NActors::IEventHandle& event, bool& badResponse)
            {
                auto* received =
                    event.Get<NStorage::TEvService::TEvReadBlocksResponse>();
                auto& record = received->Record;
                const bool failed = HasError(record.GetError());
                if (record.GetBlocks().BuffersSize() ||
                    received->GetPayloadCount() != (failed ? 0 : 1) ||
                    (!failed && received->GetPayload(0).GetSize() != bytes))
                {
                    badResponse = true;
                    return TResponse(
                        TErrorResponse(E_BADMSG, "invalid IC V2 read payload"));
                }
                TResponse response(std::move(record));
                if (!failed) {
                    auto src = GetPayloadSgList(received->GetPayload(0));
                    auto guard = request->Sglist.Acquire();
                    if (!guard) {
                        return TResponse(TErrorResponse(
                            E_CANCELLED, "IC V2 read sglist is closed"));
                    }
                    if (SgListGetSize(guard.Get()) != bytes) {
                        return TResponse(TErrorResponse(
                            E_ARGUMENT, "invalid IC V2 read sglist size"));
                    }
                    SgListCopy(src, guard.Get());
                }
                response.ClearBlocks();
                ProcessThrottleTime(context, response);
                return response;
            });
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr context,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        using TResponse = NProto::TWriteBlocksLocalResponse;
        const auto started = Timer->Now();
        if (!request || !context) {
            return Fail<TResponse>(
                MakeError(E_ARGUMENT, "empty IC V2 write request"));
        }
        auto [bytes, error] = ValidateDataRange(
            request->GetDiskId(), request->GetStartIndex(),
            request->BlocksCount, request->GetBlockSize(), Config);
        if (HasError(error)) {
            return Fail<TResponse>(error);
        }
        auto [reservation, admissionError] = Tracker->Acquire(
            GetDataReservationBytes(bytes, request->BlocksCount) +
            request->ByteSizeLong());
        if (HasError(admissionError)) {
            return Fail<TResponse>(admissionError);
        }
        auto event =
            std::make_unique<NStorage::TEvService::TEvWriteBlocksRequest>();
        const NProto::TWriteBlocksLocalRequest metadata(
            *request,
            NProto::TWriteBlocksLocalRequest::TDependentTag{});
        event->Record.CopyFrom(metadata);
        {
            auto guard = request->Sglist.Acquire();
            if (!guard) {
                return Fail<TResponse>(
                    MakeError(E_CANCELLED, "IC V2 write sglist is closed"));
            }
            if (SgListGetSize(guard.Get()) != bytes) {
                return Fail<TResponse>(
                    MakeError(E_ARGUMENT, "invalid IC V2 write sglist size"));
            }
            auto payload = TRope::Uninitialized(bytes);
            auto dst = payload.GetContiguousSpanMut();
            SgListCopy(
                guard.Get(),
                TBlockDataRef(dst.GetData(), dst.GetSize()));
            event->AddPayload(std::move(payload));
        }
        const auto deadline =
            started +
            TDuration::MilliSeconds(request->GetHeaders().GetRequestTimeout());
        return Send<TResponse>(
            std::move(event),
            deadline,
            std::move(reservation),
            [context = std::move(context)](NActors::IEventHandle& event, bool&)
            {
                auto response = std::move(
                    event.Get<NStorage::TEvService::TEvWriteBlocksResponse>()
                        ->Record);
                ProcessThrottleTime(context, response);
                return response;
            });
    }

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr context,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        using TResponse = NProto::TZeroBlocksResponse;
        const auto started = Timer->Now();
        if (!request || !context) {
            return Fail<TResponse>(
                MakeError(E_ARGUMENT, "empty IC V2 zero request"));
        }
        if (auto error =
                ValidateRange(request->GetDiskId(), request->GetStartIndex(),
                              request->GetBlocksCount());
            HasError(error))
        {
            return Fail<TResponse>(error);
        }
        auto [reservation, admissionError] =
            Tracker->Acquire(request->ByteSizeLong());
        if (HasError(admissionError)) {
            return Fail<TResponse>(admissionError);
        }
        auto event =
            std::make_unique<NStorage::TEvService::TEvZeroBlocksRequest>();
        event->Record.CopyFrom(*request);
        const auto deadline =
            started +
            TDuration::MilliSeconds(request->GetHeaders().GetRequestTimeout());
        return Send<TResponse>(
            std::move(event),
            deadline,
            std::move(reservation),
            [context = std::move(context)](NActors::IEventHandle& event, bool&)
            {
                auto response = std::move(
                    event.Get<NStorage::TEvService::TEvZeroBlocksResponse>()
                        ->Record);
                ProcessThrottleTime(context, response);
                return response;
            });
    }

private:
    template <typename TResponse, typename TDecoder>
    NThreading::TFuture<TResponse> Send(
        std::unique_ptr<NActors::IEventBase> event, TInstant deadline,
        std::shared_ptr<TRequestTracker::TReservation> reservation,
        TDecoder decoder)
    {
        if (event->CalculateSerializedSize() > Config.MaxRequestBytes) {
            return Fail<TResponse>(
                MakeError(E_ARGUMENT, "IC V2 request exceeds size limit"));
        }
        const auto now = Timer->Now();
        if (now >= deadline) {
            return Fail<TResponse>(
                MakeError(E_TIMEOUT, "IC V2 data request timed out"));
        }
        auto promise = NThreading::NewPromise<TResponse>();
        const std::weak_ptr<IRequestChannel> weak = Channel;
        Channel->Request(std::move(event), deadline - now)
            .Subscribe(
                [promise, reservation = std::move(reservation),
                 decoder = std::move(decoder), weak, timer = Timer,
                 deadline](const TResponseFuture& future) mutable
                {
                    bool badResponse = false;
                    auto response = SafeExecute<TResponse>(
                        [&]() -> TResponse
                        {
                            if (auto error = reservation->AddBytes(0);
                                HasError(error)) {
                                return TErrorResponse(error);
                            }
                            const auto& result = future.GetValue();
                            if (HasError(result.GetError())) {
                                return TErrorResponse(result.GetError());
                            }
                            if (timer->Now() >= deadline) {
                                return TErrorResponse(
                                    E_TIMEOUT, "IC V2 data request timed out");
                            }
                            try {
                                return decoder(*result.GetResult(),
                                               badResponse);
                            } catch (...) {
                                badResponse = true;
                                return TErrorResponse(
                                    E_BADMSG, "invalid IC V2 data response");
                            }
                        });
                    if (badResponse) {
                        if (auto channel = weak.lock()) {
                            channel->Close();
                        }
                    }
                    promise.SetValue(std::move(response));
                    reservation.reset();
                });
        return promise.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IDataEndpointPtr> CreateDataEndpoint(
    IRequestChannelPtr channel, ITimerPtr timer, TDataConfig config)
{
    if (!channel || !timer || !ValidateDataConfig(config)) {
        return MakeError(E_ARGUMENT,
                         "invalid IC V2 data endpoint configuration");
    }
    return IDataEndpointPtr(std::make_shared<TDataEndpoint>(
        std::move(channel), std::move(timer), config));
}

}   // namespace NCloud::NBlockStore::NIcV2
