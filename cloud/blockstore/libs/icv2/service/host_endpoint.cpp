#include "host_endpoint.h"

#include <atomic>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::TResponseFuture;
using NCloud::NIcV2::TResponseResult;

namespace {

class THostChannel final: public IRequestChannel
{
    const TString CellId;
    const ui64 BindingId;
    const IPeerConnectionsPtr Peers;
    const IRequestClientPtr Requests;
    const ITimerPtr Timer;
    std::atomic<bool> Closing = false;

public:
    THostChannel(
        TString cellId,
        ui64 bindingId,
        IPeerConnectionsPtr peers,
        IRequestClientPtr requests,
        ITimerPtr timer)
        : CellId(std::move(cellId))
        , BindingId(bindingId)
        , Peers(std::move(peers))
        , Requests(std::move(requests))
        , Timer(std::move(timer))
    {}

    ~THostChannel() override
    {
        Close();
    }

    bool Activate() override
    {
        return false;
    }   // cache activates the real channel

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        if (Closing) {
            return NThreading::MakeFuture<TResponseResult>(
                MakeError(E_CANCELLED, "IC V2 host binding closed"));
        }
        if (!event) {
            return NThreading::MakeFuture<TResponseResult>(
                MakeError(E_ARGUMENT, "empty IC V2 data event"));
        }
        const auto bytes = event->CalculateSerializedSize();
        return Requests
            ->Request(CellId, std::move(event), Timer->Now() + timeout, bytes)
            .Apply(
                [](TRequestClientFuture future) -> TResponseResult
                {
                    auto [response, error] = future.ExtractValue();
                    if (HasError(error)) {
                        // Only channel/connection failures reach this branch.
                        // Storage errors travel inside the response event.
                        const auto code = error.GetCode();
                        if (code == E_TRANSPORT_ERROR || code == E_IO ||
                            FACILITY_FROM_CODE(code) == FACILITY_SYSTEM)
                        {
                            error.SetCode(E_UNAVAILABLE);
                        }
                        return error;
                    }
                    return std::move(response.Event);
                });
    }

    void Close() override
    {
        if (!Closing.exchange(true)) {
            Requests->RequestStop();
            Peers->UnregisterHost(CellId, BindingId);
        }
    }

    NThreading::TFuture<NProto::TError> WhenClosed() const override
    {
        return Requests->WhenClosed().Apply(
            [](const auto& future)
            {
                future.GetValue();
                return NProto::TError{};
            });
    }
};
}   // namespace

TResultOrError<THostDataEndpoint> CreateHostDataEndpoint(
    const TString& cellId,
    ui64 bindingId,
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler,
    IPeerConnectionsPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    TDataConfig dataConfig)
{
    auto error = peers->RegisterHost(
        cellId,
        bindingId,
        std::move(host),
        port,
        std::move(handler));
    if (HasError(error)) {
        return error;
    }
    auto [requests, requestError] = CreateRequestClient(
        {dataConfig.MaxInflightRequests, dataConfig.MaxInflightBytes},
        [peers, cellId, bindingId](const TString&)
        { return peers->GetHostChannel(cellId, bindingId); },
        timer,
        std::move(scheduler),
        std::move(completions));
    if (HasError(requestError)) {
        peers->UnregisterHost(cellId, bindingId);
        return requestError;
    }
    requests->Start();
    auto closed = requests->WhenClosed();
    auto channel = std::make_shared<THostChannel>(
        cellId,
        bindingId,
        peers,
        std::move(requests),
        timer);
    auto [endpoint, endpointError] =
        CreateDataEndpoint(std::move(channel), dataConfig);
    if (HasError(endpointError)) {
        return endpointError;
    }
    return THostDataEndpoint{std::move(endpoint), std::move(closed)};
}
}   // namespace NCloud::NBlockStore::NIcV2
