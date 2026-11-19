#include "host_endpoint.h"

#include <cloud/blockstore/libs/service/service_method.h>

#include <atomic>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::TResponseFuture;
using NCloud::NIcV2::TResponseResult;

namespace {

class THostChannel final: public IRequestChannel
{
    const IPeerPtr Peer;
    const IRequestClientPtr Requests;
    const ITimerPtr Timer;
    std::atomic<bool> Closing = false;

public:
    THostChannel(IPeerPtr peer, IRequestClientPtr requests, ITimerPtr timer)
        : Peer(std::move(peer))
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
            ->Request("data", std::move(event), Timer->Now() + timeout, bytes)
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
            Peer->Close();
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

////////////////////////////////////////////////////////////////////////////////

class THostDataEndpointAdapter final
    : public TBlockStoreImpl<THostDataEndpointAdapter, IDataEndpoint>
{
private:
    const IDataEndpointPtr Endpoint;
    std::atomic<bool> Stopping = false;

public:
    explicit THostDataEndpointAdapter(IDataEndpointPtr endpoint)
        : Endpoint(std::move(endpoint))
    {}

    void Start() override
    {
        Endpoint->Start();
    }

    void RequestStop() override
    {
        Stopping = true;
        Endpoint->RequestStop();
    }

    void Stop() override
    {
        RequestStop();
        Endpoint->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Endpoint->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr context,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        if (Stopping) {
            return NThreading::MakeFuture<typename TMethod::TResponse>(
                TErrorResponse(E_CANCELLED, "IC V2 host binding closed"));
        }
        return TMethod::Execute(
            Endpoint.get(),
            std::move(context),
            std::move(request));
    }
};
}   // namespace

TResultOrError<THostDataEndpoint> CreateHostDataEndpoint(
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler,
    IPeerManagerPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    TDataConfig dataConfig)
{
    auto [peer, error] =
        peers->CreatePeer(std::move(host), port, std::move(handler));
    if (HasError(error)) {
        return error;
    }
    auto [requests, requestError] = CreateRequestClient(
        {dataConfig.MaxInflightRequests, dataConfig.MaxInflightBytes},
        [peer](const TString&) { return peer->GetChannel(); },
        timer,
        std::move(scheduler),
        std::move(completions));
    if (HasError(requestError)) {
        peer->Close();
        return requestError;
    }
    requests->Start();
    auto closed = requests->WhenClosed();
    auto channel =
        std::make_shared<THostChannel>(peer, std::move(requests), timer);
    auto [endpoint, endpointError] =
        CreateDataEndpoint(std::move(channel), dataConfig);
    if (HasError(endpointError)) {
        return endpointError;
    }
    return THostDataEndpoint{
        .Peer = peer,
        .Endpoint =
            std::make_shared<THostDataEndpointAdapter>(std::move(endpoint)),
        .Closed = std::move(closed),
    };
}
}   // namespace NCloud::NBlockStore::NIcV2
