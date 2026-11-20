#include "host_endpoint.h"

#include "request_client.h"

#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <atomic>

namespace NCloud::NBlockStore::NIcV2 {

using namespace NThreading;

using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::TResponseFuture;
using NCloud::NIcV2::TResponseResult;

namespace {

////////////////////////////////////////////////////////////////////////////////

class THostRequestChannel final: public IRequestChannel
{
    const IPeerRequestClientPtr Requests;
    const ITimerPtr Timer;

public:
    THostRequestChannel(IPeerRequestClientPtr requests, ITimerPtr timer)
        : Requests(std::move(requests))
        , Timer(std::move(timer))
    {}

    ~THostRequestChannel() override
    {
        Close();
    }

    bool Activate() override
    {
        // The peer supplies an already active channel.
        return false;
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        return Requests->Request(std::move(event), Timer->Now() + timeout)
            .Apply(
                [](TResponseFuture future) -> TResponseResult
                {
                    auto [response, error] = future.ExtractValue();
                    if (HasError(error)) {
                        // Only channel/connection failures reach this branch.
                        // Storage errors travel inside the response event.
                        const auto code = error.GetCode();
                        if (code == E_TRANSPORT_ERROR || code == E_IO ||
                            code == E_ABORTED ||
                            FACILITY_FROM_CODE(code) == FACILITY_SYSTEM)
                        {
                            error.SetCode(E_UNAVAILABLE);
                        }
                        return error;
                    }
                    return std::move(response);
                });
    }

    void Close() override
    {
        Requests->RequestStop();
    }

    [[nodiscard]] TFuture<NProto::TError> WhenClosed() const override
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
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr context,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        using TResponse = TMethod::TResponse;
        if (Stopping) {
            return MakeFuture<TResponse>(
                TErrorResponse(E_CANCELLED, "IC V2 host binding closed"));
        }

        // The data endpoint can also detect a bad message while decoding.
        return TMethod::Execute(
                   Endpoint.get(),
                   std::move(context),
                   std::move(request))
            .Apply(
                [](TFuture<TResponse> future)
                {
                    auto response = future.ExtractValue();
                    if (response.GetError().GetCode() == E_BADMSG) {
                        response.MutableError()->SetCode(E_UNAVAILABLE);
                    }
                    return response;
                });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

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
    if (!peers || !timer || !scheduler || !completions || !dataConfig.IsValid())
    {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 host endpoint configuration");
    }

    auto [peer, error] =
        peers->CreatePeer(std::move(host), port, std::move(handler));
    if (HasError(error)) {
        return error;
    }
    auto [requests, requestError] = CreatePeerRequestClient(
        peer,
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
        std::make_shared<THostRequestChannel>(std::move(requests), timer);
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
