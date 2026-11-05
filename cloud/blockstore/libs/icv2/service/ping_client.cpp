#include "ping_client.h"

#include "request_client.h"

#include <cloud/blockstore/libs/icv2/transport/diagnostic.h>

namespace NCloud::NBlockStore::NIcV2 {
namespace {

class TPingClient final: public IPingClient
{
    const IRequestClientPtr Requests;
    const ITimerPtr Timer;

public:
    TPingClient(IRequestClientPtr requests, ITimerPtr timer)
        : Requests(std::move(requests))
        , Timer(std::move(timer))
    {}

    ~TPingClient() override
    {
        Stop();
    }

    void Start() override
    {
        Requests->Start();
    }

    void RequestStop() override
    {
        Requests->RequestStop();
    }

    void Stop() override
    {
        Requests->Stop();
    }

    TPingFuture Ping(TString cellId, TString payload,
                     TDuration timeout) override
    {
        const auto started = Timer->Now();
        auto event = std::make_unique<TEvDiagnostic::TEvPingRequest>();
        event->Record.SetPayload(payload);
        auto future = Requests->Request(cellId, std::move(event),
                                        started + timeout, payload.size());
        return future.Apply(
            [cellId = std::move(cellId), payload = std::move(payload), started,
             timer = Timer](const TRequestClientFuture& future) -> TPingResult
            {
                const auto& result = future.GetValue();
                if (HasError(result.GetError())) {
                    return result.GetError();
                }
                try {
                    auto* event =
                        result.GetResult()
                            .Event->Get<TEvDiagnostic::TEvPingResponse>();
                    if (event->Record.GetPayload() == payload) {
                        return TPingResponse{cellId, event->Record.GetPayload(),
                                             timer->Now() - started};
                    }
                } catch (...) {
                }
                if (auto channel = result.GetResult().Channel.lock()) {
                    channel->Close();
                }
                return MakeError(E_BADMSG, "invalid IC V2 ping response");
            });
    }
};
}   // namespace

TResultOrError<IPingClientPtr> CreatePingClient(
    TPingClientConfig config, IPeerConnectionsPtr peers, ITimerPtr timer,
    ISchedulerPtr scheduler, ITaskQueuePtr completions)
{
    if (!peers) {
        return MakeError(E_ARGUMENT, "missing IC V2 peers");
    }
    auto [requests, error] = CreateRequestClient(
        {config.MaxPendingRequests, config.MaxPayloadBytes},
        [peers = std::move(peers)](const TString& cellId) {
            return peers->GetChannel(cellId);
        }, timer, std::move(scheduler), std::move(completions));
    if (HasError(error)) {
        return error;
    }
    return IPingClientPtr(
        std::make_shared<TPingClient>(std::move(requests), std::move(timer)));
}
}   // namespace NCloud::NBlockStore::NIcV2
