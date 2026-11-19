#include "ping_client.h"

#include "request_client.h"

#include <cloud/blockstore/libs/icv2/transport/diagnostic.h>

#include <atomic>

namespace NCloud::NBlockStore::NIcV2 {
namespace {

// Cell routing belongs to the diagnostic client; the connection manager only
// sees ordinary peers identified by host and port.
class TPingChannels
{
private:
    struct TPeer
    {
        const TString Host;
        const ui16 Port;
        // Published once; readers retain TPingChannels through the provider.
        // RequestStop closes the peer but leaves its handle alive for readers.
        std::atomic<IPeerPtr*> Registration = nullptr;

        ~TPeer()
        {
            delete Registration.load();
        }

        explicit TPeer(TPingPeerAddress address)
            : Host(std::move(address.Host))
            , Port(address.Port)
        {}
    };

    struct TSelection
    {
        NThreading::TPromise<TChannelResult> Result =
            NThreading::NewPromise<TChannelResult>();
        std::atomic<size_t> Remaining;

        explicit TSelection(size_t count)
            : Remaining(count)
        {}

        void Complete(const TChannelResult& result)
        {
            if (!HasError(result.GetError()) || Remaining.fetch_sub(1) == 1) {
                Result.TrySetValue(result);
            }
        }
    };

    const IPeerManagerPtr Manager;
    THashMap<TString, TVector<std::unique_ptr<TPeer>>> Cells;
    std::atomic<bool> Stopping = false;

public:
    TPingChannels(
        IPeerManagerPtr manager,
        THashMap<TString, TVector<TPingPeerAddress>> cells)
        : Manager(std::move(manager))
    {
        for (auto& [cellId, hosts]: cells) {
            auto& peers = Cells[cellId];
            for (auto& host: hosts) {
                peers.push_back(std::make_unique<TPeer>(std::move(host)));
            }
        }
    }

    TChannelFuture GetChannel(const TString& cellId)
    {
        if (Stopping) {
            return NThreading::MakeFuture<TChannelResult>(
                MakeError(E_CANCELLED, "IC V2 ping client is stopping"));
        }
        const auto* peers = Cells.FindPtr(cellId);
        if (!peers) {
            return NThreading::MakeFuture<TChannelResult>(
                MakeError(E_ARGUMENT, "unknown IC V2 peer cell"));
        }
        auto selected = std::make_shared<TSelection>(peers->size());
        for (const auto& peer: *peers) {
            auto* registration = peer->Registration.load();
            if (!registration) {
                auto [created, error] =
                    Manager->CreatePeer(peer->Host, peer->Port, {});
                if (HasError(error)) {
                    selected->Complete(error);
                    continue;
                }
                auto candidate = std::make_unique<IPeerPtr>(std::move(created));
                if (peer->Registration.compare_exchange_strong(
                        registration,
                        candidate.get()))
                {
                    registration = candidate.release();
                }
            }
            if (Stopping) {
                (*registration)->Close();
                selected->Complete(
                    MakeError(E_CANCELLED, "IC V2 ping client is stopping"));
                continue;
            }
            (*registration)
                ->GetChannel()
                .Subscribe([selected](const TChannelFuture& future)
                           { selected->Complete(future.GetValue()); });
        }
        return selected->Result.GetFuture();
    }

    void RequestStop()
    {
        if (Stopping.exchange(true)) {
            return;
        }
        for (const auto& [cellId, peers]: Cells) {
            for (const auto& peer: peers) {
                if (const auto* registration = peer->Registration.load()) {
                    (*registration)->Close();
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPingClient final: public IPingClient
{
    const IRequestClientPtr Requests;
    const ITimerPtr Timer;
    const std::shared_ptr<TPingChannels> Channels;

public:
    TPingClient(
        IRequestClientPtr requests,
        ITimerPtr timer,
        std::shared_ptr<TPingChannels> channels)
        : Requests(std::move(requests))
        , Timer(std::move(timer))
        , Channels(std::move(channels))
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
        Channels->RequestStop();
    }

    void Stop() override
    {
        RequestStop();
        Requests->Stop();
    }

    TPingFuture
    Ping(TString cellId, TString payload, TDuration timeout) override
    {
        const auto started = Timer->Now();
        auto event = std::make_unique<TEvDiagnostic::TEvPingRequest>();
        event->Record.SetPayload(payload);
        auto future = Requests->Request(
            cellId,
            std::move(event),
            started + timeout,
            payload.size());
        return future.Apply(
            [cellId = std::move(cellId),
             payload = std::move(payload),
             started,
             timer = Timer](TRequestClientFuture future) -> TPingResult
            {
                auto result = future.ExtractValue();
                if (HasError(result.GetError())) {
                    return result.GetError();
                }
                try {
                    auto* event =
                        result.GetResult()
                            .Event->Get<TEvDiagnostic::TEvPingResponse>();
                    if (event->Record.GetPayload() == payload) {
                        return TPingResponse{
                            cellId,
                            event->Record.GetPayload(),
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
    TPingClientConfig config,
    IPeerManagerPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions)
{
    if (!peers) {
        return MakeError(E_ARGUMENT, "missing IC V2 peers");
    }
    for (const auto& [cellId, hosts]: config.Peers) {
        if (cellId.empty() || hosts.empty()) {
            return MakeError(E_ARGUMENT, "empty IC V2 diagnostic peer");
        }
        for (const auto& host: hosts) {
            if (host.Host.empty() || !host.Port) {
                return MakeError(
                    E_ARGUMENT,
                    "invalid IC V2 diagnostic endpoint");
            }
        }
    }
    auto channels =
        std::make_shared<TPingChannels>(peers, std::move(config.Peers));
    auto [requests, error] = CreateRequestClient(
        {config.MaxPendingRequests, config.MaxPayloadBytes},
        [channels](const TString& cellId)
        { return channels->GetChannel(cellId); },
        timer,
        std::move(scheduler),
        std::move(completions));
    if (HasError(error)) {
        return error;
    }
    return IPingClientPtr(
        std::make_shared<TPingClient>(
            std::move(requests),
            std::move(timer),
            std::move(channels)));
}
}   // namespace NCloud::NBlockStore::NIcV2
