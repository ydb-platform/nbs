#include "ping_client.h"

#include <cloud/blockstore/libs/icv2/transport/diagnostic.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NCloud::NBlockStore::NIcV2 {

using namespace NThreading;

using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::TResponseFuture;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Cell routing belongs to the diagnostic client; the connection manager only
// sees ordinary peers identified by host and port.
class TPingChannels
{
private:
    struct TPeer
    {
        const TString Host;
        const ui16 Port;
        // Published once. Pending requests keep TPingChannels alive.
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
        TPromise<TChannelResult> Result =
            NewPromise<TChannelResult>();
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
            return MakeFuture<TChannelResult>(
                MakeError(E_CANCELLED, "IC V2 ping client is stopping"));
        }
        const auto* peers = Cells.FindPtr(cellId);
        if (!peers) {
            return MakeFuture<TChannelResult>(
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

struct TPingRequestLimits
{
    ui32 MaxPendingRequests = 1024;
    ui64 MaxPayloadBytes = 64 * 1024 * 1024;
};

struct TPingRequestResponse
{
    std::unique_ptr<NActors::IEventHandle> Event;
    std::weak_ptr<NCloud::NIcV2::IRequestChannel> Channel;
};

using TPingRequestResult = TResultOrError<TPingRequestResponse>;
// Consume the result once with ExtractValue().
using TPingRequestFuture = TFuture<TPingRequestResult>;

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeTimeoutError()
{
    return MakeError(E_TIMEOUT, "IC V2 request timed out");
}

////////////////////////////////////////////////////////////////////////////////

struct TPendingPing
{
    ui64 Id;
    TString Key;
    std::unique_ptr<NActors::IEventBase> Event;
    ui64 Bytes;
    TInstant Deadline;
    TPromise<TPingRequestResult> Promise =
        NewPromise<TPingRequestResult>();
};

////////////////////////////////////////////////////////////////////////////////

struct TConnectingPeer
{
    ui64 Id;
    THashSet<ui64> Requests;
};

////////////////////////////////////////////////////////////////////////////////

class TPingRequests final
    : public std::enable_shared_from_this<TPingRequests>
{
    const TPingRequestLimits Config;
    const std::shared_ptr<TPingChannels> Channels;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ITaskQueuePtr Completions;

    TMutex LifecycleMutex;
    TMutex Mutex;
    THashMap<ui64, std::shared_ptr<TPendingPing>> Pending;
    THashMap<TString, TConnectingPeer> Connecting;
    TSet<std::pair<TInstant, ui64>> Deadlines;
    ui64 NextRequestId = 1;
    ui64 NextConnectionId = 1;
    size_t Outstanding = 0;
    ui64 PayloadBytes = 0;
    size_t Callbacks = 0;
    bool TimerArmed = false;
    bool Started = false;
    bool Stopping = false;
    bool Published = false;
    TPromise<void> Closed = NewPromise<void>();

public:
    TPingRequests(
        TPingRequestLimits config,
        std::shared_ptr<TPingChannels> channels,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions)
        : Config(config)
        , Channels(std::move(channels))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Completions(std::move(completions))
    {}

    ~TPingRequests()
    {
        Stop();
    }

    void Start()
    {
        TGuard<TMutex> lifecycle(LifecycleMutex);
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(!Started && !Stopping);
        Started = true;
    }

    TPingRequestFuture Request(
        TString key,
        std::unique_ptr<NActors::IEventBase> event,
        TInstant deadline,
        ui64 payloadBytes)
    {
        const auto keepAlive = shared_from_this();
        std::shared_ptr<TPendingPing> request;
        ui64 connectionId = 0;
        {
            TGuard<TMutex> guard(Mutex);
            if (!Started || Stopping) {
                return MakeFuture<TPingRequestResult>(MakeError(
                    E_CANCELLED,
                    "IC V2 request client is not running"));
            }
            if (key.empty() || !event) {
                return MakeFuture<TPingRequestResult>(
                    MakeError(E_ARGUMENT, "empty IC V2 request key or event"));
            }
            if (Timer->Now() >= deadline) {
                return MakeFuture<TPingRequestResult>(MakeTimeoutError());
            }
            if (Outstanding >= Config.MaxPendingRequests ||
                payloadBytes > Config.MaxPayloadBytes ||
                PayloadBytes > Config.MaxPayloadBytes - payloadBytes)
            {
                return MakeFuture<TPingRequestResult>(MakeError(
                    E_REJECTED,
                    "IC V2 request admission limit reached"));
            }
            request = std::make_shared<TPendingPing>(
                NextRequestId++,
                std::move(key),
                std::move(event),
                payloadBytes,
                deadline);
            Pending.emplace(request->Id, request);
            Deadlines.emplace(deadline, request->Id);
            ++Outstanding;
            PayloadBytes += payloadBytes;
            auto [it, inserted] = Connecting.try_emplace(request->Key);
            if (inserted) {
                connectionId = NextConnectionId++;
                it->second.Id = connectionId;
                ++Callbacks;
            }
            it->second.Requests.insert(request->Id);
        }
        ArmTimer();
        if (connectionId) {
            Channels->GetChannel(request->Key)
                .Subscribe(
                    [weak = weak_from_this(),
                     key = request->Key,
                     connectionId]   //
                    (const TChannelFuture& future)
                    {
                        if (auto self = weak.lock()) {
                            self->OnChannel(
                                key,
                                connectionId,
                                future.GetValue());
                        }
                    });
            FinishCallback();
        }
        return request->Promise.GetFuture();
    }

    void RequestStop()
    {
        THashMap<ui64, std::shared_ptr<TPendingPing>> pending;
        ui64 bytes = 0;
        {
            TGuard<TMutex> guard(Mutex);
            if (Stopping) {
                return;
            }
            Stopping = true;
            pending.swap(Pending);
            Connecting.clear();
            Deadlines.clear();
            if (!pending.empty()) {
                ++Callbacks;
                for (const auto& [id, request]: pending) {
                    bytes += request->Bytes;
                }
            }
        }
        if (!pending.empty()) {
            Completions->ExecuteSimple(
                [self = shared_from_this(), pending = std::move(pending), bytes]
                {
                    const auto error = MakeError(
                        E_CANCELLED,
                        "IC V2 request client is stopping");
                    for (const auto& [id, request]: pending) {
                        request->Promise.SetValue(error);
                    }
                    self->FinishResults(pending.size(), bytes);
                });
        }
        PublishClosed();
    }

    void Stop()
    {
        TGuard<TMutex> lifecycle(LifecycleMutex);
        RequestStop();
        Closed.GetFuture().GetValueSync();
    }

private:
    void OnChannel(
        const TString& key,
        ui64 connectionId,
        const TChannelResult& result)
    {
        THashSet<ui64> requests;
        {
            TGuard<TMutex> guard(Mutex);
            const auto it = Connecting.find(key);
            if (Stopping || it == Connecting.end() ||
                it->second.Id != connectionId)
            {
                return;
            }
            requests.swap(it->second.Requests);
            Connecting.erase(it);
            ++Callbacks;
        }
        for (ui64 id: requests) {
            Send(id, result);
        }
        FinishCallback();
    }

    void Send(ui64 id, const TChannelResult& result)
    {
        std::unique_ptr<NActors::IEventBase> event;
        TDuration remaining;
        {
            TGuard<TMutex> guard(Mutex);
            const auto* request = Pending.FindPtr(id);
            if (Stopping || !request) {
                return;
            }
            const auto now = Timer->Now();
            if (now < (*request)->Deadline) {
                remaining = (*request)->Deadline - now;
                event = std::move((*request)->Event);
            }
        }
        if (!remaining) {
            Complete(id, MakeTimeoutError());
            return;
        }
        if (HasError(result.GetError())) {
            Complete(id, result.GetError());
            return;
        }
        const auto& channel = result.GetResult();
        const std::weak_ptr<IRequestChannel> weakChannel = channel;
        channel->Request(std::move(event), remaining)
            .Subscribe(
                [weak = weak_from_this(), id, weakChannel]   //
                (TResponseFuture future)
                {
                    if (auto self = weak.lock()) {
                        auto [event, error] = future.ExtractValue();
                        if (HasError(error)) {
                            self->Complete(id, error);
                        } else {
                            self->Complete(
                                id,
                                TPingRequestResponse{
                                    .Event = std::move(event),
                                    .Channel = weakChannel});
                        }
                    }
                });
    }

    void Complete(ui64 id, TPingRequestResult result)
    {
        std::shared_ptr<TPendingPing> request;
        {
            TGuard<TMutex> guard(Mutex);
            const auto it = Pending.find(id);
            if (it == Pending.end()) {
                return;
            }
            request = std::move(it->second);
            Pending.erase(it);
            Deadlines.erase({request->Deadline, id});
            if (auto* peer = Connecting.FindPtr(request->Key)) {
                peer->Requests.erase(id);
            }
            ++Callbacks;
        }
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             request,
             result = std::move(result)]() mutable
            {
                bool stopping;
                {
                    TGuard<TMutex> guard(self->Mutex);
                    stopping = self->Stopping;
                }
                const auto now = self->Timer->Now();
                if (stopping) {
                    result = MakeError(
                        E_CANCELLED,
                        "IC V2 request client is stopping");
                } else if (now >= request->Deadline) {
                    result = MakeTimeoutError();
                }
                request->Promise.SetValue(std::move(result));
                self->FinishResults(1, request->Bytes);
            });
    }

    void ArmTimer()
    {
        TInstant deadline;
        {
            TGuard<TMutex> guard(Mutex);
            if (Stopping || TimerArmed || Deadlines.empty()) {
                return;
            }
            deadline =
                Min(Deadlines.begin()->first,
                    Timer->Now() + TDuration::MilliSeconds(10));
            TimerArmed = true;
        }
        Scheduler->Schedule(
            nullptr,
            deadline,
            [weak = weak_from_this()]
            {
                if (auto self = weak.lock()) {
                    self->OnTimer();
                }
            });
    }

    void OnTimer()
    {
        TVector<ui64> expired;
        {
            TGuard<TMutex> guard(Mutex);
            if (Stopping) {
                TimerArmed = false;
                return;
            }
            const auto now = Timer->Now();
            for (const auto& [deadline, id]: Deadlines) {
                if (deadline > now) {
                    break;
                }
                expired.push_back(id);
            }
        }
        for (ui64 id: expired) {
            Complete(id, MakeTimeoutError());
        }
        {
            TGuard<TMutex> guard(Mutex);
            TimerArmed = false;
        }
        ArmTimer();
    }

    void FinishResults(size_t count, ui64 bytes)
    {
        {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(Outstanding >= count && PayloadBytes >= bytes);
            Outstanding -= count;
            PayloadBytes -= bytes;
        }
        FinishCallback();
    }

    void FinishCallback()
    {
        {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(Callbacks);
            --Callbacks;
        }
        PublishClosed();
    }

    void PublishClosed()
    {
        {
            TGuard<TMutex> guard(Mutex);
            if (!Stopping || Callbacks || Published) {
                return;
            }
            Y_ABORT_UNLESS(!Outstanding && !PayloadBytes && Pending.empty());
            Published = true;
        }
        Closed.SetValue();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPingClient final: public IPingClient
{
    const std::shared_ptr<TPingRequests> Requests;
    const ITimerPtr Timer;
    const std::shared_ptr<TPingChannels> Channels;

public:
    TPingClient(
        std::shared_ptr<TPingRequests> requests,
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
             timer = Timer](TPingRequestFuture future) -> TPingResult
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

////////////////////////////////////////////////////////////////////////////////

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
    if (!config.MaxPendingRequests || !config.MaxPayloadBytes || !timer ||
        !scheduler || !completions)
    {
        return MakeError(E_ARGUMENT, "invalid IC V2 ping client configuration");
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
    auto requests = std::make_shared<TPingRequests>(
        TPingRequestLimits{
            .MaxPendingRequests = config.MaxPendingRequests,
            .MaxPayloadBytes = config.MaxPayloadBytes},
        channels,
        timer,
        std::move(scheduler),
        std::move(completions));
    return IPingClientPtr(
        std::make_shared<TPingClient>(
            std::move(requests),
            std::move(timer),
            std::move(channels)));
}
}   // namespace NCloud::NBlockStore::NIcV2
