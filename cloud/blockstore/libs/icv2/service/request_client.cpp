#include "request_client.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <mutex>
#include <set>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeTimeoutError()
{
    return MakeError(E_TIMEOUT, "IC V2 request timed out");
}

struct TClientRequest
{
    ui64 Id;
    TString Key;
    std::unique_ptr<NActors::IEventBase> Event;
    ui64 Bytes;
    TInstant Deadline;
    NThreading::TPromise<TRequestClientResult> Promise =
        NThreading::NewPromise<TRequestClientResult>();
};

struct TConnectingPeer
{
    ui64 Id;
    THashSet<ui64> Requests;
};

class TRequestClient final
    : public IRequestClient
    , public std::enable_shared_from_this<TRequestClient>
{
    const TRequestClientConfig Config;
    const TChannelProvider Provider;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ITaskQueuePtr Completions;

    std::mutex LifecycleMutex;
    std::mutex Mutex;
    THashMap<ui64, std::shared_ptr<TClientRequest>> Pending;
    THashMap<TString, TConnectingPeer> Connecting;
    std::set<std::pair<TInstant, ui64>> Deadlines;
    ui64 NextRequestId = 1;
    ui64 NextConnectionId = 1;
    size_t Outstanding = 0;
    ui64 PayloadBytes = 0;
    size_t Callbacks = 0;
    bool TimerArmed = false;
    bool Started = false;
    bool Stopping = false;
    bool Published = false;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    TRequestClient(TRequestClientConfig config, TChannelProvider provider,
                   ITimerPtr timer, ISchedulerPtr scheduler,
                   ITaskQueuePtr completions)
        : Config(config)
        , Provider(std::move(provider))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Completions(std::move(completions))
    {}

    ~TRequestClient() override
    {
        Stop();
    }

    void Start() override
    {
        std::lock_guard lifecycle(LifecycleMutex);
        std::lock_guard guard(Mutex);
        Y_ABORT_UNLESS(!Started && !Stopping);
        Started = true;
    }

    TRequestClientFuture Request(TString key,
                                 std::unique_ptr<NActors::IEventBase> event,
                                 TInstant deadline, ui64 payloadBytes) override
    {
        const auto keepAlive = shared_from_this();
        std::shared_ptr<TClientRequest> request;
        ui64 connectionId = 0;
        {
            std::lock_guard guard(Mutex);
            if (!Started || Stopping) {
                return NThreading::MakeFuture<TRequestClientResult>(MakeError(
                    E_CANCELLED, "IC V2 request client is not running"));
            }
            if (key.empty() || !event) {
                return NThreading::MakeFuture<TRequestClientResult>(
                    MakeError(E_ARGUMENT, "empty IC V2 request key or event"));
            }
            if (Timer->Now() >= deadline) {
                return NThreading::MakeFuture<TRequestClientResult>(
                    MakeTimeoutError());
            }
            if (Outstanding >= Config.MaxPendingRequests ||
                payloadBytes > Config.MaxPayloadBytes ||
                PayloadBytes > Config.MaxPayloadBytes - payloadBytes)
            {
                return NThreading::MakeFuture<TRequestClientResult>(MakeError(
                    E_REJECTED, "IC V2 request admission limit reached"));
            }
            Y_ABORT_UNLESS(NextRequestId, "IC V2 request IDs exhausted");
            request = std::make_shared<TClientRequest>(
                NextRequestId++, std::move(key), std::move(event), payloadBytes,
                deadline);
            Pending.emplace(request->Id, request);
            Deadlines.emplace(deadline, request->Id);
            ++Outstanding;
            PayloadBytes += payloadBytes;
            auto [it, inserted] = Connecting.try_emplace(request->Key);
            if (inserted) {
                Y_ABORT_UNLESS(NextConnectionId,
                               "IC V2 connection IDs exhausted");
                connectionId = NextConnectionId++;
                it->second.Id = connectionId;
                ++Callbacks;
            }
            it->second.Requests.insert(request->Id);
        }
        ArmTimer();
        if (connectionId) {
            const std::weak_ptr<TRequestClient> weak = shared_from_this();
            Provider(request->Key)
                .Subscribe(
                    [weak, key = request->Key,
                     connectionId](const TChannelFuture& future)
                    {
                        if (auto self = weak.lock()) {
                            self->OnChannel(key, connectionId,
                                            future.GetValue());
                        }
                    });
            FinishCallback();
        }
        return request->Promise.GetFuture();
    }

    NThreading::TFuture<void> WhenClosed() const override
    {
        return Closed.GetFuture();
    }

    void RequestStop() override
    {
        THashMap<ui64, std::shared_ptr<TClientRequest>> pending;
        ui64 bytes = 0;
        {
            std::lock_guard guard(Mutex);
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
                        E_CANCELLED, "IC V2 request client is stopping");
                    for (const auto& [id, request]: pending) {
                        request->Promise.SetValue(error);
                    }
                    self->FinishResults(pending.size(), bytes);
                });
        }
        PublishClosed();
    }

    void Stop() override
    {
        std::lock_guard lifecycle(LifecycleMutex);
        RequestStop();
        Closed.GetFuture().GetValueSync();
    }

private:
    void OnChannel(const TString& cellId, ui64 connectionId,
                   const TChannelResult& result)
    {
        THashSet<ui64> requests;
        {
            std::lock_guard guard(Mutex);
            const auto it = Connecting.find(cellId);
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
            std::lock_guard guard(Mutex);
            const auto it = Pending.find(id);
            if (Stopping || it == Pending.end()) {
                return;
            }
            const auto now = Timer->Now();
            if (now < it->second->Deadline) {
                remaining = it->second->Deadline - now;
                event = std::move(it->second->Event);
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
        const std::weak_ptr<TRequestClient> weak = shared_from_this();
        const auto& channel = result.GetResult();
        const std::weak_ptr<IRequestChannel> weakChannel = channel;
        channel->Request(std::move(event), remaining)
            .Subscribe(
                [weak, id, weakChannel](const TResponseFuture& future)
                {
                    if (auto self = weak.lock()) {
                        if (HasError(future.GetValue().GetError())) {
                            self->Complete(id, future.GetValue().GetError());
                        } else {
                            self->Complete(
                                id,
                                TRequestClientResponse{
                                    future.GetValue().GetResult(),
                                    weakChannel});
                        }
                    }
                });
    }

    void Complete(ui64 id, TRequestClientResult result)
    {
        std::shared_ptr<TClientRequest> request;
        {
            std::lock_guard guard(Mutex);
            const auto it = Pending.find(id);
            if (it == Pending.end()) {
                return;
            }
            request = std::move(it->second);
            Pending.erase(it);
            Deadlines.erase({request->Deadline, id});
            if (auto peer = Connecting.find(request->Key);
                peer != Connecting.end())
            {
                peer->second.Requests.erase(id);
            }
            ++Callbacks;
        }
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             request, result = std::move(result)]() mutable
            {
                bool stopping;
                {
                    std::lock_guard guard(self->Mutex);
                    stopping = self->Stopping;
                }
                const auto now = self->Timer->Now();
                if (stopping) {
                    result = MakeError(
                        E_CANCELLED, "IC V2 request client is stopping");
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
            std::lock_guard guard(Mutex);
            if (Stopping || TimerArmed || Deadlines.empty()) {
                return;
            }
            deadline = Min(Deadlines.begin()->first,
                           Timer->Now() + TDuration::MilliSeconds(10));
            TimerArmed = true;
        }
        const std::weak_ptr<TRequestClient> weak = shared_from_this();
        Scheduler->Schedule(
            nullptr,
            deadline,
            [weak]
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
            std::lock_guard guard(Mutex);
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
            std::lock_guard guard(Mutex);
            TimerArmed = false;
        }
        ArmTimer();
    }

    void FinishResults(size_t count, ui64 bytes)
    {
        {
            std::lock_guard guard(Mutex);
            Y_ABORT_UNLESS(Outstanding >= count && PayloadBytes >= bytes);
            Outstanding -= count;
            PayloadBytes -= bytes;
        }
        FinishCallback();
    }

    void FinishCallback()
    {
        {
            std::lock_guard guard(Mutex);
            Y_ABORT_UNLESS(Callbacks);
            --Callbacks;
        }
        PublishClosed();
    }

    void PublishClosed()
    {
        {
            std::lock_guard guard(Mutex);
            if (!Stopping || Callbacks || Published) {
                return;
            }
            Y_ABORT_UNLESS(!Outstanding && !PayloadBytes && Pending.empty());
            Published = true;
        }
        Closed.SetValue();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IRequestClientPtr> CreateRequestClient(
    TRequestClientConfig config, TChannelProvider provider, ITimerPtr timer,
    ISchedulerPtr scheduler, ITaskQueuePtr completions)
{
    if (!config.MaxPendingRequests || !config.MaxPayloadBytes || !provider ||
        !timer || !scheduler || !completions)
    {
        return MakeError(E_ARGUMENT,
                         "invalid IC V2 request client configuration");
    }
    return IRequestClientPtr(std::make_shared<TRequestClient>(
        config, std::move(provider), std::move(timer), std::move(scheduler),
        std::move(completions)));
}

}   // namespace NCloud::NBlockStore::NIcV2
