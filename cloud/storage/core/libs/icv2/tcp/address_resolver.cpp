#include "address_resolver.h"

#include "address_resolver_private.h"

#include <cloud/storage/core/libs/common/task_queue.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/interconnect/events_local.h>
#include <contrib/ydb/library/actors/interconnect/interconnect.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;

struct TEvResolve final
    : public TEventLocal<TEvResolve, EventSpaceBegin(TEvents::ES_PRIVATE)>
{
    TString Host;
    ui16 Port;
    TMonotonic Deadline;
    NThreading::TPromise<TResolveResult> Promise;

    TEvResolve(
        TString host,
        ui16 port,
        TMonotonic deadline,
        NThreading::TPromise<TResolveResult> promise)
        : Host(std::move(host))
        , Port(port)
        , Deadline(deadline)
        , Promise(std::move(promise))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TAddressResolverActor final: public TActor<TAddressResolverActor>
{
    const ITaskQueuePtr Completions;

    ui64 NextRequestId = 0;
    THashMap<ui64, NThreading::TPromise<TResolveResult>> Pending;

public:
    explicit TAddressResolverActor(ITaskQueuePtr completions)
        : TActor(&TThis::StateWork)
        , Completions(std::move(completions))
    {}

private:
    STRICT_STFUNC(StateWork, {
        hFunc(TEvResolve, HandleResolveRequest);
        hFunc(TEvLocalNodeInfo, HandleResolveResult);
        hFunc(TEvResolveError, HandleResolveError);
        sFunc(TEvents::TEvPoison, Stop);
    });

    void HandleResolveRequest(TEvResolve::TPtr& ev)
    {
        auto& request = *ev->Get();
        const auto requestId = ++NextRequestId;
        Pending.emplace(requestId, std::move(request.Promise));
        // NodeId selects the reply with every address; IC V2 peers have no
        // NodeId.
        RegisterWithSameMailbox(CreateResolveActor(
            request.Host,
            request.Port,
            0,
            {},
            SelfId(),
            // IC puts ReplyFrom into Sender instead of preserving a cookie.
            TActorId(0, 0, requestId, 0),
            request.Deadline));
    }

    void HandleResolveResult(TEvLocalNodeInfo::TPtr& ev)
    {
        TVector<NInterconnect::TAddress> addresses;
        addresses.reserve(ev->Get()->Addresses.size());
        for (const auto& source: ev->Get()->Addresses) {
            // The textual constructor also initializes sockaddr padding.
            addresses.emplace_back(source.GetAddress(), source.GetPort());
        }
        if (addresses.empty()) {
            Complete(
                ev->Sender.LocalId(),
                MakeError(
                    E_UNAVAILABLE,
                    "DNS lookup returned no IP addresses"));
            return;
        }
        Complete(ev->Sender.LocalId(), std::move(addresses));
    }

    void HandleResolveError(TEvResolveError::TPtr& ev)
    {
        const auto& error = *ev->Get();
        // The legacy resolve actor reports its deadline as text.
        const auto code =
            error.Explain == "Deadline" ? E_TIMEOUT : E_UNAVAILABLE;
        Complete(
            ev->Sender.LocalId(),
            MakeError(
                code,
                TStringBuilder() << "Cannot resolve " << error.Host << ": "
                                 << error.Explain));
    }

    void Complete(ui64 requestId, TResolveResult result)
    {
        auto* pending = Pending.FindPtr(requestId);
        if (!pending) {
            return;
        }

        auto promise = std::move(*pending);
        Pending.erase(requestId);
        Deliver(std::move(promise), std::move(result));
    }

    void Deliver(
        NThreading::TPromise<TResolveResult> promise,
        TResolveResult result)
    {
        Completions->ExecuteSimple(
            [promise = std::move(promise), result = std::move(result)]() mutable
            { promise.SetValue(std::move(result)); });
    }

    void Stop()
    {
        for (auto& [requestId, promise]: Pending) {
            Deliver(
                std::move(promise),
                MakeError(E_CANCELLED, "DNS lookup cancelled"));
        }
        Pending.clear();
        PassAway();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAddressResolver final: public IAddressResolver
{
    TActorSystem* const ActorSystem;
    const TActorId Actor;

public:
    TAddressResolver(TActorSystem* actorSystem, ITaskQueuePtr completions)
        : ActorSystem(actorSystem)
        , Actor(actorSystem->Register(
              NPrivate::CreateAddressResolverActor(std::move(completions))
                  .release()))
    {}

    ~TAddressResolver() override
    {
        ActorSystem->Send(new IEventHandle(Actor, {}, new TEvents::TEvPoison));
    }

    TResolveFuture
    Resolve(const TString& host, ui16 port, TMonotonic deadline) override
    {
        auto promise = NThreading::NewPromise<TResolveResult>();
        auto future = promise.GetFuture();
        ActorSystem->Send(new IEventHandle(
            Actor,
            {},
            NPrivate::CreateResolveRequest(
                host,
                port,
                deadline,
                std::move(promise))
                .release()));
        return future;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IAddressResolver::~IAddressResolver() = default;

NActors::IActorPtr NPrivate::CreateAddressResolverActor(
    ITaskQueuePtr completions)
{
    return std::make_unique<TAddressResolverActor>(std::move(completions));
}

NActors::IEventBasePtr NPrivate::CreateResolveRequest(
    TString host,
    ui16 port,
    TMonotonic deadline,
    NThreading::TPromise<TResolveResult> promise)
{
    return std::make_unique<TEvResolve>(
        std::move(host),
        port,
        deadline,
        std::move(promise));
}

IAddressResolverPtr CreateAddressResolver(
    NActors::TActorSystem* actorSystem,
    ITaskQueuePtr completions)
{
    Y_DEBUG_ABORT_UNLESS(actorSystem);
    Y_DEBUG_ABORT_UNLESS(completions);

    return std::make_shared<TAddressResolver>(
        actorSystem,
        std::move(completions));
}

}   // namespace NCloud::NIcV2
