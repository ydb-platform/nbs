#include "address_resolver.h"

#include <cloud/storage/core/libs/icv2/tcp/address_resolver_private.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <utility>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;

class TRegisteredActor final: public TAtomicRefCount<TRegisteredActor>
{
    const IActorSystemPtr ActorSystem;
    const TActorId Id;

public:
    TRegisteredActor(IActorSystemPtr actorSystem, TActorId id)
        : ActorSystem(std::move(actorSystem))
        , Id(id)
    {}

    ~TRegisteredActor()
    {
        Send(std::make_unique<TEvents::TEvPoison>());
    }

    bool Send(IEventBasePtr event) const
    {
        return ActorSystem->Send(Id, std::move(event));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TResolver final: public IActorAddressResolver
{
    const IActorSystemPtr ActorSystem;
    const ITaskQueuePtr Completions;

    THotSwap<TRegisteredActor> Actor;

public:
    TResolver(IActorSystemPtr actorSystem, ITaskQueuePtr completions)
        : ActorSystem(std::move(actorSystem))
        , Completions(std::move(completions))
    {}

    // IStartable implementation
    void Start() override
    {
        Y_DEBUG_ABORT_UNLESS(!Actor.AtomicLoad());

        const auto id = ActorSystem->Register(
            NPrivate::CreateAddressResolverActor(Completions),
            {});
        if (id) {
            Actor.AtomicStore(MakeIntrusive<TRegisteredActor>(ActorSystem, id));
        }
    }

    void Stop() override
    {
        Actor.AtomicStore(nullptr);
    }

    // IAddressResolver implementation
    TResolveFuture
    Resolve(const TString& host, ui16 port, TMonotonic deadline) override
    {
        // Keep the actor alive until the admitted request has been enqueued.
        const auto actor = Actor.AtomicLoad();
        if (!actor) {
            return NThreading::MakeFuture<TResolveResult>(
                MakeError(E_REJECTED, "DNS resolver unavailable"));
        }

        auto promise = NThreading::NewPromise<TResolveResult>();
        auto future = promise.GetFuture();
        if (!actor->Send(
                NPrivate::CreateResolveRequest(host, port, deadline, promise)))
        {
            promise.SetValue(MakeError(E_REJECTED, "DNS resolver unavailable"));
        }
        return future;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorAddressResolverPtr CreateAddressResolver(
    IActorSystemPtr actorSystem,
    ITaskQueuePtr completions)
{
    Y_DEBUG_ABORT_UNLESS(actorSystem);
    Y_DEBUG_ABORT_UNLESS(completions);

    return std::make_shared<TResolver>(
        std::move(actorSystem),
        std::move(completions));
}

}   // namespace NCloud::NIcV2
