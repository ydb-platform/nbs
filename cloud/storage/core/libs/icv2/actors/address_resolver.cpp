#include "address_resolver.h"

#include <cloud/storage/core/libs/icv2/tcp/address_resolver_private.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <utility>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;

class TResolver final: public IActorAddressResolver
{
    const IActorSystemPtr ActorSystem;
    const ITaskQueuePtr Completions;
    TActorId Actor;

public:
    TResolver(IActorSystemPtr actorSystem, ITaskQueuePtr completions)
        : ActorSystem(std::move(actorSystem))
        , Completions(std::move(completions))
    {}

    ~TResolver() override
    {
        Stop();
    }

    // IStartable implementation
    void Start() override
    {
        Y_DEBUG_ABORT_UNLESS(!Actor);

        Actor = ActorSystem->Register(
            NPrivate::CreateAddressResolverActor(Completions),
            {});
    }

    void Stop() override
    {
        if (const auto actor = std::exchange(Actor, TActorId{})) {
            ActorSystem->Send(actor, std::make_unique<TEvents::TEvPoison>());
        }
    }

    // IAddressResolver implementation
    TResolveFuture
    Resolve(const TString& host, ui16 port, TMonotonic deadline) override
    {
        Y_DEBUG_ABORT_UNLESS(Actor);

        auto promise = NThreading::NewPromise<TResolveResult>();
        auto future = promise.GetFuture();
        if (!ActorSystem->Send(
                Actor,
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
