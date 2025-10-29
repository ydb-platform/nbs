#include "undelivered.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUndeliveredHandlerActor final
    : public TActor<TUndeliveredHandlerActor>
{
public:
    TUndeliveredHandlerActor()
        : TActor(&TThis::StateWork)
    {}

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    template <typename TMethod>
    void CancelRequest(
        const TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    bool HandleRequests(STFUNC_SIG);
};

////////////////////////////////////////////////////////////////////////////////

void TUndeliveredHandlerActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

template <typename TMethod>
void TUndeliveredHandlerActor::CancelRequest(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev)
{
    auto response = std::make_unique<typename TMethod::TResponse>(
        MakeTabletIsDeadError(E_REJECTED, __LOCATION__));

    NCloud::Reply(ctx, *ev, std::move(response));
}

bool TUndeliveredHandlerActor::HandleRequests(STFUNC_SIG)
{
    auto ctx(ActorContext());
#define BLOCKSTORE_HANDLE_METHOD(name, ns)                                     \
    case ns::TEv##name##Request::EventType: {                                  \
        auto* x = reinterpret_cast<ns::TEv##name##Request::TPtr*>(&ev);        \
        CancelRequest<ns::T##name##Method>(ctx, *x);                           \
        break;                                                                 \
    }                                                                          \
// BLOCKSTORE_HANDLE_METHOD

    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_HANDLE_METHOD, TEvService)
        BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_HANDLE_METHOD, TEvService)
        BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_HANDLE_METHOD, TEvVolume)

        default:
            return false;
    }

    return true;

#undef BLOCKSTORE_HANDLE_METHOD
}

STFUNC(TUndeliveredHandlerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateUndeliveredHandler()
{
    return std::make_unique<TUndeliveredHandlerActor>();
}

}    // namespace NCloud::NBlockStore::NStorage
