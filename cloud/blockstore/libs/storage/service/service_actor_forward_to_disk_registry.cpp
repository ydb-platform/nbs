#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TServiceActor::ForwardRequestToDiskRegistry(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev)
{
    const auto* msg = ev->Get();

    LWTRACK(
        RequestReceived_Service,
        msg->CallContext->LWOrbit,
        TMethod::Name,
        msg->CallContext->RequestId);

    ctx.Send(ev->Forward(MakeDiskRegistryProxyServiceId()));
}

#define BLOCKSTORE_FORWARD_REQUEST(name, ns)                        \
    void TServiceActor::Handle##name(                               \
        const ns::TEv##name##Request::TPtr& ev,                     \
        const TActorContext& ctx)                                   \
    {                                                               \
        ForwardRequestToDiskRegistry<ns::T##name##Method>(ctx, ev); \
    }                                                               \
    // BLOCKSTORE_FORWARD_REQUEST

BLOCKSTORE_DISK_REGISTRY_REQUESTS_FWD_SERVICE(
    BLOCKSTORE_FORWARD_REQUEST,
    TEvService)

#undef BLOCKSTORE_FORWARD_REQUEST

}   // namespace NCloud::NBlockStore::NStorage
