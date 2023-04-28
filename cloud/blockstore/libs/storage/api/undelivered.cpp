#include "undelivered.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeUndeliveredHandlerServiceId()
{
    return TActorId(0, "blk-undlvry");
}

IEventHandlePtr CreateRequestWithNondeliveryTracking(
    const TActorId& destination,
    IEventHandle& ev)
{
    auto undeliveredRequestActor = MakeUndeliveredHandlerServiceId();

    if (ev.HasEvent()) {
        return std::make_unique<IEventHandle>(
            destination,
            ev.Sender,
            ev.ReleaseBase().Release(),
            ev.Flags | IEventHandle::FlagForwardOnNondelivery,
            ev.Cookie,
            &undeliveredRequestActor,
            std::move(ev.TraceId));
    } else {
        return std::make_unique<IEventHandle>(
            ev.Type,
            ev.Flags | IEventHandle::FlagForwardOnNondelivery,
            destination,
            ev.Sender,
            ev.ReleaseChainBuffer(),
            ev.Cookie,
            &undeliveredRequestActor,
            std::move(ev.TraceId));
    }
}

void ForwardRequestWithNondeliveryTracking(
    const TActorContext& ctx,
    const TActorId& destination,
    IEventHandle& ev)
{
    auto event = CreateRequestWithNondeliveryTracking(destination, ev);
    ctx.Send(event.release());
}

}   // namespace NCloud::NBlockStore::NStorage
