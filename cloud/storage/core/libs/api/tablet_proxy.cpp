#include "tablet_proxy.h"

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/system/yassert.h>

#include <memory>
#include <utility>

namespace NCloud::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeTabletProxyServiceId()
{
    return TActorId(0, "sto-tproxy");
}

void ForwardToTablet(ui64 tabletId, IEventHandlePtr request)
{
    const bool sent = TActivationContext::AsActorContext().Send(
        MakeTabletProxyServiceId(),
        std::make_unique<TEvTabletProxy::TEvForward>(
            tabletId,
            std::move(request)));
    Y_ABORT_UNLESS(sent, "tablet proxy service is not registered");
}

}   // namespace NCloud::NStorage
