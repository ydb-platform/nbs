#include "ss_proxy.h"

#include "ss_proxy_actor.h"
#include "ss_proxy_fallback_actor.h"

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateSSProxy(TSSProxyConfig config)
{
    if (config.FallbackMode) {
        return std::make_unique<TSSProxyFallbackActor>(std::move(config));
    }

    return std::make_unique<TSSProxyActor>(std::move(config));
}

}   // namespace NCloud::NStorage
