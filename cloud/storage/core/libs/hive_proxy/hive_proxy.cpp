#include "hive_proxy.h"

#include "hive_proxy_actor.h"
#include "hive_proxy_fallback_actor.h"
#include "hive_proxy_router.h"

namespace NCloud::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateHiveProxy(THiveProxyConfig config)
{
    return CreateHiveProxy(std::move(config), {});
}

IActorPtr CreateHiveProxy(
    THiveProxyConfig config,
    NMonitoring::TDynamicCounterPtr counters)
{
    if (config.FallbackModeProvider) {
        return std::make_unique<THiveProxyRouter>(
            std::move(config),
            std::move(counters));
    }

    if (config.FallbackMode) {
        return std::make_unique<THiveProxyFallbackActor>(std::move(config));
    }

    return std::make_unique<THiveProxyActor>(
        std::move(config),
        std::move(counters));
}

}   // namespace NCloud::NStorage
