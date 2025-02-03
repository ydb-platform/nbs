#include "hive_proxy.h"

#include "hive_proxy_actor.h"
#include "hive_proxy_fallback_actor.h"

namespace NCloud::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateHiveProxy(THiveProxyConfig config)
{
    if (config.FallbackMode) {
        return std::make_unique<THiveProxyFallbackActor>(std::move(config));
    }

    return std::make_unique<THiveProxyActor>(std::move(config));
}

IActorPtr CreateHiveProxy(
    THiveProxyConfig config,
    NMonitoring::TDynamicCounterPtr CountersRoot)
{
    if (config.FallbackMode) {
        return std::make_unique<THiveProxyFallbackActor>(std::move(config));
    }

    return std::make_unique<THiveProxyActor>(
        std::move(config),
        std::move(CountersRoot));
}

}   // namespace NCloud::NStorage
