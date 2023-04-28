#include "ss_proxy.h"

#include "ss_proxy_actor.h"
#include "ss_proxy_fallback_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateSSProxy(
    TStorageConfigPtr config,
    IFileIOServicePtr fileIO)
{
    if (config->GetSSProxyFallbackMode()) {
        return std::make_unique<TSSProxyFallbackActor>(
            std::move(config), std::move(fileIO));
    }

    return std::make_unique<TSSProxyActor>(
        std::move(config), std::move(fileIO));
}

}   // namespace NCloud::NBlockStore::NStorage
