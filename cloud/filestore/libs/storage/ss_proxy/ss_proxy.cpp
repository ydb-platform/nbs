#include "ss_proxy.h"

#include "ss_proxy_actor.h"
#include "ss_proxy_fallback_actor.h"

#include <cloud/filestore/libs/storage/core/config.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateSSProxy(TStorageConfigPtr config)
{
    if (config->GetSSProxyFallbackMode()) {
        return std::make_unique<TSSProxyFallbackActor>(std::move(config));
    }

    return std::make_unique<TSSProxyActor>(std::move(config));
}

}   // namespace NCloud::NFileStore::NStorage
