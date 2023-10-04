#include "ss_proxy.h"

#include "ss_proxy_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateSSProxy(TStorageConfigPtr config)
{
    return std::make_unique<TSSProxyActor>(std::move(config));
}

}   // namespace NCloud::NFileStore::NStorage
