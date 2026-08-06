#include "tablet_proxy.h"

#include "tablet_proxy_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateIndexTabletProxy(
    TStorageConfigPtr config,
    ITraceSerializerPtr traceSerializer)
{
    return std::make_unique<TIndexTabletProxyActor>(
        std::move(config),
        std::move(traceSerializer));
}

}   // namespace NCloud::NFileStore::NStorage
