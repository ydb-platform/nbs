#include "tablet_proxy.h"

#include "tablet_proxy_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateIndexTabletProxy(
    TStorageConfigPtr config,
    ITraceSerializerPtr traceSerializer,
    IFileSystemEventHandlerPtr fileSystemEventHandler)
{
    return std::make_unique<TIndexTabletProxyActor>(
        std::move(config),
        std::move(traceSerializer),
        std::move(fileSystemEventHandler));
}

}   // namespace NCloud::NFileStore::NStorage
