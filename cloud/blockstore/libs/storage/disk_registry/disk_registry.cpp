#include "disk_registry.h"

#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateDiskRegistry(
    const TActorId& owner,
    TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    NLogbroker::IServicePtr logbrokerService,
    NNotify::IServicePtr notifyService)
{
    return std::make_unique<TDiskRegistryActor>(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(logbrokerService),
        std::move(notifyService));
}

}   // namespace NCloud::NBlockStore::NStorage
