#include "volume.h"

#include "volume_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateVolumeTablet(
    const TActorId& owner,
    TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorFactoryPtr blockDigestGeneratorFactory,
    ITraceSerializerPtr traceSerializer,
    NCloud::NStorage::NRdma::IClientPtr rdmaClient,
    TPartitionBudgetManagerPtr partitionBudgetManager,
    NServer::IEndpointEventHandlerPtr endpointEventHandler,
    EVolumeStartMode startMode,
    TString diskId)
{
    return std::make_unique<TVolumeActor>(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(blockDigestGeneratorFactory),
        std::move(traceSerializer),
        std::move(rdmaClient),
        std::move(partitionBudgetManager),
        std::move(endpointEventHandler),
        startMode,
        std::move(diskId));
}

}   // namespace NCloud::NBlockStore::NStorage
