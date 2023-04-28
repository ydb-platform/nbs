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
    IBlockDigestGeneratorPtr blockDigestGenerator,
    ITraceSerializerPtr traceSerializer,
    NRdma::IClientPtr rdmaClient,
    EVolumeStartMode startMode)
{
    return std::make_unique<TVolumeActor>(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(blockDigestGenerator),
        std::move(traceSerializer),
        std::move(rdmaClient),
        startMode);
}

}   // namespace NCloud::NBlockStore::NStorage
