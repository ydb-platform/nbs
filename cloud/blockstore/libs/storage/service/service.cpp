#include "service.h"

#include "service_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateStorageService(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NDiscovery::IDiscoveryServicePtr discoveryService,
    ITraceSerializerPtr traceSerializer,
    NServer::IEndpointEventHandlerPtr endpointEventHandler,
    NRdma::IClientPtr rdmaClient,
    IVolumeStatsPtr volumeStats,
    TManuallyPreemptedVolumesPtr preemptedVolumes,
    IDefaultEncryptionKeyProviderPtr defaultEncryptionKeyProvider)
{
    return std::make_unique<TServiceActor>(
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(blockDigestGenerator),
        std::move(discoveryService),
        std::move(traceSerializer),
        std::move(endpointEventHandler),
        std::move(rdmaClient),
        std::move(volumeStats),
        std::move(preemptedVolumes),
        std::move(defaultEncryptionKeyProvider));
}

}   // namespace NCloud::NBlockStore::NStorage
